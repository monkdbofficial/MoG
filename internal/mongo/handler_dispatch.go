package mongo

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
	"go.uber.org/zap"
	"gopkg.in/mgo.v2/bson"

	"mog/internal/logging"
	"mog/internal/metrics"
	mpipeline "mog/internal/mongo/pipeline"
	mwire "mog/internal/mongo/wire"
)

// MongoDB wire-protocol dispatch and command handling.
func (h *Handler) Handle(ctx context.Context, header mwire.MsgHeader, body []byte) ([]byte, error) {
	var response []byte
	var err error
	switch header.OpCode {
	case wiremessage.OpMsg:
		response, err = h.handleMsg(ctx, header, body)
	case wiremessage.OpQuery:
		response, err = h.handleQuery(ctx, header, body)
	default:
		return nil, fmt.Errorf("unsupported op code: %v", header.OpCode)
	}

	if err == nil {
		return response, nil
	}

	// Always respond with a Mongo-style error document so clients don't hang waiting for a reply.
	switch header.OpCode {
	case wiremessage.OpMsg:
		return h.newMsgError(header.RequestID, 8000, "InternalError", err.Error())
	case wiremessage.OpQuery:
		return h.newReplyError(header.RequestID, 8000, "InternalError", err.Error())
	default:
		return nil, err
	}
}

func (h *Handler) handleMsg(ctx context.Context, header mwire.MsgHeader, body []byte) ([]byte, error) {
	op, err := mwire.ParseOpMsg(header, body)
	if err != nil {
		return nil, fmt.Errorf("failed to read OpMsg: %w", err)
	}

	if len(op.Sections) == 0 {
		return nil, fmt.Errorf("OpMsg must have at least one section")
	}

	bodySection, ok := op.Sections[0].(mwire.SectionBody)
	if !ok {
		return nil, fmt.Errorf("first section must be Kind 0 (BSON Body)")
	}

	var cmd bson.M
	if err := bson.Unmarshal(bodySection.Document, &cmd); err != nil {
		return nil, fmt.Errorf("failed to unmarshal BSON body: %w", err)
	}

	// Merge document sequences (kind 1 sections) into the command document so existing handlers
	// can keep reading cmd["documents"], cmd["updates"], etc.
	for _, sec := range op.Sections[1:] {
		seq, ok := sec.(mwire.SectionDocumentSequence)
		if !ok {
			continue
		}

		arr := make([]interface{}, 0, len(seq.Documents))
		for _, docBytes := range seq.Documents {
			var m bson.M
			if err := bson.Unmarshal(docBytes, &m); err != nil {
				return nil, fmt.Errorf("failed to unmarshal OpMsg sequence doc: %w", err)
			}
			arr = append(arr, m)
		}
		cmd[seq.Identifier] = arr
	}

	return h.handleOpMsgCommand(ctx, op, cmd)
}

func (h *Handler) handleQuery(ctx context.Context, header mwire.MsgHeader, body []byte) ([]byte, error) {
	start := time.Now()
	op, err := mwire.ParseOpQuery(header, body)
	if err != nil {
		return nil, fmt.Errorf("failed to read OpQuery: %w", err)
	}

	var cmd bson.M
	if err := bson.Unmarshal(op.Query, &cmd); err != nil {
		return nil, fmt.Errorf("failed to unmarshal OpQuery command: %w", err)
	}
	if inner, ok := cmd["$query"].(bson.M); ok {
		cmd = inner
	}
	cmdName := primaryCommandKey(cmd)
	defer func() {
		metrics.ObserveMongoCommand("opquery", cmdName, time.Since(start))
	}()

	if _, ok := cmd["ping"]; ok {
		return h.newReply(op.Header.RequestID, bson.M{"ok": 1.0})
	}

	if _, ok := cmd["buildInfo"]; ok {
		return h.newReply(op.Header.RequestID, bson.M{
			"version":      reportedMongoVersion,
			"versionArray": []int32{8, 0, 0, 0},
			"gitVersion":   adapterGitVersion,
			"ok":           1.0,
		})
	}

	if _, ok := cmd["getParameter"]; ok {
		resp := bson.M{"ok": 1.0}
		if _, wantFCV := cmd["featureCompatibilityVersion"]; wantFCV {
			resp["featureCompatibilityVersion"] = bson.M{"version": reportedFCV}
		}
		return h.newReply(op.Header.RequestID, resp)
	}

	if _, ok := cmd["featureCompatibilityVersion"]; ok {
		return h.newReply(op.Header.RequestID, bson.M{
			"featureCompatibilityVersion": bson.M{"version": reportedFCV},
			"ok":                          1.0,
		})
	}

	if _, ok := cmd["atlasVersion"]; ok {
		return h.newReply(op.Header.RequestID, bson.M{
			"atlasVersion": "unsupported",
			"ok":           1.0,
		})
	}

	if _, ok := cmd["saslStart"]; ok {
		h.scramConv = h.scram.Start()
		payload, err := payloadFromCommand(cmd)
		if err != nil {
			return nil, err
		}
		serverFirst, err := h.scramConv.Step(payload)
		if err != nil {
			return nil, err
		}
		return h.newReply(op.Header.RequestID, bson.M{
			"conversationId": 1,
			"done":           false,
			"payload":        []byte(serverFirst),
			"ok":             1.0,
		})
	}

	if _, ok := cmd["saslContinue"]; ok {
		if h.scramConv == nil {
			return nil, fmt.Errorf("saslContinue received without saslStart")
		}
		payload, err := payloadFromCommand(cmd)
		if err != nil {
			return nil, err
		}
		serverFinal, err := h.scramConv.Step(payload)
		if err != nil {
			return nil, err
		}
		if h.scramConv.Done() {
			h.authenticated = true
		}
		return h.newReply(op.Header.RequestID, bson.M{
			"conversationId": 1,
			"done":           h.scramConv.Done(),
			"payload":        []byte(serverFinal),
			"ok":             1.0,
		})
	}

	if _, ok := cmd["isMaster"]; ok || cmd["ismaster"] != nil || cmd["hello"] != nil {
		resp := helloDoc(h.authenticated)
		_ = h.maybeSpeculativeAuth(cmd, resp)
		return h.newReply(op.Header.RequestID, resp)
	}

	if _, ok := cmd["buildInfo"]; ok {
		return h.newReply(op.Header.RequestID, bson.M{
			"version":      reportedMongoVersion,
			"versionArray": []int32{8, 0, 0, 0},
			"gitVersion":   adapterGitVersion,
			"ok":           1.0,
		})
	}

	if _, ok := cmd["connectionStatus"]; ok {
		resp := bson.M{
			"authInfo": bson.M{
				"authenticatedUsers":     []bson.M{},
				"authenticatedUserRoles": []bson.M{},
			},
			"ok": 1.0,
		}
		if show, _ := cmd["showPrivileges"].(bool); show {
			resp["authInfo"].(bson.M)["authenticatedUserPrivileges"] = []bson.M{}
		}
		return h.newReply(op.Header.RequestID, resp)
	}

	if _, ok := cmd["hostInfo"]; ok {
		return h.newReply(op.Header.RequestID, hostInfoDoc())
	}

	if _, ok := cmd["serverStatus"]; ok {
		return h.newReply(op.Header.RequestID, serverStatusDoc())
	}

	if _, ok := cmd["getCmdLineOpts"]; ok {
		return h.newReply(op.Header.RequestID, cmdLineOptsDoc())
	}

	if _, ok := cmd["listDatabases"]; ok {
		dbs, _ := h.catalogListDatabases(ctx)
		var out []bson.M
		for _, name := range dbs {
			empty := true
			if name != "" && name != "admin" {
				if colls, _ := h.catalogListCollections(ctx, name); len(colls) > 0 {
					empty = false
				}
			}
			out = append(out, bson.M{"name": name, "sizeOnDisk": int64(0), "empty": empty})
		}
		return h.newReply(op.Header.RequestID, bson.M{
			"databases":   out,
			"totalSize":   int64(0),
			"totalSizeMb": int64(0),
			"ok":          1.0,
		})
	}

	if _, ok := cmd["listCollections"]; ok {
		dbName, _ := cmd["$db"].(string)
		colls, _ := h.catalogListCollections(ctx, dbName)
		var firstBatch []bson.M
		for _, c := range colls {
			firstBatch = append(firstBatch, bson.M{"name": c, "type": "collection", "options": bson.M{}})
		}
		return h.newReply(op.Header.RequestID, bson.M{
			"cursor": bson.M{
				"id":         int64(0),
				"ns":         fmt.Sprintf("%s.$cmd.listCollections", dbName),
				"firstBatch": firstBatch,
			},
			"ok": 1.0,
		})
	}

	return h.newReplyError(op.Header.RequestID, 59, "CommandNotFound", fmt.Sprintf("unsupported command in OpQuery: %v", cmd))
}

func (h *Handler) handleOpMsgCommand(ctx context.Context, op *mwire.OpMsg, cmd bson.M) ([]byte, error) {
	start := time.Now()
	cmdName := primaryCommandKey(cmd)
	defer func() {
		metrics.ObserveMongoCommand("opmsg", cmdName, time.Since(start))
	}()

	if logging.Logger() != nil {
		dbName := commandDB(cmd)
		keys := make([]string, 0, len(cmd))
		for k := range cmd {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		logging.Logger().Debug("opmsg command", zap.String("db", dbName), zap.String("cmd", primaryCommandKey(cmd)), zap.Strings("keys", keys))
	}

	// Handle ping command (used by drivers for monitoring/health checks).
	if _, ok := cmd["ping"]; ok {
		return h.newMsg(op.Header.RequestID, bson.M{"ok": 1.0})
	}

	if _, ok := cmd["connectionStatus"]; ok {
		resp := bson.M{
			"authInfo": bson.M{
				"authenticatedUsers":     []bson.M{},
				"authenticatedUserRoles": []bson.M{},
			},
			"ok": 1.0,
		}
		if show, _ := cmd["showPrivileges"].(bool); show {
			resp["authInfo"].(bson.M)["authenticatedUserPrivileges"] = []bson.M{}
		}
		return h.newMsg(op.Header.RequestID, resp)
	}

	if _, ok := cmd["hostInfo"]; ok {
		return h.newMsg(op.Header.RequestID, hostInfoDoc())
	}

	if _, ok := cmd["serverStatus"]; ok {
		return h.newMsg(op.Header.RequestID, serverStatusDoc())
	}

	if _, ok := cmd["getCmdLineOpts"]; ok {
		return h.newMsg(op.Header.RequestID, cmdLineOptsDoc())
	}

	if col, ok := cmd["create"].(string); ok {
		dbName := commandDB(cmd)
		physical, err := physicalCollectionName(dbName, col)
		if err != nil {
			return h.newMsgError(op.Header.RequestID, 2, "BadValue", err.Error())
		}
		if err := h.ensureCollectionTable(ctx, physical); err != nil {
			return h.newMsgError(op.Header.RequestID, 8000, "InternalError", err.Error())
		}
		h.markTouched(physical)
		_ = h.catalogUpsert(ctx, dbName, col)
		h.refreshTouched(ctx)
		return h.newMsg(op.Header.RequestID, bson.M{"ok": 1.0})
	}

	if col, ok := cmd["drop"].(string); ok {
		dbName := commandDB(cmd)
		physical, err := physicalCollectionName(dbName, col)
		if err != nil {
			return h.newMsgError(op.Header.RequestID, 2, "BadValue", err.Error())
		}
		h.clearUniqueIndexes(physical)
		if err := h.dropCollectionTable(ctx, physical); err != nil {
			return h.newMsgError(op.Header.RequestID, 8000, "InternalError", err.Error())
		}
		h.markTouched(physical)
		_ = h.catalogRemoveCollection(ctx, dbName, col)
		h.refreshTouched(ctx)
		return h.newMsg(op.Header.RequestID, bson.M{"ok": 1.0})
	}

	if _, ok := cmd["dropDatabase"]; ok {
		dbName := commandDB(cmd)
		h.clearUniqueIndexesForDB(dbName)
		if err := h.dropDatabase(ctx, dbName); err != nil {
			return h.newMsgError(op.Header.RequestID, 8000, "InternalError", err.Error())
		}
		_ = h.catalogRemoveDatabase(ctx, dbName)
		h.refreshTouched(ctx)
		return h.newMsg(op.Header.RequestID, bson.M{"dropped": dbName, "ok": 1.0})
	}

	if _, ok := cmd["listCollections"]; ok {
		dbName := commandDB(cmd)
		colls, _ := h.catalogListCollections(ctx, dbName)
		var firstBatch []bson.M
		for _, c := range colls {
			firstBatch = append(firstBatch, bson.M{"name": c, "type": "collection", "options": bson.M{}})
		}
		return h.newMsg(op.Header.RequestID, bson.M{
			"cursor": bson.M{
				"id":         int64(0),
				"ns":         fmt.Sprintf("%s.$cmd.listCollections", dbName),
				"firstBatch": firstBatch,
			},
			"ok": 1.0,
		})
	}

	if col, ok := cmd["collStats"].(string); ok {
		dbName := commandDB(cmd)
		physical, err := physicalCollectionName(dbName, col)
		if err != nil {
			return h.newMsgError(op.Header.RequestID, 2, "BadValue", err.Error())
		}
		_ = h.catalogUpsert(ctx, dbName, col)

		var count int64
		var size int64
		var storageSize int64
		if h.pool != nil {
			sql, args, err := h.translator.TranslateCount(physical, bson.M{})
			if err == nil {
				args, err = normalizeSQLArgs(args)
			}
			if err == nil {
				if scanErr := h.db().QueryRow(ctx, sql, args...).Scan(&count); scanErr != nil {
					if isUndefinedRelation(scanErr) || isUndefinedSchema(scanErr) {
						count = 0
					} else {
						return nil, scanErr
					}
				}
			}

			// Approximate MongoDB "size" as total BSON bytes of documents as returned by this server.
			// This is logically consistent for clients even if the backing store isn't BSON.
			if count > 0 {
				rows, qerr := h.db().Query(ctx, "SELECT data FROM doc."+physical)
				if qerr == nil {
					for rows.Next() {
						var v interface{}
						if err := rows.Scan(&v); err != nil {
							continue
						}
						data, okDoc := coerceBsonM(v)
						if !okDoc {
							continue
						}
						normalizeDocForReply(data)
						if b, err := bson.Marshal(data); err == nil {
							size += int64(len(b))
						}
					}
					rows.Close()
				}
			}
			if size > 0 {
				storageSize = size
			}
		}
		return h.newMsg(op.Header.RequestID, bson.M{
			"ns":             fmt.Sprintf("%s.%s", dbName, col),
			"count":          count,
			"size":           size,
			"storageSize":    storageSize,
			"nindexes":       int32(1),
			"totalIndexSize": int64(0),
			"indexSizes":     bson.M{"_id_": int64(0)},
			"ok":             1.0,
		})
	}

	if _, ok := cmd["dbStats"]; ok {
		dbName := commandDB(cmd)
		return h.newMsg(op.Header.RequestID, bson.M{
			"db":          dbName,
			"collections": int32(0),
			"views":       int32(0),
			"objects":     int64(0),
			"avgObjSize":  float64(0),
			"dataSize":    int64(0),
			"storageSize": int64(0),
			"indexes":     int32(0),
			"indexSize":   int64(0),
			"ok":          1.0,
		})
	}

	if col, ok := cmd["listIndexes"].(string); ok {
		dbName := commandDB(cmd)
		physical, _ := physicalCollectionName(dbName, col)
		var extra []bson.M
		if physical != "" {
			uis := h.listUniqueIndexes(physical)
			for _, ui := range uis {
				key := bson.M{}
				for _, f := range ui.fields {
					key[f] = int32(1)
				}
				extra = append(extra, bson.M{"v": int32(2), "name": ui.name, "key": key, "unique": true})
			}
		}
		firstBatch := []bson.M{
			{"v": int32(2), "name": "_id_", "key": bson.M{"_id": int32(1)}},
		}
		firstBatch = append(firstBatch, extra...)
		return h.newMsg(op.Header.RequestID, bson.M{
			"cursor": bson.M{
				"id":         int64(0),
				"ns":         fmt.Sprintf("%s.%s", dbName, col),
				"firstBatch": firstBatch,
			},
			"ok": 1.0,
		})
	}

	if _, ok := cmd["createIndexes"]; ok {
		dbName := commandDB(cmd)
		col, _ := cmd["createIndexes"].(string)
		physical, err := physicalCollectionName(dbName, col)
		if err != nil {
			return h.newMsgError(op.Header.RequestID, 2, "BadValue", err.Error())
		}

		rawIndexes, _ := cmd["indexes"]
		indexes, ok := rawIndexes.([]interface{})
		if !ok {
			if typed, ok := rawIndexes.([]bson.M); ok {
				indexes = make([]interface{}, 0, len(typed))
				for _, it := range typed {
					indexes = append(indexes, it)
				}
				ok = true
			}
		}
		if !ok {
			return h.newMsgError(op.Header.RequestID, 2, "BadValue", "indexes must be an array")
		}

		var uniqueDefs []uniqueIndexDef
		for _, it := range indexes {
			idxDoc, ok := coerceBsonM(it)
			if !ok {
				continue
			}
			unique, _ := idxDoc["unique"].(bool)
			if !unique {
				continue
			}
			name, _ := idxDoc["name"].(string)
			if name == "" {
				name = "unnamed_unique"
			}
			keyDoc, _ := coerceBsonM(idxDoc["key"])
			if keyDoc == nil {
				continue
			}

			fields := make([]string, 0, len(keyDoc))
			for k := range keyDoc {
				// Only support top-level fields for uniqueness enforcement.
				if k == "" || k == "_id" || strings.Contains(k, ".") {
					continue
				}
				fields = append(fields, k)
			}
			sort.Strings(fields)
			if len(fields) == 0 {
				continue
			}
			uniqueDefs = append(uniqueDefs, uniqueIndexDef{name: name, fields: fields})
		}

		if len(uniqueDefs) > 0 && physical != "" {
			h.addUniqueIndexes(physical, uniqueDefs)

			// Best-effort: also create SQL UNIQUE indexes when the underlying columns exist.
			cols, _ := h.listColumns(ctx, physical)
			colSet := map[string]struct{}{}
			for _, c := range cols {
				if c != "" {
					colSet[c] = struct{}{}
				}
			}
			for _, ui := range uniqueDefs {
				var sqlCols []string
				allExist := true
				for _, f := range ui.fields {
					c := sqlColumnNameForField(f)
					if c == "" || c == "id" || c == "data" {
						allExist = false
						break
					}
					if _, ok := colSet[c]; !ok {
						allExist = false
						break
					}
					sqlCols = append(sqlCols, c)
				}
				if !allExist || len(sqlCols) == 0 {
					continue
				}
				// Index name must be a safe SQL identifier; encode user-provided names when needed.
				idxName := fmt.Sprintf("u_%s_%s", physical, ui.name)
				if !isSafeIdentifier(idxName) {
					idxName = fmt.Sprintf("u_%s_%s", physical, strings.ToLower(fieldB32.EncodeToString([]byte(ui.name))))
				}
				if _, err := h.pool.Exec(ctx, fmt.Sprintf("CREATE UNIQUE INDEX %s ON doc.%s (%s)", idxName, physical, strings.Join(sqlCols, ", "))); err != nil && !isDuplicateObject(err) {
					// Best-effort: still enforce uniqueness in-memory.
				}
			}
		}

		return h.newMsg(op.Header.RequestID, bson.M{"ok": 1.0, "createdCollectionAutomatically": false})
	}

	if _, ok := cmd["dropIndexes"]; ok {
		dbName := commandDB(cmd)
		col, _ := cmd["dropIndexes"].(string)
		if col != "" {
			if physical, _ := physicalCollectionName(dbName, col); physical != "" {
				h.clearUniqueIndexes(physical)
			}
		}
		return h.newMsg(op.Header.RequestID, bson.M{"ok": 1.0})
	}

	// Common admin introspection commands (often sent before auth by tools/IDEs).
	if val, ok := cmd["getLog"]; ok {
		if s, ok := val.(string); ok && s == "startupWarnings" {
			return h.newMsg(op.Header.RequestID, bson.M{
				"totalLinesWritten": 0,
				"log":               []string{},
				"ok":                1.0,
			})
		}
		return h.newMsgError(op.Header.RequestID, 59, "CommandNotFound", fmt.Sprintf("unsupported getLog variant: %v", val))
	}

	// Handle saslStart command
	if _, ok := cmd["saslStart"]; ok {
		h.scramConv = h.scram.Start()
		payload, err := payloadFromCommand(cmd)
		if err != nil {
			return nil, err
		}
		serverFirst, err := h.scramConv.Step(payload)
		if err != nil {
			return nil, err
		}
		respDoc := bson.M{
			"conversationId": 1,
			"done":           false,
			"payload":        []byte(serverFirst),
			"ok":             1.0,
		}
		return h.newMsg(op.Header.RequestID, respDoc)
	}

	if _, ok := cmd["buildInfo"]; ok {
		return h.newMsg(op.Header.RequestID, bson.M{
			"version":      reportedMongoVersion,
			"versionArray": []int32{8, 0, 0, 0},
			"gitVersion":   adapterGitVersion,
			"ok":           1.0,
		})
	}

	if _, ok := cmd["getParameter"]; ok {
		resp := bson.M{"ok": 1.0}
		if _, wantFCV := cmd["featureCompatibilityVersion"]; wantFCV {
			resp["featureCompatibilityVersion"] = bson.M{"version": reportedFCV}
		}
		return h.newMsg(op.Header.RequestID, resp)
	}

	if _, ok := cmd["featureCompatibilityVersion"]; ok {
		return h.newMsg(op.Header.RequestID, bson.M{
			"featureCompatibilityVersion": bson.M{"version": reportedFCV},
			"ok":                          1.0,
		})
	}

	if _, ok := cmd["atlasVersion"]; ok {
		return h.newMsg(op.Header.RequestID, bson.M{
			"atlasVersion": "unsupported",
			"ok":           1.0,
		})
	}

	if _, ok := cmd["endSessions"]; ok {
		return h.newMsg(op.Header.RequestID, bson.M{"ok": 1.0})
	}

	// Handle saslContinue command
	if _, ok := cmd["saslContinue"]; ok {
		if h.scramConv == nil {
			return nil, fmt.Errorf("saslContinue received without saslStart")
		}
		payload, err := payloadFromCommand(cmd)
		if err != nil {
			return nil, err
		}
		serverFinal, err := h.scramConv.Step(payload)
		if err != nil {
			return nil, err
		}
		if h.scramConv.Done() {
			h.authenticated = true
		}
		respDoc := bson.M{
			"conversationId": 1,
			"done":           h.scramConv.Done(),
			"payload":        []byte(serverFinal),
			"ok":             1.0,
		}
		return h.newMsg(op.Header.RequestID, respDoc)
	}

	// All other commands require authentication
	if !h.authenticated {
		// Common handshake / introspection commands are allowed without authentication.
		if _, ok := cmd["isMaster"]; !ok &&
			cmd["ismaster"] == nil &&
			cmd["hello"] == nil &&
			cmd["buildInfo"] == nil &&
			cmd["ping"] == nil &&
			cmd["listDatabases"] == nil {
			return h.newMsgError(op.Header.RequestID, 13, "Unauthorized", "authentication required")
		}
	}

	if _, ok := cmd["listDatabases"]; ok {
		dbs, _ := h.catalogListDatabases(ctx)
		var out []bson.M
		for _, name := range dbs {
			empty := true
			if name != "" && name != "admin" {
				if colls, _ := h.catalogListCollections(ctx, name); len(colls) > 0 {
					empty = false
				}
			}
			out = append(out, bson.M{"name": name, "sizeOnDisk": int64(0), "empty": empty})
		}
		return h.newMsg(op.Header.RequestID, bson.M{
			"databases":   out,
			"totalSize":   int64(0),
			"totalSizeMb": int64(0),
			"ok":          1.0,
		})
	}

	// Handle transaction commands
	if start, _ := cmd["startTransaction"].(bool); start {
		tx, err := h.pool.Begin(ctx)
		if err != nil {
			return nil, err
		}
		h.tx = tx
		return h.newMsg(op.Header.RequestID, bson.M{"ok": 1.0})
	}
	if _, ok := cmd["commitTransaction"]; ok {
		if h.tx == nil {
			return nil, fmt.Errorf("no active transaction to commit")
		}
		if err := h.tx.Commit(ctx); err != nil {
			return nil, err
		}
		h.tx = nil
		h.refreshTouched(ctx)
		return h.newMsg(op.Header.RequestID, bson.M{"ok": 1.0})
	}
	if _, ok := cmd["abortTransaction"]; ok {
		if h.tx == nil {
			return nil, fmt.Errorf("no active transaction to abort")
		}
		if err := h.tx.Rollback(ctx); err != nil {
			return nil, err
		}
		h.tx = nil
		h.touched = map[string]struct{}{}
		return h.newMsg(op.Header.RequestID, bson.M{"ok": 1.0})
	}

	// Handle find command
	if col, ok := cmd["find"].(string); ok {
		dbName := commandDB(cmd)
		physical, err := physicalCollectionName(dbName, col)
		if err != nil {
			return h.newMsgError(op.Header.RequestID, 2, "BadValue", err.Error())
		}
		_ = h.catalogUpsert(ctx, dbName, col)

		filter := bson.M{}
		if rawFilter, ok := cmd["filter"]; ok {
			if m, ok := coerceBsonM(rawFilter); ok {
				filter = m
			}
		}

		sortSpec := bson.M{}
		if rawSort, ok := cmd["sort"]; ok {
			if m, ok := coerceBsonM(rawSort); ok {
				sortSpec = m
			}
		}
		limit := 0
		if rawLimit, ok := cmd["limit"]; ok {
			if i, err := mpipeline.AsInt(rawLimit); err == nil {
				limit = i
			}
		}
		skip := 0
		if rawSkip, ok := cmd["skip"]; ok {
			if i, err := mpipeline.AsInt(rawSkip); err == nil {
				skip = i
			}
		}

		var docs []bson.M
		pushedDown := false

		// Preserve Mongo array semantics: exclude `$in` fields from SQL pushdown, then apply the full filter in-memory.
		pushdownFilter := filter
		needPostFilter := false
		if filterHasOperator(filter, "$in") {
			pushdownFilter = stripOperatorKeys(filter, "$in")
			needPostFilter = len(pushdownFilter) != len(filter)
		}

		if len(pushdownFilter) > 0 || len(sortSpec) > 0 || skip > 0 || limit > 0 {
			where, ok, err := buildRelationalWhere(pushdownFilter)
			if err != nil {
				return nil, err
			}
			orderBy, ok2, err := buildRelationalOrderBy(sortSpec)
			if err != nil {
				return nil, err
			}
			if ok && ok2 {
				q := "SELECT * FROM doc." + physical
				args := []interface{}(nil)
				if where != nil && where.SQL != "" {
					q += " WHERE " + where.SQL
					args = append(args, where.Args...)
				}
				q += orderBy
				if limit > 0 {
					q = fmt.Sprintf("%s LIMIT %d", q, limit)
				}
				if skip > 0 {
					q = fmt.Sprintf("%s OFFSET %d", q, skip)
				}
				pdocs, err := h.loadSQLDocsWithIDsQuery(ctx, h.db(), q, args...)
				if err == nil {
					docs = make([]bson.M, 0, len(pdocs))
					for _, pd := range pdocs {
						docs = append(docs, pd.doc)
					}
					pushedDown = true
				}
			}
		}

		if !pushedDown {
			baseDocs, err := h.loadSQLDocs(ctx, physical)
			if err != nil {
				return nil, err
			}
			docs = baseDocs
			if len(filter) > 0 {
				docs = mpipeline.ApplyMatch(docs, filter)
			}
			if len(sortSpec) > 0 {
				docs = mpipeline.ApplySort(docs, sortSpec)
			}
			if skip > 0 {
				if skip >= len(docs) {
					docs = nil
				} else {
					docs = docs[skip:]
				}
			}
			if limit > 0 && limit < len(docs) {
				docs = docs[:limit]
			}
		} else if needPostFilter && len(filter) > 0 {
			docs = mpipeline.ApplyMatch(docs, filter)
		}
		for _, d := range docs {
			normalizeDocForReply(d)
		}

		if logging.Logger() != nil {
			logging.Logger().Debug("find result", zap.String("db", dbName), zap.String("coll", col), zap.String("physical", physical), zap.Int("returned", len(docs)))
		}

		var firstBatch interface{} = docs
		if h.stableFieldOrder {
			ordered := make([]interface{}, 0, len(docs))
			for _, d := range docs {
				ordered = append(ordered, orderTopLevelDocForReply(d))
			}
			firstBatch = ordered
		}

		respDoc := bson.M{
			"cursor": bson.M{
				"id":         int64(0),
				"ns":         fmt.Sprintf("%s.%s", cmd["$db"], col),
				"firstBatch": firstBatch,
			},
			"ok": 1.0,
		}

		return h.newMsg(op.Header.RequestID, respDoc)
	}

	// Handle count command (used by some clients; count_documents uses aggregate).
	if col, ok := cmd["count"].(string); ok {
		dbName := commandDB(cmd)
		physical, err := physicalCollectionName(dbName, col)
		if err != nil {
			return h.newMsgError(op.Header.RequestID, 2, "BadValue", err.Error())
		}
		_ = h.catalogUpsert(ctx, dbName, col)

		query := bson.M{}
		if rawQuery, ok := cmd["query"]; ok {
			if m, ok := coerceBsonM(rawQuery); ok {
				query = m
			}
		}

		var n int64
		if len(query) == 0 {
			// Fast path: exact count without scanning.
			if err := h.db().QueryRow(ctx, "SELECT COUNT(*) FROM doc."+physical).Scan(&n); err != nil {
				if isUndefinedRelation(err) || isUndefinedSchema(err) {
					n = 0
				} else {
					return nil, err
				}
			}
		} else {
			// Prefer SQL pushdown when safe; preserve Mongo array semantics for `$in` by applying in-memory.
			pushdownQuery := query
			needPostFilter := false
			if filterHasOperator(query, "$in") {
				pushdownQuery = stripOperatorKeys(query, "$in")
				needPostFilter = len(pushdownQuery) != len(query)
			}
			if len(pushdownQuery) > 0 {
				where, ok, err := buildRelationalWhere(pushdownQuery)
				if err != nil {
					return nil, err
				}
				if ok && where != nil && where.SQL != "" {
					if !needPostFilter {
						q := "SELECT COUNT(*) FROM doc." + physical + " WHERE " + where.SQL
						if err := h.db().QueryRow(ctx, q, where.Args...).Scan(&n); err == nil {
							goto countDone
						}
					} else {
						pdocs, err := h.loadSQLDocsWithIDsQuery(ctx, h.db(), "SELECT * FROM doc."+physical+" WHERE "+where.SQL, where.Args...)
						if err == nil {
							baseDocs := make([]bson.M, 0, len(pdocs))
							for _, pd := range pdocs {
								baseDocs = append(baseDocs, pd.doc)
							}
							baseDocs = mpipeline.ApplyMatch(baseDocs, query)
							n = int64(len(baseDocs))
							goto countDone
						}
					}
				}
			}
			baseDocs, err := h.loadSQLDocs(ctx, physical)
			if err != nil {
				return nil, err
			}
			baseDocs = mpipeline.ApplyMatch(baseDocs, query)
			n = int64(len(baseDocs))
		}
	countDone:
		if logging.Logger() != nil {
			logging.Logger().Debug("count result", zap.String("db", dbName), zap.String("coll", col), zap.String("physical", physical), zap.Int64("n", n))
		}
		return h.newMsg(op.Header.RequestID, bson.M{"n": n, "ok": 1.0})
	}

	// Handle insert command
	if col, ok := cmd["insert"].(string); ok {
		dbName := commandDB(cmd)
		physical, err := physicalCollectionName(dbName, col)
		if err != nil {
			return h.newMsgError(op.Header.RequestID, 2, "BadValue", err.Error())
		}
		_ = h.catalogUpsert(ctx, dbName, col)

		docs, ok := cmd["documents"].([]interface{})
		if !ok {
			// Drivers may decode arrays into []bson.M or []map[string]interface{} depending on decoding path.
			switch typed := cmd["documents"].(type) {
			case []bson.M:
				docs = make([]interface{}, 0, len(typed))
				for _, d := range typed {
					docs = append(docs, d)
				}
				ok = true
			case []map[string]interface{}:
				docs = make([]interface{}, 0, len(typed))
				for _, d := range typed {
					docs = append(docs, d)
				}
				ok = true
			}
		}
		if !ok {
			return nil, fmt.Errorf("documents must be an array")
		}
		seen, inserted, err := h.insertMany(ctx, physical, docs)
		if err != nil {
			if strings.HasPrefix(err.Error(), "E11000") {
				return h.newMsgError(op.Header.RequestID, 11000, "DuplicateKey", err.Error())
			}
			if isUndefinedRelation(err) || isUndefinedSchema(err) {
				// Best-effort create and retry once.
				h.schemaCache().clear(physical)
				if err := h.ensureCollectionTable(ctx, physical); err != nil {
					return nil, err
				}
				_ = h.catalogUpsert(ctx, dbName, col)
				_, inserted, err = h.insertMany(ctx, physical, docs)
				if err != nil {
					if strings.HasPrefix(err.Error(), "E11000") {
						return h.newMsgError(op.Header.RequestID, 11000, "DuplicateKey", err.Error())
					}
					return nil, err
				}
			} else {
				return nil, err
			}
		}

		if inserted > 0 {
			h.markTouched(physical)
		}
		h.refreshTouched(ctx)

		n := int(inserted)
		if logging.Logger() != nil {
			logging.Logger().Debug("insert result",
				zap.String("db", dbName),
				zap.String("coll", col),
				zap.String("physical", physical),
				zap.Int("docs_seen", seen),
				zap.Int64("rows_affected", inserted),
			)
			if h.logWriteInfo {
				logging.Logger().Info("mongo insert",
					zap.String("remote_addr", RemoteAddr(ctx)),
					zap.Int32("request_id", op.Header.RequestID),
					zap.String("db", dbName),
					zap.String("coll", col),
					zap.Int("n", n),
				)
			}
		}
		respDoc := bson.M{"n": n, "ok": 1.0}

		return h.newMsg(op.Header.RequestID, respDoc)
	}

	// Handle update command
	if col, ok := cmd["update"].(string); ok {
		dbName := commandDB(cmd)
		physical, err := physicalCollectionName(dbName, col)
		if err != nil {
			return h.newMsgError(op.Header.RequestID, 2, "BadValue", err.Error())
		}
		_ = h.catalogUpsert(ctx, dbName, col)

		if logging.Logger() != nil {
			logging.Logger().Debug("update handler called", zap.String("db", dbName), zap.String("coll", col), zap.String("physical", physical))
		}

		updates, ok := cmd["updates"].([]interface{})
		if !ok {
			switch typed := cmd["updates"].(type) {
			case []bson.M:
				updates = make([]interface{}, 0, len(typed))
				for _, u := range typed {
					updates = append(updates, u)
				}
				ok = true
			case []map[string]interface{}:
				updates = make([]interface{}, 0, len(typed))
				for _, u := range typed {
					updates = append(updates, u)
				}
				ok = true
			}
		}
		if !ok {
			return nil, fmt.Errorf("updates must be an array")
		}

		// Use a single DB transaction for the whole update command to avoid connection conflicts
		// under high concurrency.
		tx := h.tx
		ownedTx := false
		if tx == nil {
			tx, err = h.pool.Begin(ctx)
			if err != nil {
				return nil, err
			}
			ownedTx = true
			defer func() { _ = tx.Rollback(ctx) }()
		}

		nMatched := 0
		nModified := 0
		seen := 0
		var upserted []bson.M
		for idx, u := range updates {
			seen++
			updateDoc, ok := coerceBsonM(u)
			if !ok {
				continue
			}

			filter := bson.M{}
			if q, ok := updateDoc["q"]; ok {
				if m, ok := coerceBsonM(q); ok {
					filter = m
				}
			}
			update := bson.M{}
			if uu, ok := updateDoc["u"]; ok {
				if m, ok := coerceBsonM(uu); ok {
					update = m
				}
			}
			multi, _ := updateDoc["multi"].(bool)
			upsert, _ := updateDoc["upsert"].(bool)

			matched := 0
			modified := 0
			var upsertedID interface{} = nil
			var err error
			matched, modified, upsertedID, err = h.applyPureSQLUpdate(ctx, tx, physical, filter, update, multi, upsert)
			if err != nil {
				if isUndefinedRelation(err) || isUndefinedSchema(err) {
					h.schemaCache().clear(physical)
					if err := h.ensureCollectionTable(ctx, physical); err != nil {
						return nil, err
					}
					matched, modified, upsertedID, err = h.applyPureSQLUpdate(ctx, tx, physical, filter, update, multi, upsert)
				}
				if err != nil {
					return nil, err
				}
			}
			nMatched += matched
			nModified += modified
			if matched > 0 || modified > 0 {
				h.markTouched(physical)
			}

			if upsertedID != nil {
				upserted = append(upserted, bson.M{"index": idx, "_id": upsertedID})
				h.markTouched(physical)
			}
		}

		if ownedTx {
			if err := tx.Commit(ctx); err != nil {
				return nil, err
			}
		}

		h.refreshTouched(ctx)
		if logging.Logger() != nil {
			logging.Logger().Debug("update result",
				zap.String("db", dbName),
				zap.String("coll", col),
				zap.String("physical", physical),
				zap.Int("updates_seen", seen),
				zap.Int("nMatched", nMatched),
				zap.Int("nModified", nModified),
				zap.Int("nUpserted", len(upserted)),
			)
			if h.logWriteInfo {
				logging.Logger().Info("mongo update",
					zap.String("remote_addr", RemoteAddr(ctx)),
					zap.Int32("request_id", op.Header.RequestID),
					zap.String("db", dbName),
					zap.String("coll", col),
					zap.Int("nMatched", nMatched),
					zap.Int("nModified", nModified),
					zap.Int("nUpserted", len(upserted)),
				)
			}
		}
		if nMatched == 0 && seen > 0 {
			// do not treat as error: update may legitimately match 0 docs
		}

		respDoc := bson.M{
			"n":         nMatched,
			"nModified": nModified,
			"ok":        1.0,
		}
		if len(upserted) > 0 {
			respDoc["upserted"] = upserted
		}

		return h.newMsg(op.Header.RequestID, respDoc)
	}

	// Handle delete command
	if col, ok := cmd["delete"].(string); ok {
		dbName := commandDB(cmd)
		physical, err := physicalCollectionName(dbName, col)
		if err != nil {
			return h.newMsgError(op.Header.RequestID, 2, "BadValue", err.Error())
		}
		_ = h.catalogUpsert(ctx, dbName, col)

		deletes, ok := cmd["deletes"].([]interface{})
		if !ok {
			switch typed := cmd["deletes"].(type) {
			case []bson.M:
				deletes = make([]interface{}, 0, len(typed))
				for _, d := range typed {
					deletes = append(deletes, d)
				}
				ok = true
			case []map[string]interface{}:
				deletes = make([]interface{}, 0, len(typed))
				for _, d := range typed {
					deletes = append(deletes, d)
				}
				ok = true
			}
		}
		if !ok {
			return nil, fmt.Errorf("deletes must be an array")
		}

		n := 0
		affected := int64(0)
		for _, d := range deletes {
			deleteDoc, ok := coerceBsonM(d)
			if !ok {
				continue
			}

			filter := bson.M{}
			if q, ok := deleteDoc["q"]; ok {
				if m, ok := coerceBsonM(q); ok {
					filter = m
				}
			}

			limit := 0
			if rawLimit, ok := deleteDoc["limit"]; ok {
				if i, err := mpipeline.AsInt(rawLimit); err == nil {
					limit = i
				}
			}

			pdocs, err := h.loadSQLDocsWithIDs(ctx, h.db(), physical)
			if err != nil {
				return nil, err
			}
			var delIDs []string
			for _, pd := range pdocs {
				if mpipeline.MatchDoc(pd.doc, filter) {
					delIDs = append(delIDs, pd.docID)
					if limit == 1 {
						break
					}
				}
			}
			for _, id := range delIDs {
				tag, err := h.db().Exec(ctx, "DELETE FROM doc."+physical+" WHERE id = $1", id)
				if err != nil {
					return nil, err
				}
				affected += tag.RowsAffected()
				n += int(tag.RowsAffected())
				h.markTouched(physical)
			}
		}
		h.refreshTouched(ctx)
		if logging.Logger() != nil {
			logging.Logger().Debug("delete result",
				zap.String("db", dbName),
				zap.String("coll", col),
				zap.String("physical", physical),
				zap.Int64("rows_affected", affected),
			)
			if h.logWriteInfo {
				logging.Logger().Info("mongo delete",
					zap.String("remote_addr", RemoteAddr(ctx)),
					zap.Int32("request_id", op.Header.RequestID),
					zap.String("db", dbName),
					zap.String("coll", col),
					zap.Int("n", n),
				)
			}
		}

		respDoc := bson.M{
			"n":  n,
			"ok": 1.0,
		}

		return h.newMsg(op.Header.RequestID, respDoc)
	}

	// Re-use aggregation logic if it's an aggregate command
	if col, ok := cmd["aggregate"].(string); ok {
		dbName := commandDB(cmd)
		physical, err := physicalCollectionName(dbName, col)
		if err != nil {
			return h.newMsgError(op.Header.RequestID, 2, "BadValue", err.Error())
		}
		_ = h.catalogUpsert(ctx, dbName, col)

		pipeline, ok := cmd["pipeline"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("pipeline must be an array")
		}

		var pipelineDocs []bson.M
		for _, stage := range pipeline {
			if stageDoc, ok := coerceBsonM(stage); ok {
				pipelineDocs = append(pipelineDocs, stageDoc)
			}
		}

		baseDocs, err := h.loadSQLDocs(ctx, physical)
		if err != nil {
			return nil, err
		}
		lookupCache := map[string][]bson.M{}
		resolveLookup := func(from string) ([]bson.M, error) {
			if cached, ok := lookupCache[from]; ok {
				return cached, nil
			}
			fromPhysical, err := physicalCollectionName(dbName, from)
			if err != nil {
				lookupCache[from] = []bson.M{}
				return lookupCache[from], nil
			}
			_ = h.catalogUpsert(ctx, dbName, from)
			docs, err := h.loadSQLDocs(ctx, fromPhysical)
			if err != nil {
				return nil, err
			}
			lookupCache[from] = docs
			return docs, nil
		}

		outDocs, err := mpipeline.ApplyPipelineWithLookup(baseDocs, pipelineDocs, resolveLookup)
		if err != nil {
			return nil, err
		}
		for _, d := range outDocs {
			normalizeDocForReply(d)
		}
		var firstBatch interface{} = outDocs
		if h.stableFieldOrder {
			ordered := make([]interface{}, 0, len(outDocs))
			for _, d := range outDocs {
				ordered = append(ordered, orderTopLevelDocForReply(d))
			}
			firstBatch = ordered
		}
		respDoc := bson.M{
			"cursor": bson.M{
				"id":         int64(0),
				"ns":         fmt.Sprintf("%s.%s", cmd["$db"], col),
				"firstBatch": firstBatch,
			},
			"ok": 1.0,
		}
		return h.newMsg(op.Header.RequestID, respDoc)
	}

	// Handle isMaster, hello, etc.
	if _, ok := cmd["isMaster"]; ok || cmd["ismaster"] != nil || cmd["hello"] != nil {
		resp := helloDoc(h.authenticated)
		_ = h.maybeSpeculativeAuth(cmd, resp)
		return h.newMsg(op.Header.RequestID, resp)
	}

	return h.newMsgError(op.Header.RequestID, 59, "CommandNotFound", fmt.Sprintf("unsupported command in OpMsg: %v", cmd))
}
