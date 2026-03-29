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
	"mog/internal/mongo/handler/shared"
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
	cmdName := shared.PrimaryCommandKey(cmd)
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
	cmdName := shared.PrimaryCommandKey(cmd)
	defer func() {
		metrics.ObserveMongoCommand("opmsg", cmdName, time.Since(start))
	}()

	if logging.Logger() != nil {
		dbName := shared.CommandDB(cmd)
		keys := make([]string, 0, len(cmd))
		for k := range cmd {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		logging.Logger().Debug("opmsg command", zap.String("db", dbName), zap.String("cmd", shared.PrimaryCommandKey(cmd)), zap.Strings("keys", keys))
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
		dbName := shared.CommandDB(cmd)
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
		dbName := shared.CommandDB(cmd)
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
		dbName := shared.CommandDB(cmd)
		h.clearUniqueIndexesForDB(dbName)
		if err := h.dropDatabase(ctx, dbName); err != nil {
			return h.newMsgError(op.Header.RequestID, 8000, "InternalError", err.Error())
		}
		_ = h.catalogRemoveDatabase(ctx, dbName)
		h.refreshTouched(ctx)
		return h.newMsg(op.Header.RequestID, bson.M{"dropped": dbName, "ok": 1.0})
	}

	if _, ok := cmd["listCollections"]; ok {
		dbName := shared.CommandDB(cmd)
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
		dbName := shared.CommandDB(cmd)
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
						data, okDoc := shared.CoerceBsonM(v)
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
		dbName := shared.CommandDB(cmd)
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
		dbName := shared.CommandDB(cmd)
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
		dbName := shared.CommandDB(cmd)
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
			idxDoc, ok := shared.CoerceBsonM(it)
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
			keyDoc, _ := shared.CoerceBsonM(idxDoc["key"])
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
					c := shared.SQLColumnNameForField(f)
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
				if !shared.IsSafeIdentifier(idxName) {
					idxName = fmt.Sprintf("u_%s_%s", physical, strings.ToLower(shared.FieldB32.EncodeToString([]byte(ui.name))))
				}
				if _, err := h.pool.Exec(ctx, fmt.Sprintf("CREATE UNIQUE INDEX %s ON doc.%s (%s)", idxName, physical, strings.Join(sqlCols, ", "))); err != nil && !isDuplicateObject(err) {
					// Best-effort: still enforce uniqueness in-memory.
				}
			}
		}

		return h.newMsg(op.Header.RequestID, bson.M{"ok": 1.0, "createdCollectionAutomatically": false})
	}

	if _, ok := cmd["dropIndexes"]; ok {
		dbName := shared.CommandDB(cmd)
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

	if resp, ok, err := h.opMsgCmdFind(ctx, op.Header.RequestID, cmd); ok || err != nil {
		return resp, err
	}
	if resp, ok, err := h.opMsgCmdCount(ctx, op.Header.RequestID, cmd); ok || err != nil {
		return resp, err
	}
	if resp, ok, err := h.opMsgCmdInsert(ctx, op.Header.RequestID, cmd); ok || err != nil {
		return resp, err
	}
	if resp, ok, err := h.opMsgCmdUpdate(ctx, op.Header.RequestID, cmd); ok || err != nil {
		return resp, err
	}
	if resp, ok, err := h.opMsgCmdDelete(ctx, op.Header.RequestID, cmd); ok || err != nil {
		return resp, err
	}
	if resp, ok, err := h.opMsgCmdAggregate(ctx, op.Header.RequestID, cmd); ok || err != nil {
		return resp, err
	}

	// Handle isMaster, hello, etc.
	if _, ok := cmd["isMaster"]; ok || cmd["ismaster"] != nil || cmd["hello"] != nil {
		resp := helloDoc(h.authenticated)
		_ = h.maybeSpeculativeAuth(cmd, resp)
		return h.newMsg(op.Header.RequestID, resp)
	}

	return h.newMsgError(op.Header.RequestID, 59, "CommandNotFound", fmt.Sprintf("unsupported command in OpMsg: %v", cmd))
}
