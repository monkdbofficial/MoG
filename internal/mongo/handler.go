package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
	"go.uber.org/zap"
	"gopkg.in/mgo.v2/bson"

	"mog/internal/metrics"

	"mog/internal/logging"
	"mog/internal/translator"
)

const (
	reportedMongoVersion = "8.0.0"
	reportedFCV          = "8.0"
	reportedMaxWire      = 25
	catalogCollection    = "__monkdb_catalog"
)

// Handler handles MongoDB operations.
type Handler struct {
	pool       *pgxpool.Pool
	translator *translator.Translator
	tx         pgx.Tx
	touched    map[string]struct{}

	storeRawMongoJSON bool
	logWriteInfo      bool
	stableFieldOrder  bool

	// scram conversation
	scram         *ScramSha256
	scramConv     *Conversation
	authenticated bool
}

// NewHandler creates a new Handler.
func NewHandler(pool *pgxpool.Pool, t *translator.Translator, scram *ScramSha256) *Handler {
	return &Handler{
		pool:              pool,
		translator:        t,
		scram:             scram,
		touched:           map[string]struct{}{},
		storeRawMongoJSON: envBool("MOG_STORE_RAW_MONGO_JSON", false),
		stableFieldOrder:  envBool("MOG_STABLE_FIELD_ORDER", false),
		// Minimal info-level write logging is opt-in to avoid performance overhead under high QPS.
		logWriteInfo: envBool("MOG_INFO_LOG_WRITES", false),
	}
}

func envBool(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		return def
	}
}

func (h *Handler) db() DBExecutor {
	if h.tx != nil {
		return h.tx
	}
	return h.pool
}

func (h *Handler) markTouched(physical string) {
	if physical == "" {
		return
	}
	if h.touched == nil {
		h.touched = map[string]struct{}{}
	}
	h.touched[physical] = struct{}{}
}

func (h *Handler) refreshTouched(ctx context.Context) {
	if h.pool == nil || h.tx != nil || len(h.touched) == 0 {
		return
	}
	for physical := range h.touched {
		h.refreshCollection(ctx, physical)
	}
	// reset
	h.touched = map[string]struct{}{}
}

func (h *Handler) refreshCollection(ctx context.Context, physical string) {
	if h.pool == nil || physical == "" {
		return
	}
	_ = h.ensureDocSchema(ctx)
	// Best-effort: MonkDB/Crate-style backends are near-real-time; refresh makes writes visible to reads.
	if _, err := h.pool.Exec(ctx, "REFRESH TABLE doc."+physical); err != nil {
		// Don't fail the request if refresh isn't supported; just log at debug.
		if logging.Logger() != nil {
			logging.Logger().Debug("refresh table failed", zap.String("physical", physical), zap.Error(err))
		}
	}
}

type DBExecutor interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
}

// Handle handles a single MongoDB operation.
func (h *Handler) Handle(ctx context.Context, header MsgHeader, body []byte) ([]byte, error) {
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

func (h *Handler) handleMsg(ctx context.Context, header MsgHeader, body []byte) ([]byte, error) {
	op, err := ParseOpMsg(header, body)
	if err != nil {
		return nil, fmt.Errorf("failed to read OpMsg: %w", err)
	}

	if len(op.Sections) == 0 {
		return nil, fmt.Errorf("OpMsg must have at least one section")
	}

	bodySection, ok := op.Sections[0].(SectionBody)
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
		seq, ok := sec.(SectionDocumentSequence)
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

func (h *Handler) handleQuery(ctx context.Context, header MsgHeader, body []byte) ([]byte, error) {
	start := time.Now()
	op, err := ParseOpQuery(header, body)
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

func (h *Handler) handleOpMsgCommand(ctx context.Context, op *OpMsg, cmd bson.M) ([]byte, error) {
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
			if i, err := asInt(rawLimit); err == nil {
				limit = i
			}
		}
		skip := 0
		if rawSkip, ok := cmd["skip"]; ok {
			if i, err := asInt(rawSkip); err == nil {
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
				docs = applyMatch(docs, filter)
			}
			if len(sortSpec) > 0 {
				docs = applySort(docs, sortSpec)
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
			docs = applyMatch(docs, filter)
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
							baseDocs = applyMatch(baseDocs, query)
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
			baseDocs = applyMatch(baseDocs, query)
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
				if i, err := asInt(rawLimit); err == nil {
					limit = i
				}
			}

			pdocs, err := h.loadSQLDocsWithIDs(ctx, h.db(), physical)
			if err != nil {
				return nil, err
			}
			var delIDs []string
			for _, pd := range pdocs {
				if matchDoc(pd.doc, filter) {
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

		outDocs, err := applyPipelineWithLookup(baseDocs, pipelineDocs, resolveLookup)
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

func (h *Handler) newMsg(requestID int32, doc bson.M) ([]byte, error) {
	bytes, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}

	op := &OpMsg{
		Header: MsgHeader{
			RequestID:  0, // We could increment this
			ResponseTo: requestID,
			OpCode:     wiremessage.OpMsg,
		},
		FlagBits: 0,
		Sections: []Section{SectionBody{Document: bytes}},
	}

	return op.Marshal()
}

func (h *Handler) newMsgError(requestID int32, code int32, codeName, errmsg string) ([]byte, error) {
	return h.newMsg(requestID, bson.M{
		"ok":       0.0,
		"errmsg":   errmsg,
		"code":     code,
		"codeName": codeName,
	})
}

func (h *Handler) newReply(requestID int32, doc bson.M) ([]byte, error) {
	bytes, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}

	op := &OpReply{
		Header: MsgHeader{
			RequestID:  0,
			ResponseTo: requestID,
			OpCode:     wiremessage.OpReply,
		},
		ResponseFlags:  0,
		CursorID:       0,
		StartingFrom:   0,
		NumberReturned: 1,
		Documents:      [][]byte{bytes},
	}

	return op.Marshal()
}

func (h *Handler) newReplyError(requestID int32, code int32, codeName, errmsg string) ([]byte, error) {
	return h.newReply(requestID, bson.M{
		"ok":       0.0,
		"errmsg":   errmsg,
		"code":     code,
		"codeName": codeName,
	})
}

func hostInfoDoc() bson.M {
	hostname, _ := os.Hostname()
	return bson.M{
		"system": bson.M{
			"hostname": hostname,
			"cpuArch":  runtime.GOARCH,
			"os": bson.M{
				"type": runtime.GOOS,
				"name": runtime.GOOS,
			},
		},
		"ok": 1.0,
	}
}

func helloDoc(authenticated bool) bson.M {
	doc := bson.M{
		"ismaster":                     true,
		"isWritablePrimary":            true,
		"helloOk":                      true,
		"maxBsonObjectSize":            16777216,
		"maxMessageSizeBytes":          48000000,
		"maxWriteBatchSize":            100000,
		"localTime":                    time.Now(),
		"logicalSessionTimeoutMinutes": 30,
		"minWireVersion":               0,
		"maxWireVersion":               reportedMaxWire,
		"readOnly":                     false,
		"ok":                           1.0,
	}
	if !authenticated {
		doc["saslSupportedMechs"] = []string{"SCRAM-SHA-256"}
	}
	return doc
}

func (h *Handler) maybeSpeculativeAuth(cmd bson.M, resp bson.M) error {
	if h.authenticated || h.scramConv != nil {
		return nil
	}
	raw, ok := cmd["speculativeAuthenticate"]
	if !ok || raw == nil {
		return nil
	}
	spec, ok := coerceBsonM(raw)
	if !ok {
		return nil
	}
	if _, ok := spec["saslStart"]; !ok {
		return nil
	}
	// Only SCRAM-SHA-256 is supported currently.
	mech, _ := spec["mechanism"].(string)
	if mech != "" && mech != "SCRAM-SHA-256" {
		return nil
	}
	payload, err := payloadFromCommand(spec)
	if err != nil {
		return nil
	}

	h.scramConv = h.scram.Start()
	serverFirst, err := h.scramConv.Step(payload)
	if err != nil {
		// If speculative auth fails, don't fail hello; client will retry with normal saslStart.
		h.scramConv = nil
		return nil
	}
	resp["speculativeAuthenticate"] = bson.M{
		"conversationId": 1,
		"done":           false,
		"payload":        []byte(serverFirst),
		"ok":             1.0,
	}
	return nil
}

func serverStatusDoc() bson.M {
	hostname, _ := os.Hostname()
	now := time.Now()
	return bson.M{
		"host":           hostname,
		"version":        reportedMongoVersion,
		"process":        "mongod",
		"pid":            int64(0),
		"uptime":         float64(0),
		"uptimeMillis":   int64(0),
		"uptimeEstimate": float64(0),
		"localTime":      now,
		"connections": bson.M{
			"current":      int32(0),
			"available":    int32(1000),
			"totalCreated": int64(0),
		},
		"ok": 1.0,
	}
}

func cmdLineOptsDoc() bson.M {
	return bson.M{
		"argv":   []string{},
		"parsed": bson.M{},
		"ok":     1.0,
	}
}

func payloadFromCommand(cmd bson.M) (string, error) {
	payload, ok := cmd["payload"]
	if !ok {
		return "", fmt.Errorf("missing SCRAM payload")
	}

	switch p := payload.(type) {
	case string:
		return p, nil
	case []byte:
		return string(p), nil
	case bson.Binary:
		return string(p.Data), nil
	default:
		return "", fmt.Errorf("unsupported SCRAM payload type: %T", payload)
	}
}

func (h *Handler) ensureDocSchema(ctx context.Context) error {
	if h.pool == nil {
		return nil
	}
	_, err := h.pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS doc")
	return err
}

func (h *Handler) ensureDocSchemaExec(ctx context.Context, exec DBExecutor) error {
	if exec == nil {
		return nil
	}
	_, err := exec.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS doc")
	return err
}

type pureSQLDoc struct {
	docID string
	doc   bson.M
}

func decodeDocID(docID string) interface{} {
	if docID == "" {
		return nil
	}
	var v interface{}
	if err := json.Unmarshal([]byte(docID), &v); err == nil {
		return v
	}
	// Legacy ids were stored as raw strings (not JSON string literals).
	return docID
}

func encodeDocID(v interface{}) (string, error) {
	if v == nil {
		return "", fmt.Errorf("_id is nil")
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (h *Handler) loadSQLDocsWithIDs(ctx context.Context, exec DBExecutor, physical string) ([]pureSQLDoc, error) {
	if exec == nil {
		return nil, fmt.Errorf("db executor is nil")
	}
	if physical == "" {
		return []pureSQLDoc{}, nil
	}
	return h.loadSQLDocsWithIDsQuery(ctx, exec, "SELECT * FROM doc."+physical)
}

func (h *Handler) loadSQLDocsWithIDsQuery(ctx context.Context, exec DBExecutor, query string, args ...interface{}) ([]pureSQLDoc, error) {
	if exec == nil {
		return nil, fmt.Errorf("db executor is nil")
	}
	if strings.TrimSpace(query) == "" {
		return []pureSQLDoc{}, nil
	}

	rows, err := exec.Query(ctx, query, args...)
	if err != nil {
		if isUndefinedRelation(err) || isUndefinedSchema(err) {
			return []pureSQLDoc{}, nil
		}
		return nil, err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	numFields := len(fields)
	out := make([]pureSQLDoc, 0, 16) // Pre-allocate small capacity
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		relFields := bson.M{}
		var rawDoc bson.M
		docID := ""
		for i := 0; i < numFields; i++ {
			if i >= len(vals) {
				continue
			}
			fd := fields[i]
			col := string(fd.Name)
			if col == "" {
				continue
			}
			val := vals[i]
			if col == "id" {
				if val != nil {
					docID = fmt.Sprint(val)
				}
				continue
			}
			if col == "data" {
				// When enabled, `data` stores the raw Mongo document (including arrays).
				// Keep it internal: only used for reconstruction and uniqueness checks.
				if h.storeRawMongoJSON && val != nil {
					if m, ok := coerceBsonM(val); ok {
						rawDoc = m
					}
				}
				continue
			}
			if val == nil {
				continue
			}
			field := mongoFieldNameForColumn(col)
			if field == "" || field == "_id" {
				continue
			}
			relFields[field] = normalizeRowValue(val)
		}
		if docID == "" && rawDoc != nil {
			if id, ok := rawDoc["_id"]; ok {
				docID, _ = encodeDocID(id)
			}
		}

		if docID == "" {
			continue
		}
		doc := rawDoc
		if doc == nil {
			doc = bson.M{}
		}
		doc["_id"] = decodeDocID(docID)
		for k, v := range relFields {
			if k == "" || k == "_id" {
				continue
			}
			// Some backends represent NULL object columns as `{}`. If we have a raw doc and it
			// contains a non-empty object for this field, don't overwrite it with an empty object.
			if rawDoc != nil {
				switch vv := v.(type) {
				case bson.M:
					if len(vv) == 0 {
						if existing, ok := rawDoc[k].(bson.M); ok && len(existing) > 0 {
							continue
						}
						if existing, ok := rawDoc[k].(map[string]interface{}); ok && len(existing) > 0 {
							continue
						}
					}
				case map[string]interface{}:
					if len(vv) == 0 {
						if existing, ok := rawDoc[k].(bson.M); ok && len(existing) > 0 {
							continue
						}
						if existing, ok := rawDoc[k].(map[string]interface{}); ok && len(existing) > 0 {
							continue
						}
					}
				}
			}
			doc[k] = v
		}
		out = append(out, pureSQLDoc{docID: docID, doc: doc})
	}
	return out, nil
}

func (h *Handler) loadSQLDocs(ctx context.Context, physical string) ([]bson.M, error) {
	// For performance and safety under high throughput, never load more than 1000 docs
	// if pushdown failed. In-memory filtering/sorting is extremely slow.
	pdocs, err := h.loadSQLDocsWithIDsQuery(ctx, h.db(), "SELECT * FROM doc."+physical+" LIMIT 1000")
	if err != nil {
		return nil, err
	}
	out := make([]bson.M, 0, len(pdocs))
	for _, pd := range pdocs {
		out = append(out, pd.doc)
	}
	return out, nil
}

func (h *Handler) ensureCollectionTable(ctx context.Context, collection string) error {
	if h.pool == nil {
		return fmt.Errorf("database pool is not configured")
	}
	return h.ensureCollectionTableExec(ctx, h.pool, collection)
}

func (h *Handler) ensureCollectionTableExec(ctx context.Context, exec DBExecutor, collection string) error {
	if exec == nil {
		return fmt.Errorf("db executor is nil")
	}
	if !isSafeIdentifier(collection) {
		return fmt.Errorf("invalid collection name: %s", collection)
	}
	// Schema is cached per-process and shared across connections/handlers.
	// If `MOG_STORE_RAW_MONGO_JSON` is toggled at runtime, ensure the optional
	// `data` column still gets added even when the base table is already cached
	// as initialized.
	if h.schemaCache().isInitialized(collection) {
		// Cache can get stale if the backend schema was modified by a different process
		// (or if `data` existed from an older run). Refresh the cache for this table once
		// to detect `data` so write paths can keep it in sync.
		if !h.schemaCache().hasColumn(collection, "data") {
			if cols, err := h.listColumnsExec(ctx, exec, collection); err == nil {
				for _, c := range cols {
					if c != "" {
						h.schemaCache().setColumn(collection, c, "UNKNOWN")
					}
				}
			}
		}
		if h.storeRawMongoJSON && !h.schemaCache().hasColumn(collection, "data") {
			lock := ddlLockForTable(collection)
			lock.Lock()
			defer lock.Unlock()
			if _, err := exec.Exec(ctx, "ALTER TABLE doc."+collection+" ADD COLUMN data OBJECT(DYNAMIC)"); err != nil && !isDuplicateColumnName(err) {
				return err
			}
			h.schemaCache().setColumn(collection, "data", "OBJECT(DYNAMIC)")
		}
		return nil
	}

	// Ensure the backing schema exists (fresh MonkDB instances may not have it yet).
	_ = h.ensureDocSchemaExec(ctx, exec)

	// Always create/upgrade to relational storage. Optional raw-doc storage is controlled by MOG_STORE_RAW_MONGO_JSON.
	if _, err := exec.Exec(ctx, "CREATE TABLE IF NOT EXISTS doc."+collection+" (id TEXT PRIMARY KEY)"); err != nil {
		return err
	}
	// Best-effort upgrade path for existing tables.
	if _, err := exec.Exec(ctx, "ALTER TABLE doc."+collection+" ADD COLUMN id TEXT"); err != nil && !isDuplicateColumnName(err) {
		return err
	}
	// If a legacy `data` column exists, backfill ids.
	if _, err := exec.Exec(ctx, "UPDATE doc."+collection+" SET id = data['_id'] WHERE id IS NULL AND data['_id'] IS NOT NULL"); err != nil && !isUndefinedColumn(err) {
		// Best-effort: ignore if the table doesn't have a legacy `data` column.
	}
	if h.storeRawMongoJSON {
		if _, err := exec.Exec(ctx, "ALTER TABLE doc."+collection+" ADD COLUMN data OBJECT(DYNAMIC)"); err != nil && !isDuplicateColumnName(err) {
			return err
		}
	}

	// Cache schema for this process to avoid repeated DDL.
	h.schemaCache().markInitialized(collection)
	h.schemaCache().setColumn(collection, "id", "TEXT")
	if h.storeRawMongoJSON {
		h.schemaCache().setColumn(collection, "data", "OBJECT(DYNAMIC)")
	}
	if cols, err := h.listColumnsExec(ctx, exec, collection); err == nil {
		for _, c := range cols {
			if c != "" {
				h.schemaCache().setColumn(collection, c, "UNKNOWN")
			}
		}
	}
	return nil
}

func (h *Handler) ensureColumn(ctx context.Context, physical string, col string, sqlType string) error {
	if h.pool == nil {
		return fmt.Errorf("database pool is not configured")
	}
	return h.ensureColumnExec(ctx, h.pool, physical, col, sqlType)
}

func (h *Handler) ensureColumnExec(ctx context.Context, exec DBExecutor, physical string, col string, sqlType string) error {
	if exec == nil {
		return fmt.Errorf("db executor is nil")
	}
	if physical == "" || col == "" || sqlType == "" {
		return nil
	}
	if h.schemaCache().hasColumn(physical, col) {
		return nil
	}
	if strings.HasPrefix(col, "_") || !isSafeIdentifier(col) {
		return fmt.Errorf("field %q is not supported as a SQL column name", col)
	}
	if _, err := exec.Exec(ctx, fmt.Sprintf("ALTER TABLE doc.%s ADD COLUMN %s %s", physical, col, sqlType)); err != nil {
		if isDuplicateColumnName(err) {
			h.schemaCache().setColumn(physical, col, sqlType)
			return nil
		}
		return err
	}
	h.schemaCache().setColumn(physical, col, sqlType)
	return nil
}

func (h *Handler) listColumns(ctx context.Context, physical string) ([]string, error) {
	if h.pool == nil || physical == "" {
		return nil, nil
	}
	var rows pgx.Rows
	var err error
	for _, q := range []string{
		"SELECT column_name FROM information_schema.columns WHERE table_schema = 'doc' AND table_name = $1",
		"SELECT column_name FROM information_schema.columns WHERE table_schema_name = 'doc' AND table_name = $1",
		"SELECT column_name FROM information_schema.columns WHERE schema_name = 'doc' AND table_name = $1",
	} {
		rows, err = h.pool.Query(ctx, q, physical)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err == nil && c != "" {
			cols = append(cols, c)
		}
	}
	return cols, nil
}

func (h *Handler) updateRowFromDoc(ctx context.Context, exec DBExecutor, physical string, docID string, doc bson.M) error {
	if exec == nil || physical == "" || docID == "" {
		return nil
	}
	if err := h.ensureCollectionTableExec(ctx, exec, physical); err != nil {
		return err
	}

	// Ensure columns exist for any new fields.
	for k, v := range doc {
		if k == "" || k == "_id" {
			continue
		}
		col := sqlColumnNameForField(k)
		if col == "" || col == "id" || col == "data" {
			continue
		}
		sqlType := sqlTypeForValue(v)
		if sqlType == "" {
			continue
		}
		if err := h.ensureColumnExec(ctx, exec, physical, col, sqlType); err != nil {
			return err
		}
	}

	cols, err := h.listColumnsExec(ctx, exec, physical)
	if err != nil {
		return err
	}

	setParts := make([]string, 0, len(cols))
	args := make([]interface{}, 0, len(cols)+1)
	hasDataCol := false
	for _, c := range cols {
		if c == "" || c == "id" {
			continue
		}
		if c == "data" {
			hasDataCol = true
			continue
		}
		field := mongoFieldNameForColumn(c)
		v, exists := doc[field]
		if !exists || v == nil {
			setParts = append(setParts, fmt.Sprintf("%s = NULL", c))
			continue
		}

		sqlType := sqlTypeForValue(v)
		switch sqlType {
		case "OBJECT(DYNAMIC)":
			js, err := marshalObject(v)
			if err != nil {
				return err
			}
			args = append(args, js)
			setParts = append(setParts, fmt.Sprintf("%s = CAST($%d AS OBJECT(DYNAMIC))", c, len(args)))
		case "DOUBLE PRECISION":
			if f, ok := toFloat64Match(v); ok {
				args = append(args, f)
			} else {
				args = append(args, v)
			}
			setParts = append(setParts, fmt.Sprintf("%s = $%d", c, len(args)))
		default:
			if strings.HasPrefix(sqlType, "FLOAT_VECTOR(") {
				lit, _, ok := floatVectorLiteral(v)
				if !ok {
					return fmt.Errorf("invalid FLOAT_VECTOR value for field %q", field)
				}
				setParts = append(setParts, fmt.Sprintf("%s = %s", c, lit))
				continue
			}
			// For TEXT columns, encode arrays/objects as JSON so they can be rehydrated on reads.
			if sqlType == "TEXT" {
				if _, ok := coerceInterfaceSlice(v); ok {
					js, err := marshalObject(v)
					if err != nil {
						return err
					}
					args = append(args, js)
					setParts = append(setParts, fmt.Sprintf("%s = $%d", c, len(args)))
					continue
				}
				if _, ok := coerceBsonM(v); ok {
					js, err := marshalObject(v)
					if err != nil {
						return err
					}
					args = append(args, js)
					setParts = append(setParts, fmt.Sprintf("%s = $%d", c, len(args)))
					continue
				}
			}
			args = append(args, v)
			setParts = append(setParts, fmt.Sprintf("%s = $%d", c, len(args)))
		}
	}

	// Nothing to update besides id.
	if len(setParts) == 0 {
		return nil
	}
	args = append(args, docID)
	sql := fmt.Sprintf("UPDATE doc.%s SET %s WHERE id = $%d", physical, strings.Join(setParts, ", "), len(args))
	if _, err := exec.Exec(ctx, sql, args...); err != nil {
		return err
	}

	// Raw `data` sync: do it as a separate UPDATE. Some backends/versions behave
	// inconsistently when updating many typed columns and an OBJECT/JSON column
	// in the same statement.
	wantRaw := h.storeRawMongoJSON || hasDataCol
	if wantRaw {
		docJSON, err := marshalObject(doc)
		if err != nil {
			return err
		}
		if _, err := exec.Exec(ctx, "UPDATE doc."+physical+" SET data = CAST($1 AS OBJECT(DYNAMIC)) WHERE id = $2", docJSON, docID); err != nil {
			if isUndefinedColumn(err) {
				return nil
			}
			// Best-effort fallback for backends that store raw docs as JSONB.
			if _, err2 := exec.Exec(ctx, "UPDATE doc."+physical+" SET data = CAST($1 AS JSONB) WHERE id = $2", docJSON, docID); err2 == nil {
				return nil
			}
			return err
		}
	}
	return nil
}

func (h *Handler) insertRowFromDoc(ctx context.Context, exec DBExecutor, physical string, docID string, doc bson.M) error {
	if exec == nil || physical == "" || docID == "" {
		return nil
	}
	if err := h.ensureCollectionTableExec(ctx, exec, physical); err != nil {
		return err
	}

	storeRaw := h.storeRawMongoJSON || h.schemaCache().hasColumn(physical, "data")

	cols := []string{"id"}
	exprs := []string{"$1"}
	args := []interface{}{docID}

	if storeRaw {
		cols = append(cols, "data")
		docJSON, err := marshalObject(doc)
		if err != nil {
			return err
		}
		args = append(args, docJSON)
		exprs = append(exprs, fmt.Sprintf("CAST($%d AS OBJECT(DYNAMIC))", len(args)))
	}

	var keys []string
	for k := range doc {
		if k == "" || k == "_id" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := doc[k]
		col := sqlColumnNameForField(k)
		if col == "" || col == "id" || col == "data" {
			continue
		}
		sqlType := sqlTypeForValue(v)
		if sqlType == "" {
			continue
		}
		if err := h.ensureColumnExec(ctx, exec, physical, col, sqlType); err != nil {
			return err
		}
		cols = append(cols, col)
		if v == nil {
			exprs = append(exprs, "NULL")
			continue
		}
		switch sqlType {
		case "OBJECT(DYNAMIC)":
			js, err := marshalObject(v)
			if err != nil {
				return err
			}
			args = append(args, js)
			exprs = append(exprs, fmt.Sprintf("CAST($%d AS OBJECT(DYNAMIC))", len(args)))
		case "DOUBLE PRECISION":
			if f, ok := toFloat64Match(v); ok {
				args = append(args, f)
			} else {
				args = append(args, v)
			}
			exprs = append(exprs, fmt.Sprintf("$%d", len(args)))
		default:
			if strings.HasPrefix(sqlType, "FLOAT_VECTOR(") {
				lit, _, ok := floatVectorLiteral(v)
				if !ok {
					return fmt.Errorf("invalid FLOAT_VECTOR value for field %q", k)
				}
				exprs = append(exprs, lit)
				continue
			}
			// For TEXT columns, encode arrays/objects as JSON so they can be rehydrated on reads.
			if sqlType == "TEXT" {
				if _, ok := coerceInterfaceSlice(v); ok {
					js, err := marshalObject(v)
					if err != nil {
						return err
					}
					args = append(args, js)
					exprs = append(exprs, fmt.Sprintf("$%d", len(args)))
					continue
				}
				if _, ok := coerceBsonM(v); ok {
					js, err := marshalObject(v)
					if err != nil {
						return err
					}
					args = append(args, js)
					exprs = append(exprs, fmt.Sprintf("$%d", len(args)))
					continue
				}
			}
			args = append(args, v)
			exprs = append(exprs, fmt.Sprintf("$%d", len(args)))
		}
	}

	sql := fmt.Sprintf("INSERT INTO doc.%s (%s) VALUES (%s)", physical, strings.Join(cols, ", "), strings.Join(exprs, ", "))
	_, err := exec.Exec(ctx, sql, args...)
	if err == nil {
		return nil
	}
	if storeRaw && isUndefinedColumn(err) {
		// Backend doesn't have the `data` column; retry without it.
		cols = []string{"id"}
		exprs = []string{"$1"}
		args = []interface{}{docID}
		sql = fmt.Sprintf("INSERT INTO doc.%s (%s) VALUES (%s)", physical, strings.Join(cols, ", "), strings.Join(exprs, ", "))
		_, err2 := exec.Exec(ctx, sql, args...)
		return err2
	}
	return err
}

func (h *Handler) dropCollectionTable(ctx context.Context, collection string) error {
	if h.pool == nil {
		return fmt.Errorf("database pool is not configured")
	}
	if !isSafeIdentifier(collection) {
		return fmt.Errorf("invalid collection name: %s", collection)
	}
	_ = h.ensureDocSchema(ctx)
	// If the table is dropped, clear the in-process schema cache so subsequent writes
	// don't incorrectly assume the relation still exists.
	h.schemaCache().clear(collection)
	_, err := h.pool.Exec(ctx, "DROP TABLE IF EXISTS doc."+collection)
	return err
}

func (h *Handler) dropDatabase(ctx context.Context, dbName string) error {
	if h.pool == nil {
		return nil
	}
	if dbName == "" {
		return nil
	}
	if !isSafeIdentifier(dbName) {
		return fmt.Errorf("invalid database name: %s", dbName)
	}

	// Clear any in-memory unique index definitions for this logical db.
	h.clearUniqueIndexesForDB(dbName)
	// Clear any in-process schema cache entries for this logical db so subsequent writes
	// can recreate missing relations.
	h.schemaCache().clearDB(dbName)

	colls, err := h.catalogListCollections(ctx, dbName)
	if err != nil {
		return nil
	}

	for _, logical := range colls {
		physical, err := physicalCollectionName(dbName, logical)
		if err != nil {
			continue
		}
		_ = h.dropCollectionTable(ctx, physical)
	}
	return nil
}

var (
	physicalNameCache = sync.Map{} // (dbName, collection) -> physical
)

func physicalCollectionName(dbName, collection string) (string, error) {
	if dbName == "" && collection == "" {
		return "", nil
	}
	key := dbName + ":" + collection
	if v, ok := physicalNameCache.Load(key); ok {
		return v.(string), nil
	}

	// When a driver supplies "$db", use it to namespace physical tables so multiple Mongo "databases"
	// don't collide on the same backend schema.
	var physical string
	if dbName == "" {
		if !isSafeIdentifier(collection) {
			return "", fmt.Errorf("invalid collection name: %s", collection)
		}
		physical = collection
	} else {
		if !isSafeIdentifier(dbName) {
			return "", fmt.Errorf("invalid database name: %s", dbName)
		}
		if !isSafeIdentifier(collection) {
			return "", fmt.Errorf("invalid collection name: %s", collection)
		}
		physical = dbName + "__" + collection
	}
	physicalNameCache.Store(key, physical)
	return physical, nil
}

func (h *Handler) ensureCatalogTable(ctx context.Context) error {
	if h.pool == nil {
		return nil
	}
	// Avoid repeated DDL/backfills on hot paths by caching init per-process.
	// NOTE: if the catalog table is dropped externally, call sites should clear this cache
	// (we do this on undefined-relation errors) so ensureCatalogTable runs again.
	if h.schemaCache().isInitialized(catalogCollection) {
		return nil
	}

	lock := ddlLockForTable(catalogCollection)
	lock.Lock()
	defer lock.Unlock()
	if h.schemaCache().isInitialized(catalogCollection) {
		return nil
	}

	_ = h.ensureDocSchema(ctx)

	// v2 schema (no PRIMARY KEY): some MonkDB/Crate-like backends reject PRIMARY KEY syntax.
	// We rely on best-effort dedupe in catalogUpsert instead of strict constraints.
	if _, err := h.pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS doc."+catalogCollection+" (id TEXT, db TEXT, coll TEXT, data OBJECT(DYNAMIC))"); err != nil {
		return err
	}

	// Best-effort upgrade path for old schemas (which only had `data`).
	for _, stmt := range []string{
		"ALTER TABLE doc." + catalogCollection + " ADD COLUMN id TEXT",
		"ALTER TABLE doc." + catalogCollection + " ADD COLUMN db TEXT",
		"ALTER TABLE doc." + catalogCollection + " ADD COLUMN coll TEXT",
		"ALTER TABLE doc." + catalogCollection + " ADD COLUMN data OBJECT(DYNAMIC)",
	} {
		if _, err := h.pool.Exec(ctx, stmt); err != nil && !isDuplicateColumnName(err) {
			// Ignore missing relation/schema; some backends return different errors mid-upgrade.
			if !isUndefinedRelation(err) && !isUndefinedSchema(err) {
				return err
			}
		}
	}

	// Backfill `db`, `coll`, `id` from legacy `data` when possible.
	if _, err := h.pool.Exec(ctx, "UPDATE doc."+catalogCollection+" SET db = data['db'] WHERE db IS NULL AND data['db'] IS NOT NULL"); err != nil && !isUndefinedColumn(err) {
	}
	if _, err := h.pool.Exec(ctx, "UPDATE doc."+catalogCollection+" SET coll = data['coll'] WHERE coll IS NULL AND data['coll'] IS NOT NULL"); err != nil && !isUndefinedColumn(err) {
	}
	if _, err := h.pool.Exec(ctx, "UPDATE doc."+catalogCollection+" SET id = db || '__' || coll WHERE id IS NULL AND db IS NOT NULL AND coll IS NOT NULL"); err != nil && !isUndefinedColumn(err) {
	}

	// Cache schema for this process to avoid repeated DDL.
	h.schemaCache().markInitialized(catalogCollection)
	h.schemaCache().setColumn(catalogCollection, "id", "TEXT")
	h.schemaCache().setColumn(catalogCollection, "db", "TEXT")
	h.schemaCache().setColumn(catalogCollection, "coll", "TEXT")
	h.schemaCache().setColumn(catalogCollection, "data", "OBJECT(DYNAMIC)")
	return nil
}

func (h *Handler) catalogUpsert(ctx context.Context, dbName, collection string) error {
	if h.pool == nil {
		return nil
	}
	if dbName == "" || collection == "" {
		return nil
	}
	if !isSafeIdentifier(dbName) {
		return fmt.Errorf("invalid database name: %s", dbName)
	}
	if !isSafeIdentifier(collection) {
		return fmt.Errorf("invalid collection name: %s", collection)
	}

	if err := h.ensureCatalogTable(ctx); err != nil {
		// Catalog is derived; if it can't be created right now, still keep a process-local record
		// so listCollections/listDatabases work for this running instance.
		globalCatalogCache.add(dbName, collection)
		return nil
	}

	// Process-local best-effort short-circuit to avoid repeated SQL round-trips on hot paths.
	if globalCatalogCache.has(dbName, collection) {
		return nil
	}

	docJSON, err := marshalObject(bson.M{
		"db":   dbName,
		"coll": collection,
	})
	if err != nil {
		return err
	}

	id := dbName + "__" + collection
	tryOnce := func() error {
		// Best-effort dedupe without requiring a UNIQUE index (older catalogs may have duplicates already).
		if _, err := h.pool.Exec(ctx, "DELETE FROM doc."+catalogCollection+" WHERE id = $1", id); err != nil {
			if isUndefinedColumn(err) {
				_, _ = h.pool.Exec(ctx, "DELETE FROM doc."+catalogCollection+" WHERE data['db'] = $1 AND data['coll'] = $2", dbName, collection)
			} else if isUndefinedRelation(err) || isUndefinedSchema(err) {
				return err
			}
		}

		if _, err := h.pool.Exec(ctx, "INSERT INTO doc."+catalogCollection+" (id, db, coll, data) VALUES ($1, $2, $3, CAST($4 AS OBJECT(DYNAMIC)))", id, dbName, collection, docJSON); err != nil {
			return err
		}
		return nil
	}

	if err := tryOnce(); err != nil {
		if isUndefinedRelation(err) || isUndefinedSchema(err) {
			// Catalog was dropped or schema cache is stale; clear and retry once.
			h.schemaCache().clear(catalogCollection)
			if err2 := h.ensureCatalogTable(ctx); err2 == nil {
				if err3 := tryOnce(); err3 == nil {
					globalCatalogCache.add(dbName, collection)
					return nil
				} else {
					err = err3
				}
			}
		}
		// Back-compat: older catalogs might not have the v2 columns yet.
		if isUndefinedColumn(err) {
			if _, err2 := h.pool.Exec(ctx, "INSERT INTO doc."+catalogCollection+" (data) VALUES (CAST($1 AS OBJECT))", docJSON); err2 != nil {
				// Catalog is best-effort derived metadata. Never fail writes because catalog persistence failed.
				// Still record the collection in-process so listCollections/listDatabases keep working.
				globalCatalogCache.add(dbName, collection)
				return nil
			}
			globalCatalogCache.add(dbName, collection)
			return nil
		}
		// Uniqueness is enforced at the storage layer; treat conflicts as success.
		if isUniqueViolation(err) {
			globalCatalogCache.add(dbName, collection)
			return nil
		}
		// Catalog is best-effort derived metadata. Never fail writes because catalog persistence failed.
		// Still record the collection in-process so listCollections/listDatabases keep working.
		globalCatalogCache.add(dbName, collection)
		return nil
	}
	globalCatalogCache.add(dbName, collection)
	return nil
}

func (h *Handler) catalogRemoveCollection(ctx context.Context, dbName, collection string) error {
	if h.pool == nil {
		return nil
	}
	if dbName == "" || collection == "" {
		return nil
	}
	if err := h.ensureCatalogTable(ctx); err != nil {
		return err
	}
	if _, err := h.pool.Exec(ctx, "DELETE FROM doc."+catalogCollection+" WHERE db = $1 AND coll = $2", dbName, collection); err != nil {
		if isUndefinedColumn(err) {
			_, err = h.pool.Exec(ctx, "DELETE FROM doc."+catalogCollection+" WHERE data['db'] = $1 AND data['coll'] = $2", dbName, collection)
		}
		if err != nil {
			return err
		}
	}
	globalCatalogCache.remove(dbName, collection)
	return nil
}

func (h *Handler) catalogRemoveDatabase(ctx context.Context, dbName string) error {
	if h.pool == nil {
		return nil
	}
	if dbName == "" {
		return nil
	}
	if err := h.ensureCatalogTable(ctx); err != nil {
		return err
	}
	if _, err := h.pool.Exec(ctx, "DELETE FROM doc."+catalogCollection+" WHERE db = $1", dbName); err != nil {
		if isUndefinedColumn(err) {
			_, err = h.pool.Exec(ctx, "DELETE FROM doc."+catalogCollection+" WHERE data['db'] = $1", dbName)
		}
		if err != nil {
			return err
		}
	}
	globalCatalogCache.clearDB(dbName)
	return nil
}

func (h *Handler) catalogListDatabases(ctx context.Context) ([]string, error) {
	if h.pool == nil {
		return []string{"admin"}, nil
	}
	if err := h.ensureCatalogTable(ctx); err != nil {
		// Fall back to listing from backend tables if the catalog table can't be created (e.g. dialect mismatch).
		if tables, _ := h.listDocTables(ctx); len(tables) > 0 {
			seen := map[string]bool{"admin": true}
			out := []string{"admin"}
			for _, name := range tables {
				parts := strings.SplitN(name, "__", 2)
				if len(parts) != 2 {
					continue
				}
				db := parts[0]
				if db == "" || seen[db] {
					continue
				}
				seen[db] = true
				out = append(out, db)
			}
			return out, nil
		}
		// Final fallback: include process-local cache so clients can see DBs created during this run.
		seen := map[string]bool{"admin": true}
		out := []string{"admin"}
		for _, db := range globalCatalogCache.listDBs() {
			if db == "" || seen[db] {
				continue
			}
			seen[db] = true
			out = append(out, db)
		}
		return out, nil
	}

	for attempt := 0; attempt < 2; attempt++ {
		// Prefer the v2 schema columns to avoid decoding `data`.
		rows, err := h.pool.Query(ctx, "SELECT db FROM doc."+catalogCollection)
		if err != nil {
			if isUndefinedRelation(err) || isUndefinedSchema(err) {
				// Catalog was dropped; clear cache and retry by rebuilding the table.
				h.schemaCache().clear(catalogCollection)
				_ = h.ensureCatalogTable(ctx)
				continue
			}
			if !isUndefinedColumn(err) {
				return []string{}, err
			}
			rows, err = h.pool.Query(ctx, "SELECT data FROM doc."+catalogCollection)
			if err != nil {
				return []string{}, err
			}
		}

		seen := map[string]bool{"admin": true}
		out := []string{"admin"}
		for rows.Next() {
			var db string
			var v interface{}
			if err := rows.Scan(&v); err != nil {
				// Best-effort: ignore scan errors and keep going.
				continue
			}
			switch t := v.(type) {
			case string:
				db = t
			case []byte:
				db = string(t)
			default:
				data, okDoc := coerceBsonM(v)
				if !okDoc {
					continue
				}
				db, _ = data["db"].(string)
			}
			if db == "" || seen[db] {
				continue
			}
			seen[db] = true
			out = append(out, db)
		}
		rows.Close()

		// Merge in DBs from actual backend tables to avoid catalog inconsistencies.
		if tables, _ := h.listDocTables(ctx); len(tables) > 0 {
			for _, name := range tables {
				parts := strings.SplitN(name, "__", 2)
				if len(parts) != 2 {
					continue
				}
				db := parts[0]
				if db == "" || seen[db] {
					continue
				}
				seen[db] = true
				out = append(out, db)
			}
		}

		if len(out) > 1 || attempt == 1 {
			// Merge in process-local catalog cache (helps when SQL catalog is missing/broken).
			for _, db := range globalCatalogCache.listDBs() {
				if db != "" && !seen[db] {
					seen[db] = true
					out = append(out, db)
				}
			}
			return out, nil
		}
		// Catalog may be empty after upgrade; try best-effort backfill from existing backend tables.
		_ = h.catalogBackfillFromTables(ctx)
	}
	return []string{"admin"}, nil
}

func (h *Handler) relationalWhereAndArgs(ctx context.Context, physical string, filter bson.M) (string, []interface{}, error) {
	// Relational/promotion mode removed. All pure-SQL filtering is evaluated in-memory over KV-reconstructed documents.
	return "", nil, fmt.Errorf("relational mode is not supported")
}

func relationalTagsCondition(v interface{}, argCount *int, physical string) (string, []interface{}, error) {
	return "", nil, fmt.Errorf("tags table is not supported")
}

func (h *Handler) catalogListCollections(ctx context.Context, dbName string) ([]string, error) {
	if h.pool == nil {
		return []string{}, nil
	}
	if dbName == "" {
		return []string{}, nil
	}
	if !isSafeIdentifier(dbName) {
		return []string{}, fmt.Errorf("invalid database name: %s", dbName)
	}
	if err := h.ensureCatalogTable(ctx); err != nil {
		// Fall back to listing from backend tables if the catalog table can't be created (e.g. dialect mismatch).
		if tables, _ := h.listDocTables(ctx); len(tables) > 0 {
			seen := map[string]bool{}
			out := []string{}
			prefix := dbName + "__"
			for _, name := range tables {
				if !strings.HasPrefix(name, prefix) {
					continue
				}
				coll := strings.TrimPrefix(name, prefix)
				if coll == "" || coll == catalogCollection || seen[coll] {
					continue
				}
				seen[coll] = true
				out = append(out, coll)
			}
			if len(out) > 0 {
				return out, nil
			}
		}
		// Final fallback: process-local catalog cache (collections seen during this server run).
		return globalCatalogCache.listCollections(dbName), nil
	}

	for attempt := 0; attempt < 2; attempt++ {
		existing := map[string]bool{}
		if tables, _ := h.listDocTables(ctx); len(tables) > 0 {
			prefix := dbName + "__"
			for _, name := range tables {
				if !strings.HasPrefix(name, prefix) {
					continue
				}
				coll := strings.TrimPrefix(name, prefix)
				if coll == "" || coll == catalogCollection {
					continue
				}
				existing[coll] = true
			}
		}

		// Prefer the v2 schema columns to avoid decoding `data`.
		rows, err := h.pool.Query(ctx, "SELECT coll FROM doc."+catalogCollection+" WHERE db = $1", dbName)
		if err != nil {
			if isUndefinedRelation(err) || isUndefinedSchema(err) {
				// Catalog was dropped; clear cache and retry by rebuilding the table.
				h.schemaCache().clear(catalogCollection)
				_ = h.ensureCatalogTable(ctx)
				continue
			}
			if !isUndefinedColumn(err) {
				return []string{}, err
			}
			rows, err = h.pool.Query(ctx, "SELECT data FROM doc."+catalogCollection+" WHERE data['db'] = $1", dbName)
			if err != nil {
				return []string{}, err
			}
		}

		seen := make(map[string]bool)
		var out []string
		for rows.Next() {
			var coll string
			var v interface{}
			if err := rows.Scan(&v); err != nil {
				// Best-effort: ignore scan errors and keep going.
				continue
			}
			switch t := v.(type) {
			case string:
				coll = t
			case []byte:
				coll = string(t)
			default:
				data, okDoc := coerceBsonM(v)
				if !okDoc {
					continue
				}
				coll, _ = data["coll"].(string)
			}
			if coll == "" || coll == catalogCollection {
				continue
			}
			if len(existing) > 0 && !existing[coll] {
				continue
			}
			if coll == "" || seen[coll] {
				continue
			}
			seen[coll] = true
			out = append(out, coll)
		}
		rows.Close()

		// If the v2 columns exist but are NULL/unbackfilled, the query above can return 0 rows
		// without error. Fall back to the legacy `data` scan in that case.
		if len(out) == 0 {
			legacyRows, lerr := h.pool.Query(ctx, "SELECT data FROM doc."+catalogCollection+" WHERE data['db'] = $1", dbName)
			if lerr == nil {
				for legacyRows.Next() {
					var v interface{}
					if err := legacyRows.Scan(&v); err != nil {
						continue
					}
					data, okDoc := coerceBsonM(v)
					if !okDoc {
						continue
					}
					coll, _ := data["coll"].(string)
					if coll == "" || coll == catalogCollection || seen[coll] {
						continue
					}
					if len(existing) > 0 && !existing[coll] {
						continue
					}
					seen[coll] = true
					out = append(out, coll)
				}
				legacyRows.Close()
			}
		}

		// Merge in collections from actual backend tables to avoid catalog inconsistencies.
		if tables, _ := h.listDocTables(ctx); len(tables) > 0 {
			prefix := dbName + "__"
			for _, name := range tables {
				if !strings.HasPrefix(name, prefix) {
					continue
				}
				coll := strings.TrimPrefix(name, prefix)
				if coll == "" || coll == catalogCollection || seen[coll] {
					continue
				}
				seen[coll] = true
				out = append(out, coll)
			}
		}

		if len(out) > 0 || attempt == 1 {
			// Merge in process-local catalog cache (helps when SQL catalog is missing/broken).
			for _, coll := range globalCatalogCache.listCollections(dbName) {
				if coll != "" && coll != catalogCollection && !seen[coll] {
					seen[coll] = true
					out = append(out, coll)
				}
			}
			return out, nil
		}
		_ = h.catalogBackfillFromTables(ctx)
	}
	return []string{}, nil
}

func (h *Handler) catalogRebuildFromTables(ctx context.Context) error {
	if h.pool == nil {
		return nil
	}
	_ = h.ensureDocSchema(ctx)
	// Catalog is derived; a rebuild is safe and also allows us to re-create the table
	// with a proper primary key even if it previously existed without one.
	if _, err := h.pool.Exec(ctx, "DROP TABLE IF EXISTS doc."+catalogCollection); err != nil {
		// ignore
	}
	if _, err := h.pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS doc."+catalogCollection+" (id TEXT, db TEXT, coll TEXT, data OBJECT(DYNAMIC))"); err != nil {
		return err
	}
	globalCatalogCache.clearAll()
	return h.catalogBackfillFromTables(ctx)
}

func (h *Handler) applyRelationalUpdate(ctx context.Context, exec DBExecutor, physical string, filter bson.M, update bson.M, multi bool) (matched int, modified int, err error) {
	return 0, 0, fmt.Errorf("relational mode is not supported")
}

func (h *Handler) catalogBackfillFromTables(ctx context.Context) error {
	if h.pool == nil {
		return nil
	}
	if err := h.ensureCatalogTable(ctx); err != nil {
		return err
	}

	tables, err := h.listDocTables(ctx)
	if err != nil {
		return nil
	}
	for _, name := range tables {
		parts := strings.SplitN(name, "__", 2)
		if len(parts) != 2 {
			continue
		}
		dbName := parts[0]
		coll := parts[1]
		if coll == "" || coll == catalogCollection {
			continue
		}
		_ = h.catalogUpsert(ctx, dbName, coll)
	}
	return nil
}

func (h *Handler) listDocTables(ctx context.Context) ([]string, error) {
	if h.pool == nil {
		return nil, nil
	}

	queries := []string{
		// CrateDB/MonkDB-like system catalog (often more reliable than information_schema).
		"SELECT name FROM sys.tables WHERE schema_name = 'doc'",
		"SELECT table_name FROM sys.tables WHERE schema_name = 'doc'",
		"SELECT table_name FROM sys.tables WHERE table_schema = 'doc'",
		"SELECT name FROM sys.tables WHERE table_schema = 'doc'",
		// PostgreSQL-ish catalogs (some backends emulate these).
		"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'doc'",
		"SELECT relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'doc' AND c.relkind = 'r'",
		// information_schema variants.
		"SELECT table_name FROM information_schema.tables WHERE table_schema = 'doc'",
		"SELECT table_name FROM information_schema.tables WHERE schema_name = 'doc'",
		"SELECT table_name FROM information_schema.tables WHERE table_schema_name = 'doc'",
	}

	var lastErr error
	sawSuccess := false
	for _, q := range queries {
		rows, err := h.pool.Query(ctx, q)
		if err != nil {
			lastErr = err
			continue
		}
		sawSuccess = true
		var out []string
		for rows.Next() {
			var name string
			if scanErr := rows.Scan(&name); scanErr != nil {
				// Best-effort: ignore scan errors and keep going.
				continue
			}
			if name != "" {
				out = append(out, name)
			}
		}
		rows.Close()
		if len(out) > 0 {
			return out, nil
		}
		// Query succeeded but returned 0 rows; try other catalog sources.
	}

	// If at least one query succeeded (but returned empty), return empty without error.
	if sawSuccess {
		return nil, nil
	}
	// Otherwise, return the last error so callers can decide how to behave.
	return nil, lastErr
}

func marshalObject(v interface{}) (string, error) {
	// Prefer mgo/bson's JSON marshaler so BSON-specific types produced by drivers
	// (e.g. ObjectId, Date, Binary) become valid JSON.
	if b, err := bson.MarshalJSON(v); err == nil {
		return string(b), nil
	}

	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("failed to marshal object: %w", err)
	}
	return string(b), nil
}

// orderTopLevelDocForReply makes field order stable for clients:
// `_id` first, then remaining top-level keys in alphabetical order.
// This avoids expensive recursive ordering on large result batches.
func orderTopLevelDocForReply(m bson.M) bson.D {
	if m == nil {
		return bson.D{}
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		if k == "" || k == "_id" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make(bson.D, 0, len(m))
	if id, ok := m["_id"]; ok {
		out = append(out, bson.DocElem{Name: "_id", Value: id})
	}
	for _, k := range keys {
		out = append(out, bson.DocElem{Name: k, Value: m[k]})
	}
	return out
}

func coerceBsonM(v interface{}) (bson.M, bool) {
	switch t := v.(type) {
	case bson.M:
		return t, true
	case map[string]interface{}:
		return bson.M(t), true
	case bson.D:
		m := bson.M{}
		for _, e := range t {
			m[e.Name] = e.Value
		}
		return m, true
	case []byte:
		var m bson.M
		if err := bson.Unmarshal(t, &m); err == nil {
			return m, true
		}
		if err := bson.UnmarshalJSON(t, &m); err == nil {
			return m, true
		}
		return nil, false
	case string:
		var m bson.M
		if err := bson.UnmarshalJSON([]byte(t), &m); err == nil {
			return m, true
		}
		return nil, false
	default:
		return nil, false
	}
}

func filterHasOperator(filter bson.M, op string) bool {
	for _, v := range filter {
		m, ok := coerceBsonM(v)
		if !ok {
			continue
		}
		if _, ok := m[op]; ok {
			return true
		}
	}
	return false
}

func stripOperatorKeys(filter bson.M, op string) bson.M {
	if len(filter) == 0 {
		return filter
	}
	out := bson.M{}
	for k, v := range filter {
		m, ok := coerceBsonM(v)
		if ok {
			if _, has := m[op]; has {
				// If a field condition uses the operator (even alongside others), drop it from pushdown.
				continue
			}
		}
		out[k] = v
	}
	return out
}

func primaryCommandKey(cmd bson.M) string {
	// Return the "command name" key for logging purposes.
	// Skip metadata keys that frequently appear in command documents.
	skip := map[string]bool{
		"$db":             true,
		"lsid":            true,
		"$clusterTime":    true,
		"txnNumber":       true,
		"autocommit":      true,
		"$readPreference": true,
		"readConcern":     true,
		"writeConcern":    true,
		"client":          true,
	}

	var keys []string
	for k := range cmd {
		if skip[k] {
			continue
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return ""
	}
	sort.Strings(keys)
	return keys[0]
}

func commandDB(cmd bson.M) string {
	if v, ok := cmd["$db"]; ok {
		if s, ok := asString(v); ok {
			return s
		}
	}
	return ""
}

func asString(v interface{}) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case bson.Symbol:
		return string(t), true
	default:
		return "", false
	}
}
