package mongo

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/handler/relational"
	"mog/internal/mongo/handler/shared"
	mpipeline "mog/internal/mongo/pipeline"
	mupdate "mog/internal/mongo/update"
)

func (h *Handler) applyDynamicUpdate(ctx context.Context, tx pgx.Tx, physical string, filter bson.M, update bson.M, multi bool) (matched int, modified int, err error) {
	if h.pool == nil {
		return 0, 0, fmt.Errorf("database pool is not configured")
	}

	selectSQL, args, err := h.translator.TranslateFind(physical, filter)
	if err != nil {
		return 0, 0, err
	}
	if !multi {
		selectSQL = strings.TrimSpace(selectSQL) + " LIMIT 1"
	}
	// Best-effort row locking for concurrent updates.
	selectSQL = strings.TrimSpace(selectSQL)
	// selectSQL = strings.TrimSpace(selectSQL) + " FOR UPDATE"

	args, err = normalizeSQLArgs(args)
	if err != nil {
		return 0, 0, err
	}

	rows, err := tx.Query(ctx, selectSQL, args...)
	if err != nil {
		return 0, 0, err
	}
	// IMPORTANT: pgx connections cannot run Exec/Query concurrently.
	// Read matching docs first, then close rows, then issue UPDATE statements.
	type foundDoc struct {
		id  string
		doc bson.M
	}
	var found []foundDoc
	for rows.Next() {
		var v interface{}
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return 0, 0, err
		}
		doc, okDoc := shared.CoerceBsonM(v)
		if !okDoc {
			continue
		}
		id, ok := doc["_id"]
		if !ok || id == nil {
			continue
		}
		found = append(found, foundDoc{id: stringifyID(id), doc: doc})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, 0, err
	}

	matched = len(found)
	for _, fd := range found {
		newDoc, _ := mupdate.ApplyUpdate(fd.doc, update)
		normalizeDocForStorage(newDoc)

		if err := h.offloadBlobsInDoc(ctx, tx, physical, fd.id, newDoc); err != nil {
			return 0, 0, err
		}

		docJSON, err := shared.MarshalObject(newDoc)
		if err != nil {
			return 0, 0, err
		}

		updateSQL := fmt.Sprintf("UPDATE doc.%s SET data = CAST($1 AS OBJECT(DYNAMIC)) WHERE data['_id'] = $2", physical)
		tag, err := tx.Exec(ctx, updateSQL, docJSON, fd.id)
		if err != nil {
			return 0, 0, err
		}
		modified += int(tag.RowsAffected())
	}
	return matched, modified, nil
}

func stringifyID(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case bson.ObjectId:
		return t.Hex()
	default:
		return fmt.Sprint(v)
	}
}

func (h *Handler) applyPureSQLUpdate(ctx context.Context, exec DBExecutor, physical string, filter bson.M, update bson.M, multi bool, upsert bool) (matched int, modified int, upsertedID interface{}, err error) {
	if h.pool == nil {
		return 0, 0, nil, fmt.Errorf("database pool is not configured")
	}
	if exec == nil {
		return 0, 0, nil, fmt.Errorf("db executor is nil")
	}
	if physical == "" {
		return 0, 0, nil, fmt.Errorf("empty collection")
	}

	// Fast-path: the common PyMongo bulk_write(ReplaceOne(..., upsert=True)) pattern
	// uses filter {"_id": <id>} and a full-document replacement update.
	//
	// The generic path loads *all* docs and does in-memory matching, which becomes
	// quadratic for large collections. For _id-equality, we can push down existence
	// checks and only read/modify a single row.
	if !multi && len(filter) == 1 {
		if rawID, ok := filter["_id"]; ok {
			storageID, docID, okID, err := canonicalStorageIDAndDocID(rawID)
			if err != nil {
				return 0, 0, nil, err
			}
			if okID {
				isReplacement := isReplacementUpdateDoc(update)
				if isReplacement {
					newDoc := cloneBsonM(update)
					if _, hasID := newDoc["_id"]; !hasID {
						newDoc["_id"] = storageID
					}
					normalizeDocForStorage(newDoc)
					newDoc["_id"] = storageID

					if err := h.ensureCollectionTableExec(ctx, exec, physical); err != nil {
						return 0, 0, nil, err
					}

					exists, err := h.docExistsByID(ctx, exec, physical, docID)
					if err != nil {
						return 0, 0, nil, err
					}
					if !exists {
						if !upsert {
							return 0, 0, nil, nil
						}
						if dupMsg, dupName, ok := h.checkUniqueViolation(ctx, physical, docID, newDoc, ""); ok {
							return 0, 0, nil, fmt.Errorf("E11000 duplicate key error collection: %s index: %s dup key: %s", physical, dupName, dupMsg)
						}
						if err := h.insertRowFromDoc(ctx, exec, physical, docID, newDoc); err != nil {
							return 0, 0, nil, err
						}
						return 0, 0, newDoc["_id"], nil
					}

					// Best-effort modified semantics: compare with existing doc if we can load it cheaply.
					if pds, err := h.loadSQLDocsWithIDsQuery(ctx, exec, "SELECT * FROM doc."+physical+" WHERE id = $1", docID); err == nil && len(pds) > 0 {
						if reflect.DeepEqual(pds[0].doc, newDoc) {
							modified = 0
						} else {
							modified = 1
						}
					} else {
						modified = 1
					}

					if dupMsg, dupName, ok := h.checkUniqueViolation(ctx, physical, docID, newDoc, docID); ok {
						return 1, modified, nil, fmt.Errorf("E11000 duplicate key error collection: %s index: %s dup key: %s", physical, dupName, dupMsg)
					}
					if err := h.updateRowFromDoc(ctx, exec, physical, docID, newDoc); err != nil {
						return 1, modified, nil, err
					}
					return 1, modified, nil, nil
				}

				// Operator update fast-path (e.g. $set) for {"_id": ...} filters: load one doc,
				// apply update in-memory, write back.
				if err := h.ensureCollectionTableExec(ctx, exec, physical); err != nil {
					return 0, 0, nil, err
				}
				exists, err := h.docExistsByID(ctx, exec, physical, docID)
				if err != nil {
					return 0, 0, nil, err
				}
				if !exists {
					if !upsert {
						return 0, 0, nil, nil
					}
					insDoc := mupdate.BuildUpsertBaseDoc(filter)
					insDoc, _ = mupdate.ApplyUpdate(insDoc, update)
					normalizeDocForStorage(insDoc)
					insDoc["_id"] = storageID
					if dupMsg, dupName, ok := h.checkUniqueViolation(ctx, physical, docID, insDoc, ""); ok {
						return 0, 0, nil, fmt.Errorf("E11000 duplicate key error collection: %s index: %s dup key: %s", physical, dupName, dupMsg)
					}
					if err := h.insertRowFromDoc(ctx, exec, physical, docID, insDoc); err != nil {
						return 0, 0, nil, err
					}
					return 0, 0, insDoc["_id"], nil
				}

				pds, err := h.loadSQLDocsWithIDsQuery(ctx, exec, "SELECT * FROM doc."+physical+" WHERE id = $1", docID)
				if err != nil {
					return 0, 0, nil, err
				}
				if len(pds) == 0 {
					return 0, 0, nil, nil
				}
				old := pds[0].doc
				newDoc, _ := mupdate.ApplyUpdate(old, update)
				normalizeDocForStorage(newDoc)
				newDoc["_id"] = storageID

				if !reflect.DeepEqual(old, newDoc) {
					modified = 1
				}
				if dupMsg, dupName, ok := h.checkUniqueViolation(ctx, physical, docID, newDoc, docID); ok {
					return 1, modified, nil, fmt.Errorf("E11000 duplicate key error collection: %s index: %s dup key: %s", physical, dupName, dupMsg)
				}
				if err := h.updateRowFromDoc(ctx, exec, physical, docID, newDoc); err != nil {
					return 1, modified, nil, err
				}
				return 1, modified, nil, nil
			}
		}
	}

	// Use SQL as a coarse pre-filter when we can, then preserve Mongo semantics in-memory.
	pdocs, residualFilter, err := h.loadCandidateSQLDocsWithIDs(ctx, exec, physical, filter, !multi)
	if err != nil {
		return 0, 0, nil, err
	}

	var targets []pureSQLDoc
	for _, pd := range pdocs {
		if len(residualFilter) == 0 || mpipeline.MatchDoc(pd.doc, residualFilter) {
			targets = append(targets, pd)
			if !multi {
				break
			}
		}
	}

	matched = len(targets)
	if matched == 0 {
		if !upsert {
			return 0, 0, nil, nil
		}

		insDoc := mupdate.BuildUpsertBaseDoc(filter)
		insDoc, _ = mupdate.ApplyUpdate(insDoc, update)
		normalizeDocForStorage(insDoc)
		docID, err := encodeDocID(insDoc["_id"])
		if err != nil {
			return 0, 0, nil, err
		}

		if err := h.ensureCollectionTable(ctx, physical); err != nil {
			return 0, 0, nil, err
		}
		if dupMsg, dupName, ok := h.checkUniqueViolation(ctx, physical, docID, insDoc, ""); ok {
			return 0, 0, nil, fmt.Errorf("E11000 duplicate key error collection: %s index: %s dup key: %s", physical, dupName, dupMsg)
		}
		if err := h.insertRowFromDoc(ctx, exec, physical, docID, insDoc); err != nil {
			return 0, 0, nil, err
		}
		return 0, 0, insDoc["_id"], nil
	}

	for _, t := range targets {
		fullDocs, err := h.loadSQLDocsWithIDsQuery(ctx, exec, "SELECT "+strings.Join(orderSelectColumns(mustListColumnsExec(ctx, h, exec, physical)), ", ")+" FROM doc."+physical+" WHERE id = $1", t.docID)
		if err != nil {
			return matched, modified, nil, err
		}
		if len(fullDocs) == 0 {
			continue
		}
		oldDoc := fullDocs[0].doc
		newDoc, _ := mupdate.ApplyUpdate(oldDoc, update)
		normalizeDocForStorage(newDoc)
		// Ensure _id remains stable even if an update tries to change it.
		if oldID, ok := oldDoc["_id"]; ok {
			newDoc["_id"] = oldID
		}

		if !reflect.DeepEqual(oldDoc, newDoc) {
			modified++
		}

		if dupMsg, dupName, ok := h.checkUniqueViolation(ctx, physical, t.docID, newDoc, t.docID); ok {
			return matched, modified, nil, fmt.Errorf("E11000 duplicate key error collection: %s index: %s dup key: %s", physical, dupName, dupMsg)
		}
		if err := h.updateRowFromDoc(ctx, exec, physical, t.docID, newDoc); err != nil {
			return matched, modified, nil, err
		}
	}

	return matched, modified, nil, nil
}

func (h *Handler) loadCandidateSQLDocsWithIDs(ctx context.Context, exec DBExecutor, physical string, filter bson.M, single bool) ([]pureSQLDoc, bson.M, error) {
	pushdown, err := relational.BuildFilterPushdown(filter)
	if err != nil {
		return nil, nil, err
	}
	if len(pushdown.PushedFilter) == 0 || pushdown.Where == nil || strings.TrimSpace(pushdown.Where.SQL) == "" {
		pdocs, err := h.loadSQLDocsWithIDs(ctx, exec, physical)
		return pdocs, filter, err
	}

	selectList, err := h.candidateSelectList(ctx, exec, physical, pushdown.ResidualFilter)
	if err != nil {
		return nil, nil, err
	}
	query := "SELECT " + selectList + " FROM doc." + physical + " WHERE " + pushdown.Where.SQL
	if single && len(pushdown.ResidualFilter) == 0 {
		query += " LIMIT 1"
	}

	pdocs, err := h.loadSQLDocsWithIDsQuery(ctx, exec, query, pushdown.Where.Args...)
	if err == nil {
		return pdocs, pushdown.ResidualFilter, nil
	}
	pdocs, err = h.loadSQLDocsWithIDs(ctx, exec, physical)
	return pdocs, filter, err
}

func (h *Handler) candidateSelectList(ctx context.Context, exec DBExecutor, physical string, filter bson.M) (string, error) {
	_ = ctx
	_ = exec
	_ = physical
	needed := map[string]struct{}{"id": {}}
	if h.storeRawMongoJSON {
		needed["data"] = struct{}{}
	}
	for _, field := range filterFieldRoots(filter) {
		col := shared.SQLColumnNameForField(field)
		if col != "" {
			needed[col] = struct{}{}
		}
	}
	out := make([]string, 0, len(needed))
	for _, col := range orderNeededColumns(sortedNeededColumns(needed)) {
		out = append(out, col)
	}
	if len(out) == 0 {
		out = append(out, "id")
	}
	return strings.Join(out, ", "), nil
}

func filterFieldRoots(filter bson.M) []string {
	roots := map[string]struct{}{}
	var walk func(bson.M)
	walk = func(m bson.M) {
		for k, v := range m {
			if strings.HasPrefix(k, "$") {
				if sub, ok := shared.CoerceBsonM(v); ok {
					walk(sub)
				}
				continue
			}
			root := strings.Split(k, ".")[0]
			if root != "" && root != "_id" {
				roots[root] = struct{}{}
			}
			if cond, ok := shared.CoerceBsonM(v); ok && mpipeline.DocHasOperatorKeys(cond) {
				walk(cond)
			}
		}
	}
	walk(filter)
	out := make([]string, 0, len(roots))
	for root := range roots {
		out = append(out, root)
	}
	sort.Strings(out)
	return out
}

func mustListColumnsExec(ctx context.Context, h *Handler, exec DBExecutor, physical string) []string {
	cols, err := h.listColumnsExec(ctx, exec, physical)
	if err != nil || len(cols) == 0 {
		return []string{"id"}
	}
	return cols
}

func sortedNeededColumns(cols map[string]struct{}) []string {
	out := make([]string, 0, len(cols))
	for col := range cols {
		out = append(out, col)
	}
	sort.Strings(out)
	return out
}

func orderNeededColumns(cols []string) []string {
	available := map[string]struct{}{}
	for _, col := range cols {
		available[col] = struct{}{}
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(cols))
	appendCol := func(col string) {
		if col == "" {
			return
		}
		if _, ok := available[col]; !ok {
			return
		}
		if _, ok := seen[col]; ok {
			return
		}
		seen[col] = struct{}{}
		out = append(out, col)
	}
	appendCol("id")
	appendCol("data")
	for _, col := range cols {
		if col == "id" || col == "data" {
			continue
		}
		appendCol(col)
	}
	return out
}

func isReplacementUpdateDoc(update bson.M) bool {
	for k := range update {
		if strings.HasPrefix(k, "$") {
			return false
		}
	}
	return true
}

func cloneBsonM(in bson.M) bson.M {
	if in == nil {
		return bson.M{}
	}
	out := make(bson.M, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func canonicalStorageIDAndDocID(rawID interface{}) (storageID interface{}, docID string, ok bool, err error) {
	if rawID == nil {
		return nil, "", false, nil
	}
	// Coerce Extended JSON wrappers if the client sent them as an object.
	if m, okM := shared.CoerceBsonM(rawID); okM {
		if vv, okV := coerceExtendedJSONValue(m, false); okV {
			rawID = vv
		}
	}
	// Keep storage ids stable with normalizeDocForStorage behavior (ObjectId -> hex string).
	if oid, okOID := rawID.(bson.ObjectId); okOID {
		rawID = oid.Hex()
	}
	docID, err = encodeDocID(rawID)
	if err != nil {
		return nil, "", false, err
	}
	return rawID, docID, true, nil
}
