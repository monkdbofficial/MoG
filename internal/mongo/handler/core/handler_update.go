package mongo

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"
	"gopkg.in/mgo.v2/bson"

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

	// Load all docs and apply the match/update in-memory for maximum Mongo compatibility.
	pdocs, err := h.loadSQLDocsWithIDs(ctx, exec, physical)
	if err != nil {
		return 0, 0, nil, err
	}

	var targets []pureSQLDoc
	for _, pd := range pdocs {
		if mpipeline.MatchDoc(pd.doc, filter) {
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
		newDoc, _ := mupdate.ApplyUpdate(t.doc, update)
		normalizeDocForStorage(newDoc)
		// Ensure _id remains stable even if an update tries to change it.
		if oldID, ok := t.doc["_id"]; ok {
			newDoc["_id"] = oldID
		}

		if !reflect.DeepEqual(t.doc, newDoc) {
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
