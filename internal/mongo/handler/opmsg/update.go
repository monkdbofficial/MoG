package opmsg

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"gopkg.in/mgo.v2/bson"

	"mog/internal/logging"
	"mog/internal/mongo/handler/shared"
)

func CmdUpdate(deps Deps, ctx context.Context, requestID int32, cmd bson.M) ([]byte, bool, error) {
	col, ok := cmd["update"].(string)
	if !ok {
		return nil, false, nil
	}

	dbName := deps.CommandDB(cmd)
	physical, err := deps.PhysicalCollectionName(dbName, col)
	if err != nil {
		resp, rerr := deps.NewMsgError(requestID, 2, "BadValue", err.Error())
		return resp, true, rerr
	}
	_ = deps.CatalogUpsert(ctx, dbName, col)

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
		return nil, true, fmt.Errorf("updates must be an array")
	}

	// Use a single DB transaction for the whole update command to avoid connection conflicts
	// under high concurrency.
	tx := deps.CurrentTx()
	ownedTx := false
	if tx == nil {
		tx, err = deps.BeginTx(ctx)
		if err != nil {
			return nil, true, err
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
		updateDoc, ok := shared.CoerceBsonM(u)
		if !ok {
			continue
		}

		filter := bson.M{}
		if q, ok := updateDoc["q"]; ok {
			if m, ok := shared.CoerceBsonM(q); ok {
				filter = m
			}
		}
		update := bson.M{}
		if uu, ok := updateDoc["u"]; ok {
			if m, ok := shared.CoerceBsonM(uu); ok {
				update = m
			}
		}
		multi, _ := updateDoc["multi"].(bool)
		upsert, _ := updateDoc["upsert"].(bool)

		matched := 0
		modified := 0
		var upsertedID interface{} = nil
		var err error
		matched, modified, upsertedID, err = deps.ApplyPureSQLUpdate(ctx, tx, physical, filter, update, multi, upsert)
		if err != nil {
			if deps.IsUndefinedRelation(err) || deps.IsUndefinedSchema(err) {
				deps.ClearSchemaCache(physical)
				if err := deps.EnsureCollectionTable(ctx, physical); err != nil {
					return nil, true, err
				}
				matched, modified, upsertedID, err = deps.ApplyPureSQLUpdate(ctx, tx, physical, filter, update, multi, upsert)
			}
			if err != nil {
				return nil, true, err
			}
		}
		nMatched += matched
		nModified += modified
		if matched > 0 || modified > 0 {
			deps.MarkTouched(physical)
		}

		if upsertedID != nil {
			upserted = append(upserted, bson.M{"index": idx, "_id": upsertedID})
			deps.MarkTouched(physical)
		}
	}

	if ownedTx {
		if err := tx.Commit(ctx); err != nil {
			return nil, true, err
		}
	}

	deps.RefreshTouched(ctx)
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
		if deps.LogWriteInfo {
			logging.Logger().Info("mongo update",
				zap.String("remote_addr", deps.RemoteAddr(ctx)),
				zap.Int32("request_id", requestID),
				zap.String("db", dbName),
				zap.String("coll", col),
				zap.Int("nMatched", nMatched),
				zap.Int("nModified", nModified),
				zap.Int("nUpserted", len(upserted)),
			)
		}
	}

	respDoc := bson.M{
		"n":         nMatched,
		"nModified": nModified,
		"ok":        1.0,
	}
	if len(upserted) > 0 {
		respDoc["upserted"] = upserted
	}

	resp, err := deps.NewMsg(requestID, respDoc)
	return resp, true, err
}
