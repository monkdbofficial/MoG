package opmsg

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"gopkg.in/mgo.v2/bson"

	"mog/internal/logging"
	mongopath "mog/internal/mongo"
)

// CmdInsert is a helper used by the adapter.
func CmdInsert(deps Deps, ctx context.Context, requestID int32, cmd bson.M) ([]byte, bool, error) {
	col, ok := cmd["insert"].(string)
	if !ok {
		return nil, false, nil
	}
	start := time.Now()

	dbName := deps.CommandDB(cmd)
	physical, err := deps.PhysicalCollectionName(dbName, col)
	if err != nil {
		mongopath.LogQuery(mongopath.QueryLogOptions{
			Stage:         mongopath.RequestStageComplete,
			Method:        "insert",
			Operation:     "insert",
			QueryName:     "insert",
			Table:         "",
			Error:         err,
			TotalDuration: time.Since(start),
			StartedAt:     start,
		})
		resp, rerr := deps.NewMsgError(requestID, 2, "BadValue", err.Error())
		return resp, true, rerr
	}
	_ = deps.CatalogUpsert(ctx, dbName, col)

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
		mongopath.LogQuery(mongopath.QueryLogOptions{
			Stage:         mongopath.RequestStageComplete,
			Method:        "insert",
			Operation:     "insert",
			QueryName:     "insert",
			Table:         physical,
			Error:         fmt.Errorf("documents must be an array"),
			TotalDuration: time.Since(start),
			StartedAt:     start,
		})
		return nil, true, fmt.Errorf("documents must be an array")
	}

	seen, inserted, err := deps.InsertMany(ctx, physical, docs)
	if err != nil {
		if strings.HasPrefix(err.Error(), "E11000") {
			resp, rerr := deps.NewMsgError(requestID, 11000, "DuplicateKey", err.Error())
			return resp, true, rerr
		}
		if deps.IsUndefinedRelation(err) || deps.IsUndefinedSchema(err) {
			// Best-effort create and retry once.
			deps.ClearSchemaCache(physical)
			if err := deps.EnsureCollectionTable(ctx, physical); err != nil {
				return nil, true, err
			}
			_ = deps.CatalogUpsert(ctx, dbName, col)
			_, inserted, err = deps.InsertMany(ctx, physical, docs)
			if err != nil {
				if strings.HasPrefix(err.Error(), "E11000") {
					resp, rerr := deps.NewMsgError(requestID, 11000, "DuplicateKey", err.Error())
					return resp, true, rerr
				}
				mongopath.LogQuery(mongopath.QueryLogOptions{
					Stage:         mongopath.RequestStageComplete,
					Method:        "insert",
					Operation:     "insert",
					QueryName:     "insert",
					Table:         physical,
					Error:         err,
					TotalDuration: time.Since(start),
					StartedAt:     start,
				})
				return nil, true, err
			}
		} else {
			mongopath.LogQuery(mongopath.QueryLogOptions{
				Stage:         mongopath.RequestStageComplete,
				Method:        "insert",
				Operation:     "insert",
				QueryName:     "insert",
				Table:         physical,
				Error:         err,
				TotalDuration: time.Since(start),
				StartedAt:     start,
			})
			return nil, true, err
		}
	}

	if inserted > 0 {
		deps.MarkTouched(physical)
	}
	deps.RefreshTouched(ctx)

	n := int(inserted)
	if logging.Logger() != nil {
		logging.Logger().Debug("insert result",
			zap.String("db", dbName),
			zap.String("coll", col),
			zap.String("physical", physical),
			zap.Int("docs_seen", seen),
			zap.Int64("rows_affected", inserted),
		)
		if deps.LogWriteInfo {
			logging.Logger().Info("mongo insert",
				zap.String("remote_addr", deps.RemoteAddr(ctx)),
				zap.Int32("request_id", requestID),
				zap.String("db", dbName),
				zap.String("coll", col),
				zap.Int("n", n),
			)
		}
	}
	respDoc := bson.M{"n": n, "ok": 1.0}

	resp, err := deps.NewMsg(requestID, respDoc)
	mongopath.LogQuery(mongopath.QueryLogOptions{
		Stage:         mongopath.RequestStageComplete,
		Method:        "insert",
		Operation:     "insert",
		QueryName:     "insert",
		Table:         physical,
		RowsAffected:  inserted,
		Error:         err,
		TotalDuration: time.Since(start),
		StartedAt:     start,
	})
	return resp, true, err
}
