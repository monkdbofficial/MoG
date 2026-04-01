package opmsg

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"gopkg.in/mgo.v2/bson"

	"mog/internal/logging"
	"mog/internal/mongo/handler/relational"
	"mog/internal/mongo/handler/shared"
	mpipeline "mog/internal/mongo/pipeline"
)

func CmdDelete(deps Deps, ctx context.Context, requestID int32, cmd bson.M) ([]byte, bool, error) {
	col, ok := cmd["delete"].(string)
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
		return nil, true, fmt.Errorf("deletes must be an array")
	}

	n := 0
	affected := int64(0)
	for _, d := range deletes {
		deleteDoc, ok := shared.CoerceBsonM(d)
		if !ok {
			continue
		}

		filter := bson.M{}
		if q, ok := deleteDoc["q"]; ok {
			if m, ok := shared.CoerceBsonM(q); ok {
				filter = m
			}
		}

		limit := 0
		if rawLimit, ok := deleteDoc["limit"]; ok {
			if i, err := mpipeline.AsInt(rawLimit); err == nil {
				limit = i
			}
		}

		pdocs, err := loadDeleteCandidateDocs(ctx, deps, physical, filter, limit == 1)
		if err != nil {
			return nil, true, err
		}
		var delIDs []string
		for _, pd := range pdocs {
			if mpipeline.MatchDoc(pd.Doc, filter) {
				delIDs = append(delIDs, pd.DocID)
				if limit == 1 {
					break
				}
			}
		}
		for _, id := range delIDs {
			tag, err := deps.DB().Exec(ctx, "DELETE FROM doc."+physical+" WHERE id = $1", id)
			if err != nil {
				return nil, true, err
			}
			affected += tag.RowsAffected()
			n += int(tag.RowsAffected())
			deps.MarkTouched(physical)
		}
	}
	deps.RefreshTouched(ctx)
	if logging.Logger() != nil {
		logging.Logger().Debug("delete result",
			zap.String("db", dbName),
			zap.String("coll", col),
			zap.String("physical", physical),
			zap.Int64("rows_affected", affected),
		)
		if deps.LogWriteInfo {
			logging.Logger().Info("mongo delete",
				zap.String("remote_addr", deps.RemoteAddr(ctx)),
				zap.Int32("request_id", requestID),
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

	resp, err := deps.NewMsg(requestID, respDoc)
	return resp, true, err
}

func loadDeleteCandidateDocs(ctx context.Context, deps Deps, physical string, filter bson.M, single bool) ([]SQLDoc, error) {
	pushdown, err := relational.BuildFilterPushdown(filter)
	if err != nil {
		return nil, err
	}
	if len(pushdown.PushedFilter) == 0 || pushdown.Where == nil || pushdown.Where.SQL == "" {
		return deps.LoadSQLDocsWithIDs(ctx, physical)
	}

	query := "SELECT * FROM doc." + physical + " WHERE " + pushdown.Where.SQL
	if single && len(pushdown.ResidualFilter) == 0 {
		query += " LIMIT 1"
	}

	pdocs, err := deps.LoadSQLDocsWithIDsQry(ctx, query, pushdown.Where.Args...)
	if err == nil {
		return pdocs, nil
	}
	return deps.LoadSQLDocsWithIDs(ctx, physical)
}
