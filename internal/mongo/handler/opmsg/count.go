package opmsg

import (
	"context"
	"time"

	"go.uber.org/zap"
	"gopkg.in/mgo.v2/bson"

	"mog/internal/logging"
	mongopath "mog/internal/mongo"
	"mog/internal/mongo/handler/relational"
	"mog/internal/mongo/handler/shared"
	mpipeline "mog/internal/mongo/pipeline"
)

// CmdCount is a helper used by the adapter.
func CmdCount(deps Deps, ctx context.Context, requestID int32, cmd bson.M) ([]byte, bool, error) {
	col, ok := cmd["count"].(string)
	if !ok {
		return nil, false, nil
	}
	start := time.Now()

	dbName := deps.CommandDB(cmd)
	physical, err := deps.PhysicalCollectionName(dbName, col)
	if err != nil {
		mongopath.LogQuery(mongopath.QueryLogOptions{
			Stage:         mongopath.RequestStageComplete,
			Method:        "count",
			Operation:     "count",
			QueryName:     "count",
			Table:         "",
			Error:         err,
			TotalDuration: time.Since(start),
			StartedAt:     start,
		})
		resp, rerr := deps.NewMsgError(requestID, 2, "BadValue", err.Error())
		return resp, true, rerr
	}
	_ = deps.CatalogUpsert(ctx, dbName, col)

	query := bson.M{}
	if rawQuery, ok := cmd["query"]; ok {
		if m, ok := shared.CoerceBsonM(rawQuery); ok {
			query = m
		}
	}

	var n int64
	if len(query) == 0 {
		// Fast path: exact count without scanning.
		if err := deps.DB().QueryRow(ctx, "SELECT COUNT(*) FROM doc."+physical).Scan(&n); err != nil {
			if deps.IsUndefinedRelation(err) || deps.IsUndefinedSchema(err) {
				n = 0
			} else {
				mongopath.LogQuery(mongopath.QueryLogOptions{
					Stage:         mongopath.RequestStageComplete,
					Method:        "count",
					Operation:     "count",
					QueryName:     "count",
					Table:         physical,
					Error:         err,
					TotalDuration: time.Since(start),
					StartedAt:     start,
				})
				return nil, true, err
			}
		}
	} else {
		// Prefer SQL pushdown when safe; preserve Mongo array semantics for `$in` by applying in-memory.
		pushdownQuery := query
		needPostFilter := false
		if shared.FilterHasOperator(query, "$in") {
			pushdownQuery = shared.StripOperatorKeys(query, "$in")
			needPostFilter = len(pushdownQuery) != len(query)
		}
		if len(pushdownQuery) > 0 {
			where, ok, err := relational.BuildWhere(pushdownQuery)
			if err != nil {
				mongopath.LogQuery(mongopath.QueryLogOptions{
					Stage:         mongopath.RequestStageComplete,
					Method:        "count",
					Operation:     "count",
					QueryName:     "count",
					Table:         physical,
					Error:         err,
					TotalDuration: time.Since(start),
					StartedAt:     start,
				})
				return nil, true, err
			}
			if ok && where != nil && where.SQL != "" {
				if !needPostFilter {
					q := "SELECT COUNT(*) FROM doc." + physical + " WHERE " + where.SQL
					if err := deps.DB().QueryRow(ctx, q, where.Args...).Scan(&n); err == nil {
						goto countDone
					}
				} else {
					pdocs, err := deps.LoadSQLDocsWithIDsQry(ctx, "SELECT * FROM doc."+physical+" WHERE "+where.SQL, where.Args...)
					if err == nil {
						baseDocs := make([]bson.M, 0, len(pdocs))
						for _, pd := range pdocs {
							baseDocs = append(baseDocs, pd.Doc)
						}
						baseDocs = mpipeline.ApplyMatch(baseDocs, query)
						n = int64(len(baseDocs))
						goto countDone
					}
				}
			}
		}
		baseDocs, err := deps.LoadSQLDocs(ctx, physical)
		if err != nil {
			mongopath.LogQuery(mongopath.QueryLogOptions{
				Stage:         mongopath.RequestStageComplete,
				Method:        "count",
				Operation:     "count",
				QueryName:     "count",
				Table:         physical,
				Error:         err,
				TotalDuration: time.Since(start),
				StartedAt:     start,
			})
			return nil, true, err
		}
		baseDocs = mpipeline.ApplyMatch(baseDocs, query)
		n = int64(len(baseDocs))
	}

countDone:
	if logging.Logger() != nil {
		logging.Logger().Debug("count result", zap.String("db", dbName), zap.String("coll", col), zap.String("physical", physical), zap.Int64("n", n))
	}
	resp, err := deps.NewMsg(requestID, bson.M{"n": n, "ok": 1.0})
	mongopath.LogQuery(mongopath.QueryLogOptions{
		Stage:         mongopath.RequestStageComplete,
		Method:        "count",
		Operation:     "count",
		QueryName:     "count",
		Table:         physical,
		RowsAffected:  n,
		Error:         err,
		TotalDuration: time.Since(start),
		StartedAt:     start,
	})
	return resp, true, err
}
