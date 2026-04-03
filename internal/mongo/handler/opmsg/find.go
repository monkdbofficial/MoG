package opmsg

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"gopkg.in/mgo.v2/bson"

	"mog/internal/logging"
	mongopath "mog/internal/mongo"
	"mog/internal/mongo/handler/relational"
	"mog/internal/mongo/handler/shared"
	mpipeline "mog/internal/mongo/pipeline"
)

// CmdFind is a helper used by the adapter.
func CmdFind(deps Deps, ctx context.Context, requestID int32, cmd bson.M) ([]byte, bool, error) {
	col, ok := cmd["find"].(string)
	if !ok {
		return nil, false, nil
	}
	start := time.Now()

	dbName := deps.CommandDB(cmd)
	physical, err := deps.PhysicalCollectionName(dbName, col)
	if err != nil {
		mongopath.LogQuery(mongopath.QueryLogOptions{
			Stage:         mongopath.RequestStageComplete,
			Method:        "find",
			Operation:     "find",
			QueryName:     "find",
			Table:         "",
			Rows:          0,
			Error:         err,
			TotalDuration: time.Since(start),
			StartedAt:     start,
		})
		resp, rerr := deps.NewMsgError(requestID, 2, "BadValue", err.Error())
		return resp, true, rerr
	}
	_ = deps.CatalogUpsert(ctx, dbName, col)

	filter := bson.M{}
	if rawFilter, ok := cmd["filter"]; ok {
		if m, ok := shared.CoerceBsonM(rawFilter); ok {
			filter = m
		}
	}

	sortSpec := bson.M{}
	if rawSort, ok := cmd["sort"]; ok {
		if m, ok := shared.CoerceBsonM(rawSort); ok {
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
	fieldOrderByID := map[string][]string{}
	pushedDown := false

	// Preserve Mongo array semantics: exclude `$in` fields from SQL pushdown, then apply the full filter in-memory.
	pushdownFilter := filter
	needPostFilter := false
	if shared.FilterHasOperator(filter, "$in") {
		pushdownFilter = shared.StripOperatorKeys(filter, "$in")
		needPostFilter = len(pushdownFilter) != len(filter)
	}

	if len(pushdownFilter) > 0 || len(sortSpec) > 0 || skip > 0 || limit > 0 {
		where, ok, err := relational.BuildWhere(pushdownFilter)
		if err != nil {
			mongopath.LogQuery(mongopath.QueryLogOptions{
				Stage:         mongopath.RequestStageComplete,
				Method:        "find",
				Operation:     "find",
				QueryName:     "find",
				Table:         physical,
				Rows:          0,
				Error:         err,
				TotalDuration: time.Since(start),
				StartedAt:     start,
			})
			return nil, true, err
		}
		orderBy, ok2, err := relational.BuildOrderBy(sortSpec)
		if err != nil {
			mongopath.LogQuery(mongopath.QueryLogOptions{
				Stage:         mongopath.RequestStageComplete,
				Method:        "find",
				Operation:     "find",
				QueryName:     "find",
				Table:         physical,
				Rows:          0,
				Error:         err,
				TotalDuration: time.Since(start),
				StartedAt:     start,
			})
			return nil, true, err
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
			q = rewriteSelectStarQuery(ctx, deps, physical, q)
			pdocs, err := deps.LoadSQLDocsWithIDsQry(ctx, q, args...)
			if err == nil {
				docs = make([]bson.M, 0, len(pdocs))
				for _, pd := range pdocs {
					docs = append(docs, pd.Doc)
					fieldOrderByID[fmt.Sprint(pd.Doc["_id"])] = pd.FieldOrder
				}
				pushedDown = true
			}
		}
	}

	if !pushedDown {
		pdocs, err := deps.LoadSQLDocsWithIDs(ctx, physical)
		if err != nil {
			mongopath.LogQuery(mongopath.QueryLogOptions{
				Stage:         mongopath.RequestStageComplete,
				Method:        "find",
				Operation:     "find",
				QueryName:     "find",
				Table:         physical,
				Rows:          0,
				Error:         err,
				TotalDuration: time.Since(start),
				StartedAt:     start,
			})
			return nil, true, err
		}
		docs = make([]bson.M, 0, len(pdocs))
		for _, pd := range pdocs {
			docs = append(docs, pd.Doc)
			fieldOrderByID[fmt.Sprint(pd.Doc["_id"])] = pd.FieldOrder
		}
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
		if err := deps.NormalizeDocForReply(ctx, d); err != nil {
			mongopath.LogQuery(mongopath.QueryLogOptions{
				Stage:         mongopath.RequestStageComplete,
				Method:        "find",
				Operation:     "find",
				QueryName:     "find",
				Table:         physical,
				Rows:          0,
				Error:         err,
				TotalDuration: time.Since(start),
				StartedAt:     start,
			})
			return nil, true, err
		}
	}

	if logging.Logger() != nil {
		logging.Logger().Debug("find result", zap.String("db", dbName), zap.String("coll", col), zap.String("physical", physical), zap.Int("returned", len(docs)))
	}

	ordered := make([]interface{}, 0, len(docs))
	for _, d := range docs {
		ordered = append(ordered, deps.OrderTopLevelDocForReply(d, fieldOrderByID[fmt.Sprint(d["_id"])]))
	}
	var firstBatch interface{} = ordered

	respDoc := bson.M{
		"cursor": bson.M{
			"id":         int64(0),
			"ns":         fmt.Sprintf("%s.%s", cmd["$db"], col),
			"firstBatch": firstBatch,
		},
		"ok": 1.0,
	}

	resp, err := deps.NewMsg(requestID, respDoc)
	mongopath.LogQuery(mongopath.QueryLogOptions{
		Stage:         mongopath.RequestStageComplete,
		Method:        "find",
		Operation:     "find",
		QueryName:     "find",
		Table:         physical,
		Rows:          len(docs),
		Error:         err,
		TotalDuration: time.Since(start),
		StartedAt:     start,
	})
	return resp, true, err
}
