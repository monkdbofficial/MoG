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

func CmdFind(deps Deps, ctx context.Context, requestID int32, cmd bson.M) ([]byte, bool, error) {
	col, ok := cmd["find"].(string)
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
			return nil, true, err
		}
		orderBy, ok2, err := relational.BuildOrderBy(sortSpec)
		if err != nil {
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
			pdocs, err := deps.LoadSQLDocsWithIDsQry(ctx, q, args...)
			if err == nil {
				docs = make([]bson.M, 0, len(pdocs))
				for _, pd := range pdocs {
					docs = append(docs, pd.Doc)
				}
				pushedDown = true
			}
		}
	}

	if !pushedDown {
		baseDocs, err := deps.LoadSQLDocs(ctx, physical)
		if err != nil {
			return nil, true, err
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
		deps.NormalizeDocForReply(d)
	}

	if logging.Logger() != nil {
		logging.Logger().Debug("find result", zap.String("db", dbName), zap.String("coll", col), zap.String("physical", physical), zap.Int("returned", len(docs)))
	}

	var firstBatch interface{} = docs
	if deps.StableFieldOrder {
		ordered := make([]interface{}, 0, len(docs))
		for _, d := range docs {
			ordered = append(ordered, deps.OrderTopLevelDocForReply(d))
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

	resp, err := deps.NewMsg(requestID, respDoc)
	return resp, true, err
}
