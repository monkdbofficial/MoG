package opmsg

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.uber.org/zap"
	"gopkg.in/mgo.v2/bson"

	"mog/internal/logging"
	mongopath "mog/internal/mongo"
	"mog/internal/mongo/handler/relational"
	"mog/internal/mongo/handler/shared"
	mpipeline "mog/internal/mongo/pipeline"
)

// CmdDelete is a helper used by the adapter.
func CmdDelete(deps Deps, ctx context.Context, requestID int32, cmd bson.M) ([]byte, bool, error) {
	col, ok := cmd["delete"].(string)
	if !ok {
		return nil, false, nil
	}
	start := time.Now()

	dbName := deps.CommandDB(cmd)
	physical, err := deps.PhysicalCollectionName(dbName, col)
	if err != nil {
		mongopath.LogQuery(mongopath.QueryLogOptions{
			Stage:         mongopath.RequestStageComplete,
			Method:        "delete",
			Operation:     "delete",
			QueryName:     "delete",
			Table:         "",
			Error:         err,
			TotalDuration: time.Since(start),
			StartedAt:     start,
		})
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
		mongopath.LogQuery(mongopath.QueryLogOptions{
			Stage:         mongopath.RequestStageComplete,
			Method:        "delete",
			Operation:     "delete",
			QueryName:     "delete",
			Table:         physical,
			Error:         fmt.Errorf("deletes must be an array"),
			TotalDuration: time.Since(start),
			StartedAt:     start,
		})
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
			mongopath.LogQuery(mongopath.QueryLogOptions{
				Stage:         mongopath.RequestStageComplete,
				Method:        "delete",
				Operation:     "delete",
				QueryName:     "delete",
				Table:         physical,
				Error:         err,
				TotalDuration: time.Since(start),
				StartedAt:     start,
			})
			return nil, true, err
		}
		residualFilter := deleteResidualFilter(filter)
		var delIDs []string
		for _, pd := range pdocs {
			if len(residualFilter) == 0 || mpipeline.MatchDoc(pd.Doc, residualFilter) {
				delIDs = append(delIDs, pd.DocID)
				if limit == 1 {
					break
				}
			}
		}
		for _, id := range delIDs {
			tag, err := deps.DB().Exec(ctx, "DELETE FROM doc."+physical+" WHERE id = $1", id)
			if err != nil {
				mongopath.LogQuery(mongopath.QueryLogOptions{
					Stage:         mongopath.RequestStageComplete,
					Method:        "delete",
					Operation:     "delete",
					QueryName:     "delete",
					Table:         physical,
					Error:         err,
					TotalDuration: time.Since(start),
					StartedAt:     start,
				})
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
	mongopath.LogQuery(mongopath.QueryLogOptions{
		Stage:         mongopath.RequestStageComplete,
		Method:        "delete",
		Operation:     "delete",
		QueryName:     "delete",
		Table:         physical,
		RowsAffected:  affected,
		Error:         err,
		TotalDuration: time.Since(start),
		StartedAt:     start,
	})
	return resp, true, err
}

// loadDeleteCandidateDocs is a helper used by the adapter.
func loadDeleteCandidateDocs(ctx context.Context, deps Deps, physical string, filter bson.M, single bool) ([]SQLDoc, error) {
	pushdown, err := relational.BuildFilterPushdown(filter)
	if err != nil {
		return nil, err
	}
	if len(pushdown.PushedFilter) == 0 || pushdown.Where == nil || pushdown.Where.SQL == "" {
		return deps.LoadSQLDocsWithIDs(ctx, physical)
	}

	query := "SELECT " + deleteCandidateSelectList(ctx, deps, physical, pushdown.ResidualFilter) + " FROM doc." + physical + " WHERE " + pushdown.Where.SQL
	if single && len(pushdown.ResidualFilter) == 0 {
		query += " LIMIT 1"
	}

	pdocs, err := deps.LoadSQLDocsWithIDsQry(ctx, query, pushdown.Where.Args...)
	if err == nil {
		return pdocs, nil
	}
	return deps.LoadSQLDocsWithIDs(ctx, physical)
}

// deleteCandidateSelectList is a helper used by the adapter.
func deleteCandidateSelectList(ctx context.Context, deps Deps, physical string, residual bson.M) string {
	needed := map[string]struct{}{"id": {}}
	for _, field := range deleteFilterFieldRoots(residual) {
		col := shared.SQLColumnNameForField(field)
		if col != "" {
			needed[col] = struct{}{}
		}
	}
	_ = ctx
	_ = deps
	_ = physical
	cols := make([]string, 0, len(needed))
	for col := range needed {
		cols = append(cols, col)
	}
	slices.Sort(cols)
	out := orderNeededColumns(cols)
	if len(out) == 0 {
		return "id"
	}
	return strings.Join(out, ", ")
}

// deleteFilterFieldRoots is a helper used by the adapter.
func deleteFilterFieldRoots(filter bson.M) []string {
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
	slices.Sort(out)
	return out
}

// deleteResidualFilter is a helper used by the adapter.
func deleteResidualFilter(filter bson.M) bson.M {
	pushdown, err := relational.BuildFilterPushdown(filter)
	if err != nil {
		return filter
	}
	return pushdown.ResidualFilter
}

// orderNeededColumns is a helper used by the adapter.
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
