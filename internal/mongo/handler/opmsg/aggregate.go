package opmsg

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.uber.org/zap"

	mongopath "mog/internal/mongo"
	"mog/internal/mongo/handler/relational"
	handlerSql "mog/internal/mongo/handler/sql"

	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/handler/shared"
	mpipeline "mog/internal/mongo/pipeline"
)

func CmdAggregate(deps Deps, ctx context.Context, requestID int32, cmd bson.M) ([]byte, bool, error) {
	col, ok := cmd["aggregate"].(string)
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

	pipeline, ok := cmd["pipeline"].([]interface{})
	if !ok {
		return nil, true, fmt.Errorf("pipeline must be an array")
	}

	var pipelineDocs []bson.M
	for _, stage := range pipeline {
		if stageDoc, ok := shared.CoerceBsonM(stage); ok {
			pipelineDocs = append(pipelineDocs, stageDoc)
		}
	}
	start := time.Now()
	vectorTimeout := aggregateMaxTime(cmd)
	matchFields := mongopath.MatchFieldNames(pipelineDocs)
	sortFields := mongopath.SortFields(pipelineDocs)
	limit, skip := mongopath.Pagination(pipelineDocs)
	mongopath.LogQuery(mongopath.QueryLogOptions{
		Stage:             mongopath.RequestStageEntry,
		Method:            "aggregate",
		Operation:         "aggregate",
		QueryName:         "aggregate",
		Table:             physical,
		QueryTemplate:     "",
		PushedDownFilters: matchFields,
		NonPushedFilters:  matchFields,
		SortFields:        sortFields,
		Limit:             limit,
		Skip:              skip,
		PushdownReason:    "in-memory",
		StartedAt:         start,
	})

	var (
		baseDocs   []bson.M
		vectorPlan *vectorSearchPlan
	)

	pushdownStart := time.Now()
	pushdownQuery := ""
	pushdownArgsCount := 0
	pushedFilters := matchFields
	nonPushedFilters := matchFields
	vectorPlan, err = buildAndExecuteVectorSearchPlan(ctx, deps, physical, pipelineDocs, vectorTimeout)
	pushdownDuration := time.Since(pushdownStart)
	if err != nil {
		return nil, true, err
	}
	if vectorPlan != nil {
		pipelineDocs = vectorPlan.RemainingPipeline
		baseDocs = vectorPlan.Docs
		pushdownQuery = vectorPlan.Query
		pushdownArgsCount = vectorPlan.ArgsCount
		pushedFilters = vectorPlan.PushedDownFilters
		nonPushedFilters = vectorPlan.NonPushedFilters
	} else {
		if plan, ok, err := buildAggregatePrefilterPlan(physical, pipelineDocs); err != nil {
			return nil, true, err
		} else if ok {
			pushdownQuery = plan.Query
			pushdownArgsCount = len(plan.Args)
			pushedFilters = plan.PushedDownFilters
			nonPushedFilters = plan.NonPushedFilters
			pdocs, err := deps.LoadSQLDocsWithIDsQry(ctx, plan.Query, plan.Args...)
			if err == nil {
				baseDocs = make([]bson.M, 0, len(pdocs))
				for _, pd := range pdocs {
					baseDocs = append(baseDocs, pd.Doc)
				}
				pipelineDocs = plan.RemainingPipeline
			} else {
				baseDocs, err = deps.LoadSQLDocs(ctx, physical)
				if err != nil {
					return nil, true, err
				}
			}
		} else {
			baseDocs, err = deps.LoadSQLDocs(ctx, physical)
			if err != nil {
				return nil, true, err
			}
		}
	}
	pushdownReason := "full_memory"
	if vectorPlan != nil {
		pushdownReason = "vector_search"
	} else if pushdownQuery != "" {
		pushdownReason = "partial_sql_prefilter"
	}
	mongopath.LogQuery(mongopath.QueryLogOptions{
		Stage:             mongopath.PushdownStageName,
		Method:            "aggregate",
		Operation:         "aggregate",
		QueryName:         "aggregate",
		Table:             physical,
		QueryTemplate:     pushdownQuery,
		ArgsCount:         pushdownArgsCount,
		PushedDownFilters: pushedFilters,
		NonPushedFilters:  nonPushedFilters,
		SortFields:        sortFields,
		Limit:             limit,
		Skip:              skip,
		PushdownReason:    pushdownReason,
		PushdownDuration:  pushdownDuration,
		StartedAt:         pushdownStart,
	})
	lookupCache := map[string][]bson.M{}
	resolveLookup := func(from string) ([]bson.M, error) {
		if cached, ok := lookupCache[from]; ok {
			return cached, nil
		}
		fromPhysical, err := deps.PhysicalCollectionName(dbName, from)
		if err != nil {
			lookupCache[from] = []bson.M{}
			return lookupCache[from], nil
		}
		_ = deps.CatalogUpsert(ctx, dbName, from)
		docs, err := deps.LoadSQLDocs(ctx, fromPhysical)
		if err != nil {
			return nil, err
		}
		lookupCache[from] = docs
		return docs, nil
	}

	postStages := mongopath.StageNames(pipelineDocs)
	postProcessingStart := time.Now()
	outDocs, err := executeAggregateStages(ctx, deps, dbName, baseDocs, pipelineDocs, resolveLookup, vectorTimeout)
	postProcessingDuration := time.Since(postProcessingStart)
	if err != nil {
		mongopath.AdapterError("request.failure", err,
			zap.String("method", "aggregate"),
			zap.String("table", physical),
			zap.Strings("post_processing_steps", postStages),
			zap.Float64("post_processing_duration_ms", float64(postProcessingDuration.Milliseconds())),
			zap.Float64("total_duration_ms", float64(time.Since(start).Milliseconds())),
		)
		return nil, true, err
	}
	for _, d := range outDocs {
		if err := deps.NormalizeDocForReply(ctx, d); err != nil {
			return nil, true, err
		}
	}
	totalDuration := time.Since(start)
	mongopath.LogQuery(mongopath.QueryLogOptions{
		Stage:                  mongopath.RequestStageComplete,
		Method:                 "aggregate",
		Operation:              "aggregate",
		QueryName:              "aggregate",
		Table:                  physical,
		QueryTemplate:          pushdownQuery,
		ArgsCount:              pushdownArgsCount,
		PushedDownFilters:      pushedFilters,
		NonPushedFilters:       nonPushedFilters,
		SortFields:             sortFields,
		Rows:                   len(outDocs),
		PostProcessingDuration: postProcessingDuration,
		TotalDuration:          totalDuration,
		StartedAt:              start,
	})
	var firstBatch interface{} = outDocs
	if deps.StableFieldOrder {
		ordered := make([]interface{}, 0, len(outDocs))
		for _, d := range outDocs {
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

func executeAggregateStages(ctx context.Context, deps Deps, dbName string, docs []bson.M, pipeline []bson.M, resolveLookup func(from string) ([]bson.M, error), timeout time.Duration) ([]bson.M, error) {
	current := docs
	for _, stage := range pipeline {
		if spec, ok := shared.CoerceBsonM(stage["$graphLookup"]); ok {
			nextDocs, pushed, err := applyGraphLookupPushdown(ctx, deps, dbName, current, spec, timeout)
			if err != nil {
				return nil, err
			}
			if pushed {
				current = nextDocs
				continue
			}
		}
		nextDocs, err := mpipeline.ApplyPipelineWithLookup(current, []bson.M{stage}, resolveLookup)
		if err != nil {
			return nil, err
		}
		current = nextDocs
	}
	return current, nil
}

const defaultVectorLimit = 10
const vectorSearchScoreField = "__mog_vectorSearchScore"

type vectorSearchPlan struct {
	Docs              []bson.M
	Query             string
	Args              []any
	ArgsCount         int
	RemainingPipeline []bson.M
	PushedDownFilters []string
	NonPushedFilters  []string
}

type vectorProjectionPlan struct {
	SelectColumns        []string
	ConsumedProjectStage bool
}

const vectorScoreAliasPrefix = "__vector_score_alias__:"

func buildAndExecuteVectorSearchPlan(ctx context.Context, deps Deps, physical string, pipeline []bson.M, timeout time.Duration) (*vectorSearchPlan, error) {
	stageIndex := -1
	var spec bson.M
	for idx, stage := range pipeline {
		rawStage := stage["$vectorSearch"]
		if rawStage == nil {
			continue
		}
		stageSpec, ok := shared.CoerceBsonM(rawStage)
		if !ok {
			return nil, fmt.Errorf("$vectorSearch stage must be a document")
		}
		stageIndex = idx
		spec = stageSpec
		break
	}
	if stageIndex < 0 {
		return nil, nil
	}

	leadingMatch := bson.M{}
	for idx := 0; idx < stageIndex; idx++ {
		stageMatch, ok := shared.CoerceBsonM(pipeline[idx]["$match"])
		if !ok {
			return nil, fmt.Errorf("$vectorSearch must be the first stage or only be preceded by $match stages")
		}
		for k, v := range stageMatch {
			leadingMatch[k] = v
		}
	}

	projection := buildVectorProjectionPlan(pipeline[stageIndex+1:])
	docs, query, args, split, err := executeVectorSearch(ctx, deps, physical, spec, leadingMatch, projection, timeout)
	if err != nil {
		return nil, err
	}

	remaining := make([]bson.M, 0, len(pipeline))
	if len(split.ResidualFilter) > 0 {
		remaining = append(remaining, bson.M{"$match": split.ResidualFilter})
	}
	nextStages := pipeline[stageIndex+1:]
	if projection.ConsumedProjectStage && len(nextStages) > 0 {
		nextStages = nextStages[1:]
	}
	remaining = append(remaining, nextStages...)

	return &vectorSearchPlan{
		Docs:              docs,
		Query:             query,
		Args:              args,
		ArgsCount:         len(args),
		RemainingPipeline: remaining,
		PushedDownFilters: sortedFilterKeys(split.PushedFilter),
		NonPushedFilters:  sortedFilterKeys(split.ResidualFilter),
	}, nil
}

func executeVectorSearch(ctx context.Context, deps Deps, physical string, spec bson.M, prefilter bson.M, projection vectorProjectionPlan, timeout time.Duration) ([]bson.M, string, []any, relational.FilterPushdown, error) {
	path, _ := spec["path"].(string)
	if path == "" {
		return nil, "", nil, relational.FilterPushdown{}, fmt.Errorf("$vectorSearch requires non-empty path")
	}
	rawQuery, ok := spec["queryVector"]
	if !ok || rawQuery == nil {
		return nil, "", nil, relational.FilterPushdown{}, fmt.Errorf("$vectorSearch requires queryVector")
	}
	arr, ok := shared.CoerceInterfaceSlice(rawQuery)
	if !ok || len(arr) == 0 {
		return nil, "", nil, relational.FilterPushdown{}, fmt.Errorf("$vectorSearch queryVector must be a non-empty array")
	}
	literal, dims, ok := handlerSql.FloatVectorLiteral(arr)
	if !ok {
		return nil, "", nil, relational.FilterPushdown{}, fmt.Errorf("$vectorSearch queryVector must be numeric")
	}
	limit, err := parseVectorLimit(spec["limit"])
	if err != nil {
		return nil, "", nil, relational.FilterPushdown{}, err
	}
	if limit <= 0 {
		limit = defaultVectorLimit
	}
	col := shared.SQLColumnNameForField(path)
	if col == "" {
		return nil, "", nil, relational.FilterPushdown{}, fmt.Errorf("field %q is not supported as a vector column", path)
	}
	similarity := ""
	if rawSim, ok := spec["similarity"]; ok && rawSim != nil {
		if sim, ok := mongopath.NormalizeVectorSimilarity(fmt.Sprint(rawSim)); ok {
			similarity = sim
		} else {
			return nil, "", nil, relational.FilterPushdown{}, fmt.Errorf("invalid similarity override: %v", rawSim)
		}
	}
	split, err := relational.BuildFilterPushdown(prefilter)
	if err != nil {
		return nil, "", nil, relational.FilterPushdown{}, err
	}
	query := buildVectorSearchSQL(physical, projection.SelectColumns, col, literal, dims, limit, similarity, split.Where)
	args := cloneWhereArgs(split.Where)
	queryCtx, cancel := withOptionalTimeout(ctx, timeout)
	defer cancel()
	if err := refreshVectorSearchTable(queryCtx, deps, physical); err != nil {
		return nil, "", nil, relational.FilterPushdown{}, err
	}
	mongopath.AdapterDebug("vector_search.sql",
		zap.String("table", physical),
		zap.String("sql", query),
		zap.Int("args_count", len(args)),
		zap.Float64("timeout_ms", float64(timeout.Milliseconds())),
	)
	pdocs, err := deps.LoadSQLDocsWithIDsQry(queryCtx, query, args...)
	if err != nil {
		return nil, "", nil, relational.FilterPushdown{}, err
	}
	docs := make([]bson.M, 0, len(pdocs))
	for _, pd := range pdocs {
		docs = append(docs, pd.Doc)
	}
	return docs, query, args, split, nil
}

func buildVectorSearchSQL(physical string, selectColumns []string, column, vectorLiteral string, dims, limit int, similarity string, prefilter *relational.Where) string {
	vectorExpr := fmt.Sprintf("%s::float_vector(%d)", vectorLiteral, dims)
	simParam := ""
	if similarity != "" {
		simParam = fmt.Sprintf(", '%s'", similarity)
	}
	scoreExpr := fmt.Sprintf("VECTOR_SIMILARITY(%s, %s%s)", column, vectorExpr, simParam)
	knnExpr := fmt.Sprintf("KNN_MATCH(%s, %s, %d%s)", column, vectorExpr, limit, simParam)
	whereParts := []string{knnExpr}
	if prefilter != nil && prefilter.SQL != "" {
		whereParts = append(whereParts, prefilter.SQL)
	}
	selectList := "id, _score"
	orderExpr := "_score"
	if len(selectColumns) > 0 {
		rendered := make([]string, 0, len(selectColumns))
		for _, col := range selectColumns {
			if strings.HasPrefix(col, vectorScoreAliasPrefix) {
				alias := strings.TrimPrefix(col, vectorScoreAliasPrefix)
				rendered = append(rendered, fmt.Sprintf("%s AS %s", scoreExpr, quoteIdentifier(alias)))
				orderExpr = quoteIdentifier(alias)
				continue
			}
			rendered = append(rendered, col)
		}
		selectList = strings.Join(rendered, ", ")
	}
	return fmt.Sprintf(`SELECT %s
FROM doc.%s
WHERE %s
ORDER BY %s DESC
LIMIT %d`, selectList, physical, strings.Join(whereParts, " AND "), orderExpr, limit)
}

func aggregateMaxTime(cmd bson.M) time.Duration {
	raw, ok := cmd["maxTimeMS"]
	if !ok || raw == nil {
		return 10 * time.Second
	}
	n, err := mpipeline.AsInt(raw)
	if err != nil || n <= 0 {
		return 10 * time.Second
	}
	return time.Duration(n) * time.Millisecond
}

func withOptionalTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

func refreshVectorSearchTable(ctx context.Context, deps Deps, physical string) error {
	if deps.DB == nil || physical == "" {
		return nil
	}
	db := deps.DB()
	if db == nil {
		return nil
	}
	_, err := db.Exec(ctx, "REFRESH TABLE doc."+physical)
	return err
}

func buildVectorProjectionPlan(stages []bson.M) vectorProjectionPlan {
	plan := vectorProjectionPlan{
		SelectColumns: []string{"id", vectorScoreAliasPrefix + vectorSearchScoreField},
	}
	if len(stages) == 0 {
		plan.SelectColumns = append(plan.SelectColumns, "data")
		return plan
	}
	project, ok := shared.CoerceBsonM(stages[0]["$project"])
	if !ok {
		plan.SelectColumns = append(plan.SelectColumns, "data")
		return plan
	}

	cols := []string{"id"}
	for field, raw := range project {
		if field == "" || field == "_id" {
			continue
		}
		if include, ok := raw.(int); ok && include == 1 {
			col := shared.SQLColumnNameForField(field)
			if col == "" || col == "data" {
				return vectorProjectionPlan{SelectColumns: []string{"id", "data", fmt.Sprintf("_score AS %q", vectorSearchScoreField)}}
			}
			cols = append(cols, col)
			continue
		}
		if include, ok := raw.(int32); ok && include == 1 {
			col := shared.SQLColumnNameForField(field)
			if col == "" || col == "data" {
				return vectorProjectionPlan{SelectColumns: []string{"id", "data", fmt.Sprintf("_score AS %q", vectorSearchScoreField)}}
			}
			cols = append(cols, col)
			continue
		}
		if spec, ok := shared.CoerceBsonM(raw); ok {
			if meta, ok := spec["$meta"].(string); ok && meta == "vectorSearchScore" {
				cols = append(cols, vectorScoreAliasPrefix+field)
				continue
			}
		}
		return vectorProjectionPlan{SelectColumns: []string{"id", "data", fmt.Sprintf("_score AS %q", vectorSearchScoreField)}}
	}
	cols = append(cols, vectorScoreAliasPrefix+vectorSearchScoreField)
	return vectorProjectionPlan{
		SelectColumns:        cols,
		ConsumedProjectStage: true,
	}
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func applyGraphLookupPushdown(ctx context.Context, deps Deps, dbName string, baseDocs []bson.M, spec bson.M, timeout time.Duration) ([]bson.M, bool, error) {
	from, _ := spec["from"].(string)
	as, _ := spec["as"].(string)
	connectFromField, _ := spec["connectFromField"].(string)
	connectToField, _ := spec["connectToField"].(string)
	startWith := spec["startWith"]
	if from == "" || as == "" || connectFromField == "" || connectToField == "" || startWith == nil {
		return nil, false, nil
	}
	fromPhysical, err := deps.PhysicalCollectionName(dbName, from)
	if err != nil {
		return nil, false, err
	}
	graphCfg, ok := buildGraphLookupConfig(fromPhysical, connectFromField, connectToField)
	if !ok {
		return nil, false, nil
	}
	maxDepth := -1
	if rawDepth, ok := spec["maxDepth"]; ok && rawDepth != nil {
		depth, err := mpipeline.AsInt(rawDepth)
		if err != nil {
			return nil, false, err
		}
		maxDepth = depth
	}
	depthField, _ := spec["depthField"].(string)

	restrictWhere := (*relational.Where)(nil)
	if rawRestrict, ok := spec["restrictSearchWithMatch"]; ok && rawRestrict != nil {
		restrictDoc, ok := shared.CoerceBsonM(rawRestrict)
		if !ok {
			return nil, false, nil
		}
		where, ok, err := relational.BuildWhere(restrictDoc)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		restrictWhere = where
	}

	fromExpr, ok := graphLookupSQLExpr(connectFromField)
	if !ok {
		return nil, false, nil
	}
	toExpr, ok := graphLookupSQLExpr(connectToField)
	if !ok {
		return nil, false, nil
	}

	queryCtx, cancel := withOptionalTimeout(ctx, timeout)
	defer cancel()
	if err := ensureGraphLookupResources(queryCtx, deps, fromPhysical, graphCfg, fromExpr, toExpr); err != nil {
		return nil, false, err
	}

	out := make([]bson.M, 0, len(baseDocs))
	for _, doc := range baseDocs {
		starts, ok, err := graphLookupStartValues(doc, startWith, connectToField)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		hits, err := queryGraphLookupHits(queryCtx, deps, fromPhysical, graphCfg, starts, maxDepth)
		if err != nil {
			return nil, false, err
		}
		joined, err := loadGraphLookupDocs(queryCtx, deps, fromPhysical, graphCfg, toExpr, hits, depthField, restrictWhere)
		if err != nil {
			return nil, false, err
		}
		clone := bson.M{}
		for k, v := range doc {
			clone[k] = v
		}
		clone[as] = joined
		out = append(out, clone)
	}
	return out, true, nil
}

type graphLookupHit struct {
	Vertex string
	Depth  int64
}

func queryGraphLookupHits(ctx context.Context, deps Deps, physical string, cfg graphLookupConfig, starts []any, maxDepth int) ([]graphLookupHit, error) {
	if len(starts) == 0 {
		return []graphLookupHit{}, nil
	}
	hits := make([]graphLookupHit, 0, len(starts)*4)
	seen := map[string]int64{}
	for _, start := range starts {
		query := buildGraphTraverseSQL(cfg.GraphName, start, maxDepth)
		mongopath.AdapterDebug("graph_lookup.sql",
			zap.String("table", physical),
			zap.String("sql", query),
			zap.Int("args_count", 0),
		)
		rows, err := deps.DB().Query(ctx, query)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				return nil, err
			}
			if len(values) < 2 {
				rows.Close()
				return nil, fmt.Errorf("graph traverse returned %d columns, expected at least 2", len(values))
			}
			vertex := fmt.Sprint(values[0])
			depthInt, err := mpipeline.AsInt(values[1])
			if err != nil {
				rows.Close()
				return nil, err
			}
			depth := int64(depthInt)
			if existing, ok := seen[vertex]; ok && existing <= depth {
				continue
			}
			seen[vertex] = depth
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	for vertex, depth := range seen {
		hits = append(hits, graphLookupHit{Vertex: vertex, Depth: depth})
	}
	slices.SortFunc(hits, func(a, b graphLookupHit) int {
		if a.Depth < b.Depth {
			return -1
		}
		if a.Depth > b.Depth {
			return 1
		}
		return strings.Compare(a.Vertex, b.Vertex)
	})
	return hits, nil
}

func loadGraphLookupDocs(ctx context.Context, deps Deps, physical string, cfg graphLookupConfig, toExpr string, hits []graphLookupHit, depthField string, restrictWhere *relational.Where) ([]bson.M, error) {
	if len(hits) == 0 {
		return []bson.M{}, nil
	}
	query, extraArgs := buildLoadDocsByGraphKeySQL(physical, toExpr, len(hits), restrictWhere)
	args := make([]any, 0, len(hits))
	for _, hit := range hits {
		args = append(args, hit.Vertex)
	}
	args = append(args, extraArgs...)
	pdocs, err := deps.LoadSQLDocsWithIDsQry(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	docByID := make(map[string]bson.M, len(pdocs))
	for _, pd := range pdocs {
		key := graphLookupDocKey(pd.Doc, cfg.ConnectToField)
		docByID[key] = pd.Doc
	}
	out := make([]bson.M, 0, len(hits))
	for _, hit := range hits {
		doc, ok := docByID[hit.Vertex]
		if !ok {
			continue
		}
		clone := bson.M{}
		for k, v := range doc {
			clone[k] = v
		}
		if depthField != "" {
			clone[depthField] = hit.Depth
		}
		out = append(out, clone)
	}
	return out, nil
}

func graphLookupStartValues(doc bson.M, expr any, connectToField string) ([]any, bool, error) {
	values := []any{}
	appendValue := func(v any) error {
		if v == nil {
			return nil
		}
		if connectToField == "_id" {
			encoded, err := encodeGraphLookupID(v)
			if err != nil {
				return err
			}
			values = append(values, encoded)
			return nil
		}
		values = append(values, fmt.Sprint(v))
		return nil
	}

	switch raw := expr.(type) {
	case string:
		if strings.HasPrefix(raw, "$") {
			v := mpipeline.GetPathValue(doc, strings.TrimPrefix(raw, "$"))
			if arr, ok := shared.CoerceInterfaceSlice(v); ok {
				for _, item := range arr {
					if err := appendValue(item); err != nil {
						return nil, false, err
					}
				}
			} else if err := appendValue(v); err != nil {
				return nil, false, err
			}
			return values, true, nil
		}
		if err := appendValue(raw); err != nil {
			return nil, false, err
		}
		return values, true, nil
	default:
		if arr, ok := shared.CoerceInterfaceSlice(raw); ok {
			for _, item := range arr {
				subValues, ok, err := graphLookupStartValues(doc, item, connectToField)
				if err != nil || !ok {
					return nil, ok, err
				}
				values = append(values, subValues...)
			}
			return values, true, nil
		}
		if err := appendValue(raw); err != nil {
			return nil, false, err
		}
		return values, true, nil
	}
}

func encodeGraphLookupID(v any) (string, error) {
	if v == nil {
		return "", fmt.Errorf("_id is nil")
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func graphLookupSQLExpr(path string) (string, bool) {
	if path == "" {
		return "", false
	}
	parts := strings.Split(path, ".")
	for _, part := range parts {
		if part == "" || !shared.IsSafeIdentifier(part) {
			return "", false
		}
	}
	if len(parts) == 1 && parts[0] == "_id" {
		return "id", true
	}
	col := shared.SQLColumnNameForField(parts[0])
	if col == "" {
		return "", false
	}
	expr := col
	for _, part := range parts[1:] {
		expr += fmt.Sprintf("['%s']", part)
	}
	return expr, true
}

type graphLookupConfig struct {
	GraphName      string
	EdgeTable      string
	VertexTable    string
	VertexKeyCol   string
	ConnectToField string
}

func buildGraphLookupConfig(physical, connectFromField, connectToField string) (graphLookupConfig, bool) {
	vertexKeyCol, ok := graphVertexKeyColumn(connectToField)
	if !ok {
		return graphLookupConfig{}, false
	}
	suffix := encodeGraphNamePart(connectFromField) + "_" + encodeGraphNamePart(connectToField)
	graphName := physical + "__graph__" + suffix
	edgeTable := physical + "__graph_edges__" + suffix
	vertexTable := physical + "__graph_vertices__" + suffix
	return graphLookupConfig{
		GraphName:      graphName,
		EdgeTable:      edgeTable,
		VertexTable:    vertexTable,
		VertexKeyCol:   vertexKeyCol,
		ConnectToField: connectToField,
	}, true
}

func graphVertexKeyColumn(path string) (string, bool) {
	if path == "_id" {
		return "id", true
	}
	if strings.Contains(path, ".") || path == "" {
		return "", false
	}
	col := shared.SQLColumnNameForField(path)
	if col == "" || col == "data" {
		return "", false
	}
	return col, true
}

func encodeGraphNamePart(part string) string {
	if part == "" {
		return "x"
	}
	return strings.ToLower(shared.FieldB32.EncodeToString([]byte(part)))
}

func ensureGraphLookupResources(ctx context.Context, deps Deps, physical string, cfg graphLookupConfig, fromExpr, toExpr string) error {
	db := deps.DB()
	if db == nil {
		return fmt.Errorf("db executor is nil")
	}
	stmts := []string{
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS doc.%s (vertex_id TEXT NOT NULL PRIMARY KEY)", cfg.VertexTable),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS doc.%s (src_id TEXT NOT NULL, dst_id TEXT NOT NULL, PRIMARY KEY (src_id, dst_id))", cfg.EdgeTable),
		fmt.Sprintf("DELETE FROM doc.%s", cfg.VertexTable),
		fmt.Sprintf("DELETE FROM doc.%s", cfg.EdgeTable),
		fmt.Sprintf("INSERT INTO doc.%s (vertex_id) SELECT DISTINCT CAST(%s AS TEXT) FROM doc.%s WHERE %s IS NOT NULL", cfg.VertexTable, toExpr, physical, toExpr),
		fmt.Sprintf("INSERT INTO doc.%s (src_id, dst_id) SELECT CAST(%s AS TEXT), CAST(%s AS TEXT) FROM doc.%s WHERE %s IS NOT NULL AND %s IS NOT NULL", cfg.EdgeTable, toExpr, fromExpr, physical, toExpr, fromExpr),
		fmt.Sprintf("REFRESH TABLE doc.%s, doc.%s, doc.%s", physical, cfg.VertexTable, cfg.EdgeTable),
		fmt.Sprintf("CREATE GRAPH %s", cfg.GraphName),
		fmt.Sprintf("CREATE VERTEX TABLE doc.%s FOR GRAPH %s KEY vertex_id", cfg.VertexTable, cfg.GraphName),
		fmt.Sprintf("CREATE EDGE TABLE doc.%s FOR GRAPH %s SOURCE KEY src_id TARGET KEY dst_id", cfg.EdgeTable, cfg.GraphName),
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(ctx, stmt); err != nil && !isIgnorableGraphDDL(err) {
			return err
		}
	}
	return nil
}

func isIgnorableGraphDDL(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") ||
		strings.Contains(msg, "duplicate") ||
		strings.Contains(msg, "exists")
}

func buildGraphTraverseSQL(graphName string, start any, maxDepth int) string {
	depth := maxDepth
	if depth < 0 {
		depth = 64
	}
	return fmt.Sprintf("SELECT * FROM traverse('%s', %s, %d) ORDER BY 2, 1", graphName, graphSQLLiteral(start), depth)
}

func graphSQLLiteral(v any) string {
	if v == nil {
		return "NULL"
	}
	s := fmt.Sprint(v)
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func buildLoadDocsByIDSQL(physical string, n int) string {
	placeholders := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
	}
	return fmt.Sprintf("SELECT * FROM doc.%s WHERE id IN (%s)", physical, strings.Join(placeholders, ", "))
}

func buildLoadDocsByGraphKeySQL(physical, toExpr string, n int, restrictWhere *relational.Where) (string, []any) {
	placeholders := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
	}
	query := fmt.Sprintf("SELECT * FROM doc.%s WHERE CAST(%s AS TEXT) IN (%s)", physical, toExpr, strings.Join(placeholders, ", "))
	if restrictWhere != nil && strings.TrimSpace(restrictWhere.SQL) != "" {
		query += " AND " + rebindWhereSQL(restrictWhere.SQL, n)
		return query, restrictWhere.Args
	}
	return query, nil
}

func graphLookupDocKey(doc bson.M, connectToField string) string {
	if connectToField == "_id" {
		encoded, err := encodeGraphLookupID(doc["_id"])
		if err == nil {
			return encoded
		}
	}
	return fmt.Sprint(mpipeline.GetPathValue(doc, connectToField))
}

func rebindWhereSQL(sql string, offset int) string {
	if offset <= 0 {
		return sql
	}
	var out strings.Builder
	for i := 0; i < len(sql); i++ {
		if sql[i] != '$' {
			out.WriteByte(sql[i])
			continue
		}
		j := i + 1
		for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
			j++
		}
		if j == i+1 {
			out.WriteByte(sql[i])
			continue
		}
		num := 0
		for k := i + 1; k < j; k++ {
			num = num*10 + int(sql[k]-'0')
		}
		out.WriteString(fmt.Sprintf("$%d", num+offset))
		i = j - 1
	}
	return out.String()
}

func parseVectorLimit(raw interface{}) (int, error) {
	if raw == nil {
		return 0, nil
	}
	switch v := raw.(type) {
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case float32:
		return int(v), nil
	case float64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("$vectorSearch limit must be an integer")
	}
}

type aggregatePrefilterPlan struct {
	Query             string
	Args              []any
	RemainingPipeline []bson.M
	PushedDownFilters []string
	NonPushedFilters  []string
}

func buildAggregatePrefilterPlan(physical string, pipeline []bson.M) (aggregatePrefilterPlan, bool, error) {
	if physical == "" || len(pipeline) == 0 {
		return aggregatePrefilterPlan{}, false, nil
	}

	matchCount := 0
	mergedMatch := bson.M{}
	for matchCount < len(pipeline) {
		stageMatch, ok := shared.CoerceBsonM(pipeline[matchCount]["$match"])
		if !ok {
			break
		}
		for k, v := range stageMatch {
			mergedMatch[k] = v
		}
		matchCount++
	}

	query := "SELECT * FROM doc." + physical
	pushedFilters := []string{}
	nonPushedFilters := []string{}
	var split relational.FilterPushdown
	if len(mergedMatch) > 0 {
		var err error
		split, err = relational.BuildFilterPushdown(mergedMatch)
		if err != nil {
			return aggregatePrefilterPlan{}, false, err
		}
		if split.Where != nil && split.Where.SQL != "" {
			query += " WHERE " + split.Where.SQL
			pushedFilters = sortedFilterKeys(split.PushedFilter)
			nonPushedFilters = sortedFilterKeys(split.ResidualFilter)
		} else {
			nonPushedFilters = sortedFilterKeys(mergedMatch)
		}
	}

	cursor := matchCount
	sortIndex := -1
	skipIndex := -1
	limitIndex := -1
	if cursor < len(pipeline) {
		if sortSpec, ok := shared.CoerceBsonM(pipeline[cursor]["$sort"]); ok {
			orderBy, supported, err := relational.BuildOrderBy(sortSpec)
			if err != nil {
				return aggregatePrefilterPlan{}, false, err
			}
			if supported && orderBy != "" {
				query += orderBy
				sortIndex = cursor
				cursor++
			}
		}
	}
	if len(split.ResidualFilter) == 0 {
		if cursor < len(pipeline) {
			if rawSkip, ok := pipeline[cursor]["$skip"]; ok {
				skipValue, err := mpipeline.AsInt(rawSkip)
				if err != nil {
					return aggregatePrefilterPlan{}, false, err
				}
				if skipValue > 0 {
					query += fmt.Sprintf(" OFFSET %d", skipValue)
					skipIndex = cursor
					cursor++
				}
			}
		}
		if cursor < len(pipeline) {
			if rawLimit, ok := pipeline[cursor]["$limit"]; ok {
				limitValue, err := mpipeline.AsInt(rawLimit)
				if err != nil {
					return aggregatePrefilterPlan{}, false, err
				}
				if limitValue > 0 {
					query += fmt.Sprintf(" LIMIT %d", limitValue)
					limitIndex = cursor
				}
			}
		}
	}

	if query == "SELECT * FROM doc."+physical {
		return aggregatePrefilterPlan{}, false, nil
	}

	remaining := make([]bson.M, 0, len(pipeline))
	for idx, stage := range pipeline {
		switch {
		case idx < matchCount && len(split.ResidualFilter) == 0 && len(split.PushedFilter) > 0:
			continue
		case idx == sortIndex || idx == skipIndex || idx == limitIndex:
			continue
		default:
			remaining = append(remaining, stage)
		}
	}

	return aggregatePrefilterPlan{
		Query:             query,
		Args:              cloneWhereArgs(split.Where),
		RemainingPipeline: remaining,
		PushedDownFilters: pushedFilters,
		NonPushedFilters:  nonPushedFilters,
	}, true, nil
}

func sortedFilterKeys(filter bson.M) []string {
	if len(filter) == 0 {
		return nil
	}
	keys := make([]string, 0, len(filter))
	for k := range filter {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func cloneWhereArgs(where *relational.Where) []any {
	if where == nil || len(where.Args) == 0 {
		return nil
	}
	return slices.Clone(where.Args)
}
