package opmsg

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	mongopath "mog/internal/mongo"
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
	pipelineStages := mongopath.StageNames(pipelineDocs)
	matchFields := mongopath.MatchFieldNames(pipelineDocs)
	sortFields := mongopath.SortFields(pipelineDocs)
	limit, skip := mongopath.Pagination(pipelineDocs)
	aggIntent := mongopath.AggregationIntent(pipelineDocs)
	if mongopath.AdapterDebugEnabled() {
		mongopath.AdapterDebug(mongopath.RequestStageEntry,
			zap.String("method", "aggregate"),
			zap.String("query_name", "aggregate"),
			zap.String("table", physical),
			zap.Strings("pipeline_stages", pipelineStages),
			zap.Strings("match_fields", matchFields),
			zap.Strings("non_pushed_filters", matchFields),
			zap.Strings("sort_fields", sortFields),
			zap.Int("limit", limit),
			zap.Int("skip", skip),
			zap.String("aggregation_intent", aggIntent),
			zap.String("pushdown_reason", "in-memory"),
		)
	}

	var (
		baseDocs   []bson.M
		vectorDocs []bson.M
	)

	pushdownStart := time.Now()
	vectorDocs, err = vectorSearchResultDocs(ctx, deps, physical, pipelineDocs)
	pushdownDuration := time.Since(pushdownStart)
	if err != nil {
		return nil, true, err
	}
	if vectorDocs != nil {
		pipelineDocs = pipelineDocs[1:]
		baseDocs = vectorDocs
	} else {
		baseDocs, err = deps.LoadSQLDocs(ctx, physical)
		if err != nil {
			return nil, true, err
		}
	}
	pushdownReason := "full_memory"
	if vectorDocs != nil {
		pushdownReason = "vector_search"
	}
	if mongopath.AdapterDebugEnabled() {
		mongopath.AdapterDebug(mongopath.PushdownStageName,
			zap.String("table", physical),
			zap.Bool("vector_search_pushdown", vectorDocs != nil),
			zap.String("pushdown_reason", pushdownReason),
			zap.Float64("pushdown_duration_ms", float64(pushdownDuration.Milliseconds())),
			zap.Strings("pushed_down_filters", matchFields),
			zap.Strings("non_pushed_filters", matchFields),
			zap.Strings("sort_fields", sortFields),
			zap.Int("limit", limit),
			zap.Int("skip", skip),
			zap.String("aggregation_intent", aggIntent),
		)
	}
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
	outDocs, err := mpipeline.ApplyPipelineWithLookup(baseDocs, pipelineDocs, resolveLookup)
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
	if mongopath.AdapterDebugEnabled() {
		mongopath.AdapterDebug(mongopath.RequestStageComplete,
			zap.String("method", "aggregate"),
			zap.String("table", physical),
			zap.String("query_name", "aggregate"),
			zap.Int("result_rows", len(outDocs)),
			zap.Strings("post_processing_steps", postStages),
			zap.Float64("post_processing_duration_ms", float64(postProcessingDuration.Milliseconds())),
			zap.Float64("total_duration_ms", float64(totalDuration.Milliseconds())),
		)
	}
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

func vectorSearchResultDocs(ctx context.Context, deps Deps, physical string, pipeline []bson.M) ([]bson.M, error) {
	if len(pipeline) == 0 {
		return nil, nil
	}
	rawStage := pipeline[0]["$vectorSearch"]
	if rawStage == nil {
		return nil, nil
	}
	spec, ok := shared.CoerceBsonM(rawStage)
	if !ok {
		return nil, fmt.Errorf("$vectorSearch stage must be a document")
	}
	return executeVectorSearch(ctx, deps, physical, spec)
}

const defaultVectorLimit = 10

func executeVectorSearch(ctx context.Context, deps Deps, physical string, spec bson.M) ([]bson.M, error) {
	path, _ := spec["path"].(string)
	if path == "" {
		return nil, fmt.Errorf("$vectorSearch requires non-empty path")
	}
	rawQuery, ok := spec["queryVector"]
	if !ok || rawQuery == nil {
		return nil, fmt.Errorf("$vectorSearch requires queryVector")
	}
	arr, ok := shared.CoerceInterfaceSlice(rawQuery)
	if !ok || len(arr) == 0 {
		return nil, fmt.Errorf("$vectorSearch queryVector must be a non-empty array")
	}
	literal, dims, ok := handlerSql.FloatVectorLiteral(arr)
	if !ok {
		return nil, fmt.Errorf("$vectorSearch queryVector must be numeric")
	}
	limit, err := parseVectorLimit(spec["limit"])
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = defaultVectorLimit
	}
	col := shared.SQLColumnNameForField(path)
	if col == "" {
		return nil, fmt.Errorf("field %q is not supported as a vector column", path)
	}
	similarity := ""
	if rawSim, ok := spec["similarity"]; ok && rawSim != nil {
		if sim, ok := mongopath.NormalizeVectorSimilarity(fmt.Sprint(rawSim)); ok {
			similarity = sim
		} else {
			return nil, fmt.Errorf("invalid similarity override: %v", rawSim)
		}
	}
	query := buildVectorSearchSQL(physical, col, literal, dims, limit, similarity)
	pdocs, err := deps.LoadSQLDocsWithIDsQry(ctx, query)
	if err != nil {
		return nil, err
	}
	out := make([]bson.M, 0, len(pdocs))
	for _, pd := range pdocs {
		out = append(out, pd.Doc)
	}
	return out, nil
}

func buildVectorSearchSQL(physical, column, vectorLiteral string, dims, limit int, similarity string) string {
	vectorExpr := fmt.Sprintf("%s::float_vector(%d)", vectorLiteral, dims)
	simParam := ""
	if similarity != "" {
		simParam = fmt.Sprintf(", '%s'", similarity)
	}
	embedRef := fmt.Sprintf("d.%s", column)
	scoreExpr := fmt.Sprintf("VECTOR_SIMILARITY(%s, (SELECT v FROM q)%s)", embedRef, simParam)
	knnExpr := fmt.Sprintf("KNN_MATCH(%s, (SELECT v FROM q), %d%s)", embedRef, limit, simParam)
	return fmt.Sprintf(`WITH q AS (SELECT %s AS v)
SELECT d.*, %s AS "__mog_vectorSearchScore", %s AS "_score"
FROM doc.%s d
WHERE %s
ORDER BY "__mog_vectorSearchScore" DESC
LIMIT %d`, vectorExpr, scoreExpr, scoreExpr, physical, knnExpr, limit)
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
