package opmsg

import (
	"context"
	"fmt"

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

	baseDocs, err := deps.LoadSQLDocs(ctx, physical)
	if err != nil {
		return nil, true, err
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

	outDocs, err := mpipeline.ApplyPipelineWithLookup(baseDocs, pipelineDocs, resolveLookup)
	if err != nil {
		return nil, true, err
	}
	for _, d := range outDocs {
		deps.NormalizeDocForReply(d)
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
