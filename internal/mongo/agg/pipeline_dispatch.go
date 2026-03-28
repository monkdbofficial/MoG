package agg

import (
	"fmt"

	"gopkg.in/mgo.v2/bson"
)

// lookupResolver resolves a collection name used by $lookup/$unionWith/$graphLookup
// into a slice of documents. This is used by the in-memory aggregation pipeline
// evaluator to emulate cross-collection stages.
type lookupResolver func(from string) ([]bson.M, error)

// mogVectorSearchScoreKey is an internal key used to temporarily attach vector
// similarity scores to documents while evaluating pipelines.
//
// It is removed from final results before returning to the caller.
const mogVectorSearchScoreKey = "__mog_vectorSearchScore"

// applyPipeline applies an aggregation pipeline to in-memory documents.
//
// This is used for compatibility with MongoDB drivers and to keep behavior
// consistent across SQL backends.
func applyPipeline(docs []bson.M, pipeline []bson.M) ([]bson.M, error) {
	return applyPipelineWithLookup(docs, pipeline, nil)
}

func applyPipelineWithLookup(docs []bson.M, pipeline []bson.M, resolve lookupResolver) ([]bson.M, error) {
	out := docs
	for _, stage := range pipeline {
		switch {
		case stage["$vectorSearch"] != nil:
			spec, ok := coerceBsonM(stage["$vectorSearch"])
			if !ok {
				return nil, fmt.Errorf("$vectorSearch stage must be a document")
			}
			var err error
			out, err = applyVectorSearch(out, spec)
			if err != nil {
				return nil, err
			}
		case stage["$match"] != nil:
			m, ok := stage["$match"].(bson.M)
			if !ok {
				return nil, fmt.Errorf("$match stage must be a document")
			}
			out = applyMatch(out, m)
		case stage["$project"] != nil:
			p, ok := stage["$project"].(bson.M)
			if !ok {
				return nil, fmt.Errorf("$project stage must be a document")
			}
			var err error
			out, err = applyProject(out, p)
			if err != nil {
				return nil, err
			}
		case stage["$addFields"] != nil:
			spec, ok := coerceBsonM(stage["$addFields"])
			if !ok {
				return nil, fmt.Errorf("$addFields stage must be a document")
			}
			var err error
			out, err = applyAddFields(out, spec)
			if err != nil {
				return nil, err
			}
		case stage["$unset"] != nil:
			paths, err := parseUnsetStage(stage["$unset"])
			if err != nil {
				return nil, err
			}
			out = applyUnsetStage(out, paths)
		case stage["$set"] != nil:
			// MongoDB treats $set as an alias for $addFields in aggregation pipelines.
			spec, ok := coerceBsonM(stage["$set"])
			if !ok {
				return nil, fmt.Errorf("$set stage must be a document")
			}
			var err error
			out, err = applyAddFields(out, spec)
			if err != nil {
				return nil, err
			}
		case stage["$lookup"] != nil:
			if resolve == nil {
				return nil, fmt.Errorf("$lookup requires resolver")
			}
			spec, ok := coerceBsonM(stage["$lookup"])
			if !ok {
				return nil, fmt.Errorf("$lookup stage must be a document")
			}
			var err error
			out, err = applyLookup(out, spec, resolve)
			if err != nil {
				return nil, err
			}
		case stage["$graphLookup"] != nil:
			if resolve == nil {
				return nil, fmt.Errorf("$graphLookup requires resolver")
			}
			spec, ok := coerceBsonM(stage["$graphLookup"])
			if !ok {
				return nil, fmt.Errorf("$graphLookup stage must be a document")
			}
			var err error
			out, err = applyGraphLookup(out, spec, resolve)
			if err != nil {
				return nil, err
			}
		case stage["$unwind"] != nil:
			path, preserve, err := parseUnwindStage(stage["$unwind"])
			if err != nil {
				return nil, err
			}
			out, err = applyUnwind(out, path, preserve)
			if err != nil {
				return nil, err
			}
		case stage["$group"] != nil:
			g, ok := stage["$group"].(bson.M)
			if !ok {
				return nil, fmt.Errorf("$group stage must be a document")
			}
			var err error
			out, err = applyGroup(out, g)
			if err != nil {
				return nil, err
			}
		case stage["$count"] != nil:
			field, ok := stage["$count"].(string)
			if !ok || field == "" {
				return nil, fmt.Errorf("$count stage must be a non-empty string")
			}
			out = []bson.M{{field: int64(len(out))}}
		case stage["$sort"] != nil:
			s, ok := stage["$sort"].(bson.M)
			if !ok {
				return nil, fmt.Errorf("$sort stage must be a document")
			}
			out = applySort(out, s)
		case stage["$limit"] != nil:
			lim, err := asInt(stage["$limit"])
			if err != nil {
				return nil, fmt.Errorf("$limit stage must be an integer")
			}
			if lim < 0 {
				lim = 0
			}
			if lim < len(out) {
				out = out[:lim]
			}
		case stage["$sample"] != nil:
			spec, ok := coerceBsonM(stage["$sample"])
			if !ok {
				return nil, fmt.Errorf("$sample stage must be a document")
			}
			size, err := parseSampleSize(spec)
			if err != nil {
				return nil, err
			}
			out = applySample(out, size)
		case stage["$facet"] != nil:
			spec, ok := coerceBsonM(stage["$facet"])
			if !ok {
				return nil, fmt.Errorf("$facet stage must be a document")
			}
			var err error
			out, err = applyFacet(out, spec, resolve)
			if err != nil {
				return nil, err
			}
		case stage["$setWindowFields"] != nil:
			spec, ok := coerceBsonM(stage["$setWindowFields"])
			if !ok {
				return nil, fmt.Errorf("$setWindowFields stage must be a document")
			}
			var err error
			out, err = applySetWindowFields(out, spec)
			if err != nil {
				return nil, err
			}
		case stage["$replaceRoot"] != nil:
			spec, ok := coerceBsonM(stage["$replaceRoot"])
			if !ok {
				return nil, fmt.Errorf("$replaceRoot stage must be a document")
			}
			var err error
			out, err = applyReplaceRoot(out, spec)
			if err != nil {
				return nil, err
			}
		case stage["$replaceWith"] != nil:
			var err error
			out, err = applyReplaceWith(out, stage["$replaceWith"])
			if err != nil {
				return nil, err
			}
		case stage["$sortByCount"] != nil:
			var err error
			out, err = applySortByCount(out, stage["$sortByCount"])
			if err != nil {
				return nil, err
			}
		case stage["$unionWith"] != nil:
			if resolve == nil {
				return nil, fmt.Errorf("$unionWith requires resolver")
			}
			var err error
			out, err = applyUnionWith(out, stage["$unionWith"], resolve)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported aggregation stage: %v", stage)
		}
	}
	for _, d := range out {
		delete(d, mogVectorSearchScoreKey)
	}
	return out, nil
}
