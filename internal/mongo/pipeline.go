package mongo

import (
	"mog/internal/mongo/agg"

	"gopkg.in/mgo.v2/bson"
)

// lookupResolver matches the historical in-package type used by applyPipelineWithLookup.
// It is kept in package mongo so call sites don't need to import subpackages.
type lookupResolver func(from string) ([]bson.M, error)

func applyPipeline(docs []bson.M, pipeline []bson.M) ([]bson.M, error) {
	return agg.ApplyPipeline(docs, pipeline)
}

func applyPipelineWithLookup(docs []bson.M, pipeline []bson.M, resolve lookupResolver) ([]bson.M, error) {
	return agg.ApplyPipelineWithLookup(docs, pipeline, agg.LookupResolver(resolve))
}

func applyMatch(docs []bson.M, filter bson.M) []bson.M {
	return agg.ApplyMatch(docs, filter)
}

func applySort(docs []bson.M, spec bson.M) []bson.M {
	return agg.ApplySort(docs, spec)
}
