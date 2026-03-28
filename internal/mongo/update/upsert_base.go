package update

import (
	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/pipeline"
)

// BuildUpsertBaseDoc creates a base document for upserts using simple equality terms from the filter.
// Operator filters (e.g. {"age":{"$gte":1}}) are ignored.
func BuildUpsertBaseDoc(filter bson.M) bson.M {
	doc := bson.M{}
	for path, v := range filter {
		if m, ok := pipeline.CoerceBsonM(v); ok && pipeline.DocHasOperatorKeys(m) {
			continue
		}
		pipeline.SetPathValue(doc, path, pipeline.DeepClone(v))
	}
	return doc
}
