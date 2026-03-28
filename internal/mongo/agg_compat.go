package mongo

import (
	"mog/internal/mongo/agg"

	"gopkg.in/mgo.v2/bson"
)

// These helpers are implemented in the aggregation engine subpackage but are also
// used by non-aggregation code paths (updates, schema inference, etc.). Keep
// the historical unexported names in package mongo via thin wrappers.

func asInt(v interface{}) (int, error) {
	return agg.AsInt(v)
}

func matchDoc(doc bson.M, filter bson.M) bool {
	return agg.MatchDoc(doc, filter)
}

func toFloat64Match(v interface{}) (float64, bool) {
	return agg.ToFloat64Match(v)
}

func isNumericOne(v interface{}) bool {
	return agg.IsNumericOne(v)
}

func getPathValue(doc bson.M, path string) interface{} {
	return agg.GetPathValue(doc, path)
}

func setPathValue(doc bson.M, path string, value interface{}) {
	agg.SetPathValue(doc, path, value)
}

func deepCloneDoc(doc bson.M) bson.M {
	return agg.DeepCloneDoc(doc)
}

func deepClone(v interface{}) interface{} {
	return agg.DeepClone(v)
}

func matchEquals(a, b interface{}) bool {
	return agg.MatchEquals(a, b)
}
