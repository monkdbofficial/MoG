package pipeline

import "gopkg.in/mgo.v2/bson"

// LookupResolver resolves a collection name used by $lookup/$unionWith/$graphLookup
// into a slice of documents.
//
// It is used by ApplyPipelineWithLookup to emulate cross-collection stages.
type LookupResolver func(from string) ([]bson.M, error)

// ApplyPipeline applies an aggregation pipeline to in-memory documents.
func ApplyPipeline(docs []bson.M, pipeline []bson.M) ([]bson.M, error) {
	return applyPipeline(docs, pipeline)
}

// CoerceBsonM converts common BSON/JSON representations into a bson.M.
func CoerceBsonM(v interface{}) (bson.M, bool) {
	return coerceBsonM(v)
}

// DocHasOperatorKeys reports whether m contains any "$"-prefixed keys.
func DocHasOperatorKeys(m bson.M) bool {
	return docHasOperatorKeys(m)
}

// CoerceInterfaceSlice attempts to view v as a slice/array of interface{} while
// avoiding treating raw bytes as an array for MongoDB-like semantics.
func CoerceInterfaceSlice(v interface{}) ([]interface{}, bool) {
	return coerceInterfaceSlice(v)
}

// ApplyPipelineWithLookup applies an aggregation pipeline to in-memory documents and
// provides a resolver for stages that need to read from other collections.
func ApplyPipelineWithLookup(docs []bson.M, pipeline []bson.M, resolve LookupResolver) ([]bson.M, error) {
	return applyPipelineWithLookup(docs, pipeline, lookupResolver(resolve))
}

// ApplyMatch filters documents using a MongoDB-style $match filter document.
func ApplyMatch(docs []bson.M, filter bson.M) []bson.M {
	return applyMatch(docs, filter)
}

// ApplySort sorts documents using a MongoDB-style $sort document.
func ApplySort(docs []bson.M, spec bson.M) []bson.M {
	return applySort(docs, spec)
}

// MatchDoc evaluates a MongoDB-style filter document against a single document.
func MatchDoc(doc bson.M, filter bson.M) bool {
	return matchDoc(doc, filter)
}

// ToFloat64Match converts common numeric types used in filters to float64.
func ToFloat64Match(v interface{}) (float64, bool) {
	return toFloat64Match(v)
}

// AsInt converts common numeric-like values used in stage specs to int.
func AsInt(v interface{}) (int, error) {
	return asInt(v)
}

// IsNumericOne reports whether v is numerically equal to 1 for common number types.
func IsNumericOne(v interface{}) bool {
	return isNumericOne(v)
}

// GetPathValue returns the value at a dotted path within doc.
func GetPathValue(doc bson.M, path string) interface{} {
	return getPathValue(doc, path)
}

// SetPathValue sets a dotted path within doc, creating intermediate objects as needed.
func SetPathValue(doc bson.M, path string, value interface{}) {
	setPathValue(doc, path, value)
}

// UnsetPathValue deletes a dotted path from doc if present.
func UnsetPathValue(doc bson.M, path string) {
	unsetPathValue(doc, path)
}

// DeepCloneDoc recursively clones a bson.M.
func DeepCloneDoc(doc bson.M) bson.M {
	return deepCloneDoc(doc)
}

// DeepClone recursively clones common BSON/JSON values.
func DeepClone(v interface{}) interface{} {
	return deepClone(v)
}

// MatchEquals implements MongoDB-like equality semantics used by match evaluation.
func MatchEquals(a, b interface{}) bool {
	return matchEquals(a, b)
}
