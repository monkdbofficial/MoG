package pipeline

import (
	"fmt"
	"math"
	"sort"

	"gopkg.in/mgo.v2/bson"
)

// applyVectorSearch evaluates a $vectorSearch stage in-memory.
func applyVectorSearch(docs []bson.M, spec bson.M) ([]bson.M, error) {
	path, _ := spec["path"].(string)
	if path == "" {
		return nil, fmt.Errorf("$vectorSearch requires non-empty path")
	}
	rawQuery, ok := spec["queryVector"]
	if !ok || rawQuery == nil {
		return nil, fmt.Errorf("$vectorSearch requires queryVector")
	}
	rawArr, ok := coerceInterfaceSlice(rawQuery)
	if !ok || len(rawArr) == 0 {
		return nil, fmt.Errorf("$vectorSearch queryVector must be a non-empty array")
	}
	query, ok := coerceFloat64Slice(rawArr)
	if !ok {
		return nil, fmt.Errorf("$vectorSearch queryVector must be numeric")
	}
	limit := 0
	if rawLim, ok := spec["limit"]; ok {
		if n, err := asInt(rawLim); err == nil {
			limit = n
		}
	}
	if limit <= 0 {
		limit = 10
	}

	qnorm := l2Norm(query)
	if qnorm == 0 {
		return []bson.M{}, nil
	}

	type scored struct {
		doc   bson.M
		score float64
	}
	scoredDocs := make([]scored, 0, len(docs))
	for _, d := range docs {
		rawV := getPathValue(d, path)
		arr, ok := coerceInterfaceSlice(rawV)
		if !ok {
			continue
		}
		vec, ok := coerceFloat64Slice(arr)
		if !ok || len(vec) != len(query) {
			continue
		}
		vnorm := l2Norm(vec)
		if vnorm == 0 {
			continue
		}
		dot := 0.0
		for i := range vec {
			dot += vec[i] * query[i]
		}
		score := dot / (vnorm * qnorm)

		nd := bson.M{}
		for k, vv := range d {
			nd[k] = vv
		}
		nd[mogVectorSearchScoreKey] = score
		scoredDocs = append(scoredDocs, scored{doc: nd, score: score})
	}

	sort.SliceStable(scoredDocs, func(i, j int) bool {
		return scoredDocs[i].score > scoredDocs[j].score
	})
	if limit > len(scoredDocs) {
		limit = len(scoredDocs)
	}
	out := make([]bson.M, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, scoredDocs[i].doc)
	}
	return out, nil
}

// l2Norm is a helper used by the adapter.
func l2Norm(v []float64) float64 {
	sum := 0.0
	for _, x := range v {
		sum += x * x
	}
	return math.Sqrt(sum)
}
