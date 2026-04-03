package pipeline

import (
	"fmt"
	"sort"
	"time"

	"gopkg.in/mgo.v2/bson"
)

// $bucketAuto stage implementation (subset).
//
// This implementation focuses on:
// - groupBy: any expression that evaluates to a sortable scalar (nil, numbers, strings, bool, time)
// - buckets: positive integer
// - output: if omitted, emits "count" like MongoDB
//
// Granularity and complex output accumulators are intentionally not implemented yet.
func applyBucketAuto(docs []bson.M, spec bson.M) ([]bson.M, error) {
	rawGroupBy, okGB := spec["groupBy"]
	rawBuckets, okB := spec["buckets"]
	if !okGB || !okB {
		return nil, fmt.Errorf("$bucketAuto requires groupBy/buckets")
	}

	buckets, err := asInt(rawBuckets)
	if err != nil || buckets <= 0 {
		return nil, fmt.Errorf("$bucketAuto.buckets must be a positive integer")
	}

	if g, ok := spec["granularity"]; ok && g != nil {
		// MongoDB supports specific numeric granularities (e.g. R5/R10/E-series).
		// MoG currently does not implement boundary rounding.
		return nil, fmt.Errorf("unsupported $bucketAuto.granularity")
	}
	if outSpec, ok := spec["output"]; ok && outSpec != nil {
		return nil, fmt.Errorf("unsupported $bucketAuto.output")
	}

	type keyed struct {
		key interface{}
		doc bson.M
	}
	items := make([]keyed, 0, len(docs))
	for _, d := range docs {
		k, err := evalComputedWithOpts(d, rawGroupBy, evalOpts{sizeNonArrayZero: false, vars: stageVars(d, nil)})
		if err != nil {
			return nil, err
		}
		items = append(items, keyed{key: k, doc: d})
	}

	if len(items) == 0 {
		return []bson.M{}, nil
	}
	if buckets > len(items) {
		buckets = len(items)
	}

	sort.SliceStable(items, func(i, j int) bool {
		return bucketAutoLess(items[i].key, items[j].key)
	})

	// Split into roughly equal sized buckets.
	n := len(items)
	base := n / buckets
	rem := n % buckets
	start := 0
	out := make([]bson.M, 0, buckets)
	for i := 0; i < buckets; i++ {
		size := base
		if i < rem {
			size++
		}
		end := start + size
		if end > n {
			end = n
		}
		if start >= end {
			break
		}

		minK := items[start].key
		maxK := items[end-1].key
		out = append(out, bson.M{
			"_id":   bson.M{"min": minK, "max": maxK},
			"count": int64(end - start),
		})
		start = end
	}
	return out, nil
}

func bucketAutoLess(a, b interface{}) bool {
	ra := bucketAutoTypeRank(a)
	rb := bucketAutoTypeRank(b)
	if ra != rb {
		return ra < rb
	}

	// Nil values are equal within rank.
	if a == nil && b == nil {
		return false
	}

	if ai, ok := toInt64IfIntegral(a); ok {
		if bi, ok := toInt64IfIntegral(b); ok {
			return ai < bi
		}
	}
	if af, ok := toFloat64(a); ok {
		if bf, ok := toFloat64(b); ok {
			return af < bf
		}
	}
	if as, ok := a.(string); ok {
		if bs, ok := b.(string); ok {
			return as < bs
		}
	}
	if ab, ok := a.(bool); ok {
		if bb, ok := b.(bool); ok {
			return !ab && bb
		}
	}
	if at, ok := a.(time.Time); ok {
		if bt, ok := b.(time.Time); ok {
			return at.Before(bt)
		}
	}

	// Fallback: deterministic string compare.
	return fmt.Sprint(a) < fmt.Sprint(b)
}

func bucketAutoTypeRank(v interface{}) int {
	// Very small subset of BSON comparison order: null < numbers < strings < bool < date < other
	if v == nil {
		return 0
	}
	if _, ok := toFloat64(v); ok {
		return 1
	}
	if _, ok := v.(string); ok {
		return 2
	}
	if _, ok := v.(bool); ok {
		return 3
	}
	if _, ok := v.(time.Time); ok {
		return 4
	}
	return 5
}

