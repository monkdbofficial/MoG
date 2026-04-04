package pipeline

import (
	"fmt"

	"gopkg.in/mgo.v2/bson"
)

// applyCollStats evaluates a $collStats stage (placeholder).
//
// In MongoDB, $collStats returns collection/storage-engine level statistics and
// does not depend on the input documents. MoG's in-memory pipeline evaluator
// cannot derive real storage/latency stats, but some clients (e.g. Compass) use
// this stage for UI metadata. We return a best-effort, schema-compatible shape
// with zero/empty values instead of failing the entire aggregation.
func applyCollStats(spec bson.M) ([]bson.M, error) {
	out := bson.M{}

	for k, raw := range spec {
		switch k {
		case "latencyStats":
			// Mongo expects a document; common form is { histograms: true }.
			if raw == nil {
				out["latencyStats"] = bson.M{}
				continue
			}
			if _, ok := coerceBsonM(raw); !ok {
				return nil, fmt.Errorf("$collStats.latencyStats must be a document")
			}
			out["latencyStats"] = bson.M{
				"reads":    bson.M{"ops": int64(0), "latency": int64(0), "histogram": []interface{}{}},
				"writes":   bson.M{"ops": int64(0), "latency": int64(0), "histogram": []interface{}{}},
				"commands": bson.M{"ops": int64(0), "latency": int64(0), "histogram": []interface{}{}},
			}
		case "storageStats":
			if raw == nil {
				out["storageStats"] = bson.M{}
				continue
			}
			if _, ok := coerceBsonM(raw); !ok {
				return nil, fmt.Errorf("$collStats.storageStats must be a document")
			}
			out["storageStats"] = bson.M{
				"size":            int64(0),
				"count":           int64(0),
				"avgObjSize":      int64(0),
				"storageSize":     int64(0),
				"freeStorageSize": int64(0),
				"capped":          false,
				"nindexes":        int64(0),
				"totalIndexSize":  int64(0),
				"totalSize":       int64(0),
				"indexSizes":      bson.M{},
			}
		default:
			// Accept unknown options for forward compatibility; ignore.
		}
	}

	return []bson.M{out}, nil
}
