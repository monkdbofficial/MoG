package shared

import (
	"encoding/json"
	"fmt"
	"sort"

	"gopkg.in/mgo.v2/bson"
)

func MarshalObject(v interface{}) (string, error) {
	// Prefer mgo/bson's JSON marshaler so BSON-specific types produced by drivers
	// (e.g. ObjectId, Date, Binary) become valid JSON.
	if b, err := bson.MarshalJSON(v); err == nil {
		return string(b), nil
	}

	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("failed to marshal object: %w", err)
	}
	return string(b), nil
}

// orderTopLevelDocForReply makes field order stable for clients:
// `_id` first, then remaining top-level keys in alphabetical order.
// This avoids expensive recursive ordering on large result batches.
func OrderTopLevelDocForReply(m bson.M) bson.D {
	if m == nil {
		return bson.D{}
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		if k == "" || k == "_id" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make(bson.D, 0, len(m))
	if id, ok := m["_id"]; ok {
		out = append(out, bson.DocElem{Name: "_id", Value: id})
	}
	for _, k := range keys {
		out = append(out, bson.DocElem{Name: k, Value: m[k]})
	}
	return out
}

func CoerceBsonM(v interface{}) (bson.M, bool) {
	switch t := v.(type) {
	case bson.M:
		return t, true
	case map[string]interface{}:
		return bson.M(t), true
	case bson.D:
		m := bson.M{}
		for _, e := range t {
			m[e.Name] = e.Value
		}
		return m, true
	case []byte:
		var m bson.M
		if err := bson.Unmarshal(t, &m); err == nil {
			return m, true
		}
		if err := bson.UnmarshalJSON(t, &m); err == nil {
			return m, true
		}
		return nil, false
	case string:
		var m bson.M
		if err := bson.UnmarshalJSON([]byte(t), &m); err == nil {
			return m, true
		}
		return nil, false
	default:
		return nil, false
	}
}
