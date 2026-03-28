package agg

import (
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/mgo.v2/bson"
)

// coerceBsonM converts common BSON/JSON representations into a bson.M.
//
// This helper is intentionally permissive because aggregation stages often accept
// both BSON documents and JSON-like maps/arrays depending on the driver.
func coerceBsonM(v interface{}) (bson.M, bool) {
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

func marshalObject(v interface{}) (string, error) {
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

func coerceFloat64Slice(arr []interface{}) ([]float64, bool) {
	out := make([]float64, 0, len(arr))
	for _, v := range arr {
		switch t := v.(type) {
		case float64:
			out = append(out, t)
		case float32:
			out = append(out, float64(t))
		case int:
			out = append(out, float64(t))
		case int32:
			out = append(out, float64(t))
		case int64:
			out = append(out, float64(t))
		default:
			return nil, false
		}
	}
	return out, true
}

func unsetPathValue(doc bson.M, path string) {
	if path == "" {
		return
	}
	cur := interface{}(doc)
	parts := strings.Split(path, ".")
	for _, p := range parts[:len(parts)-1] {
		m, ok := coerceBsonM(cur)
		if !ok {
			return
		}
		v, ok := m[p]
		if !ok || v == nil {
			return
		}
		cur = v
	}
	m, ok := coerceBsonM(cur)
	if !ok {
		return
	}
	delete(m, parts[len(parts)-1])
}
