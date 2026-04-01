package shared

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"gopkg.in/mgo.v2/bson"
)

func MarshalObject(v interface{}) (string, error) {
	v = prepareForObjectPayload(v)
	// Prefer the standard library JSON encoder to avoid Extended JSON wrappers like:
	//   {"$numberLong":"25"}
	// which some MonkDB backends will store as objects instead of numbers.
	//
	// MoG normalizes most BSON-specific types before calling MarshalObject.
	b, err := json.Marshal(v)
	if err != nil {
		// Fallback: mgo/bson's JSON marshaler understands more BSON-specific types, but it
		// may emit Extended JSON wrappers for int64/Decimal/etc.
		if bb, err2 := bson.MarshalJSON(v); err2 == nil {
			return string(bb), nil
		}
		return "", fmt.Errorf("failed to marshal object: %w", err)
	}
	return string(b), nil
}

func prepareForObjectPayload(v interface{}) interface{} {
	switch t := v.(type) {
	case nil:
		return nil
	case time.Time:
		// Use explicit Extended JSON for dates so OBJECT(DYNAMIC) subcolumn typing remains stable
		// (avoids string-vs-object conflicts on existing schemas).
		return bson.M{"$date": t.UTC().Format(time.RFC3339Nano)}
	case bson.ObjectId:
		return t.Hex()
	case bson.D:
		m := bson.M{}
		for _, e := range t {
			if e.Name == "" {
				continue
			}
			m[e.Name] = prepareForObjectPayload(e.Value)
		}
		return m
	case bson.M:
		m := bson.M{}
		for k, vv := range t {
			if k == "" {
				continue
			}
			m[k] = prepareForObjectPayload(vv)
		}
		return m
	case map[string]interface{}:
		m := map[string]interface{}{}
		for k, vv := range t {
			if k == "" {
				continue
			}
			m[k] = prepareForObjectPayload(vv)
		}
		return m
	case []interface{}:
		out := make([]interface{}, 0, len(t))
		for _, el := range t {
			out = append(out, prepareForObjectPayload(el))
		}
		return out
	default:
		// Best-effort: try to treat slices/arrays generically (but don't treat raw bytes as arrays).
		if arr, ok := CoerceInterfaceSlice(v); ok {
			out := make([]interface{}, 0, len(arr))
			for _, el := range arr {
				out = append(out, prepareForObjectPayload(el))
			}
			return out
		}
		if m, ok := CoerceBsonM(v); ok {
			return prepareForObjectPayload(m)
		}
		return v
	}
}

// OrderTopLevelDocForReply makes top-level reply field order stable for clients.
// Preferred field order is used first (typically SQL/select order), then any
// remaining keys are appended alphabetically. Nested values are left unchanged.
func OrderTopLevelDocForReply(m bson.M, preferred []string) bson.D {
	if m == nil {
		return bson.D{}
	}
	seen := map[string]struct{}{}
	out := make(bson.D, 0, len(m))
	for _, key := range preferred {
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		val, ok := m[key]
		if !ok {
			continue
		}
		out = append(out, bson.DocElem{Name: key, Value: val})
		seen[key] = struct{}{}
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		if k == "" {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
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
