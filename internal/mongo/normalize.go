package mongo

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gopkg.in/mgo.v2/bson"
)

func normalizeRowValue(v interface{}) interface{} {
	switch t := v.(type) {
	case []byte:
		if !utf8.Valid(t) {
			return t
		}
		s := strings.TrimSpace(string(t))
		if (strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}")) || (strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]")) {
			var out interface{}
			if err := json.Unmarshal([]byte(s), &out); err == nil {
				switch vv := out.(type) {
				case map[string]interface{}:
					return bson.M(vv)
				case []interface{}:
					return vv
				}
			}
		}
		return string(t)
	case string:
		s := strings.TrimSpace(t)
		if (strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}")) || (strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]")) {
			var out interface{}
			if err := json.Unmarshal([]byte(s), &out); err == nil {
				switch vv := out.(type) {
				case map[string]interface{}:
					return bson.M(vv)
				case []interface{}:
					return vv
				}
			}
		}
		return t
	default:
		return v
	}
}

func normalizeSQLArgs(args []interface{}) ([]interface{}, error) {
	out := make([]interface{}, 0, len(args))
	for _, a := range args {
		switch a.(type) {
		case bson.ObjectId:
			out = append(out, a.(bson.ObjectId).Hex())
		case bson.M, bson.D, map[string]interface{}, []interface{}:
			s, err := marshalObject(a)
			if err != nil {
				return nil, err
			}
			out = append(out, s)
		default:
			out = append(out, a)
		}
	}
	return out, nil
}

func normalizeDocForReply(doc bson.M) {
	for k, v := range doc {
		doc[k] = normalizeValueForReply(k, v)
	}
}

func normalizeDocForStorage(doc bson.M) {
	// Ensure _id exists and is stored as a stable string (hex) so the backend doesn't persist
	// Extended JSON objects like {"$oid": "..."}.
	if v, ok := doc["_id"]; ok {
		switch id := v.(type) {
		case bson.ObjectId:
			doc["_id"] = id.Hex()
		case map[string]interface{}:
			if oid, ok := id["$oid"].(string); ok && bson.IsObjectIdHex(oid) {
				doc["_id"] = oid
			}
		case bson.M:
			if oid, ok := id["$oid"].(string); ok && bson.IsObjectIdHex(oid) {
				doc["_id"] = oid
			}
		}
	} else {
		doc["_id"] = bson.NewObjectId().Hex()
	}
}

func normalizeValueForReply(key string, v interface{}) interface{} {
	switch t := v.(type) {
	case bson.M:
		normalizeDocForReply(t)
		return t
	case map[string]interface{}:
		m := bson.M(t)
		normalizeDocForReply(m)

		// Handle Extended JSON objects commonly produced by bson.MarshalJSON.
		if oid, ok := m["$oid"].(string); ok && bson.IsObjectIdHex(oid) {
			return bson.ObjectIdHex(oid)
		}
		if rawDate, ok := m["$date"]; ok {
			switch dt := rawDate.(type) {
			case string:
				if ts, err := time.Parse(time.RFC3339Nano, dt); err == nil {
					return ts
				}
			case float64:
				// Treat as milliseconds since epoch.
				return time.Unix(0, int64(dt)*int64(time.Millisecond)).UTC()
			case map[string]interface{}:
				// {"$date":{"$numberLong":"..."}}
				if n, ok := dt["$numberLong"].(string); ok {
					if ms, err := parseInt64(n); err == nil {
						return time.Unix(0, ms*int64(time.Millisecond)).UTC()
					}
				}
			}
		}
		if rawBin, ok := m["$binary"]; ok {
			if spec, ok := rawBin.(map[string]interface{}); ok {
				if b64, ok := spec["base64"].(string); ok {
					if b, err := base64.StdEncoding.DecodeString(b64); err == nil {
						return b
					}
				}
			}
		}
		if n, ok := m["$numberInt"].(string); ok {
			if i64, err := parseInt64(n); err == nil {
				if i64 >= math.MinInt32 && i64 <= math.MaxInt32 {
					return int32(i64)
				}
				return i64
			}
		}
		if n, ok := m["$numberLong"].(string); ok {
			if i64, err := parseInt64(n); err == nil {
				return i64
			}
		}
		if n, ok := m["$numberDouble"].(string); ok {
			if f, err := parseFloat64(n); err == nil {
				return f
			}
		}

		return m
	case []interface{}:
		out := make([]interface{}, 0, len(t))
		for _, el := range t {
			out = append(out, normalizeValueForReply("", el))
		}
		return out
	case float64:
		// Best-effort type preservation: if value is integral, return int32/int64.
		if isIntegralFloat(t) {
			i64 := int64(t)
			if float64(i64) == t {
				if i64 >= math.MinInt32 && i64 <= math.MaxInt32 {
					return int32(i64)
				}
				return i64
			}
		}
		return t
	case float32:
		f := float64(t)
		if isIntegralFloat(f) {
			i64 := int64(f)
			if float64(i64) == f {
				if i64 >= math.MinInt32 && i64 <= math.MaxInt32 {
					return int32(i64)
				}
				return i64
			}
		}
		return t
	case string:
		// Normalize _id stored as a hex string into a BSON ObjectId for driver compatibility.
		if key == "_id" && len(t) == 24 && bson.IsObjectIdHex(t) {
			return bson.ObjectIdHex(t)
		}
		return t
	default:
		return v
	}
}

func isIntegralFloat(f float64) bool {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return false
	}
	return math.Trunc(f) == f
}

func parseInt64(s string) (int64, error) {
	// strconv.ParseInt doesn't accept leading '+' for some cases in extjson, but it's fine.
	return strconv.ParseInt(s, 10, 64)
}

func parseFloat64(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}
