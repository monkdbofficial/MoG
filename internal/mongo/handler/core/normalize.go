package mongo

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/handler/shared"
)

const (
	mongoKeyDotEsc    = "\uFF0E" // fullwidth full stop (．)
	mongoKeyDollarEsc = "\uFF04" // fullwidth dollar sign (＄)
	mogMixedArrayWrap = "__mog_v__"

	mogBinKey      = "__mog_bin__"
	mogBinKindKey  = "__mog_kind__"
	mogCodeKey     = "__mog_code__"
	mogScopeKey    = "__mog_scope__"
	mogDBPtrKey    = "__mog_dbptr__"
	mogDBPtrNSKey  = "__mog_ns__"
	mogDBPtrIDKey  = "__mog_id__"
	mogRegexKey    = "__mog_regex__"
	mogRegexPatKey = "__mog_pat__"
	mogRegexOptKey = "__mog_opt__"
	mogMinKey      = "__mog_minkey__"
	mogMaxKey      = "__mog_maxkey__"
	mogDec128Key   = "__mog_decimal128__"
	mogRawKey      = "__mog_raw__"
	mogRawKindKey  = "__mog_raw_kind__"
	mogRawDataKey  = "__mog_raw_data__"
)

// normalizeRowValue is a helper used by the adapter.
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

// normalizeSQLArgs is a helper used by the adapter.
func normalizeSQLArgs(args []interface{}) ([]interface{}, error) {
	out := make([]interface{}, 0, len(args))
	for _, a := range args {
		switch a.(type) {
		case bson.ObjectId:
			out = append(out, a.(bson.ObjectId).Hex())
		case bson.M, bson.D, map[string]interface{}, []interface{}:
			s, err := shared.MarshalObject(a)
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

// normalizeDocForReply is a helper used by the adapter.
func normalizeDocForReply(doc bson.M) {
	if doc == nil {
		return
	}
	// Decode escaped keys before doing type rehydration.
	decoded := bson.M{}
	changed := false
	for k, v := range doc {
		nk := decodeMongoKey(k)
		if nk != k {
			changed = true
		}
		decoded[nk] = v
	}
	if changed {
		for k := range doc {
			delete(doc, k)
		}
		for k, v := range decoded {
			doc[k] = v
		}
	}
	for k, v := range doc {
		doc[k] = normalizeValueForReply(k, v)
	}
}

// normalizeDocForStorage is a helper used by the adapter.
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

	// Keep storage representation stable and SQL-friendly:
	// - store ObjectIds as hex strings (works better with SQL pushdown and parameter binding)
	// - leave time.Time intact (typed columns can store it; raw JSON will still be ISO strings)
	out := bson.M{"_id": doc["_id"]}
	for k, v := range doc {
		if k == "" || k == "_id" {
			continue
		}
		out[encodeMongoKey(k)] = normalizeValueForStorage(v)
	}
	for k := range doc {
		delete(doc, k)
	}
	for k, v := range out {
		doc[k] = v
	}
}

// normalizeValueForStorage is a helper used by the adapter.
func normalizeValueForStorage(v interface{}) interface{} {
	// Handle special sentinel values first (unexported types in bson).
	if v == bson.MinKey {
		return bson.M{mogMinKey: true}
	}
	if v == bson.MaxKey {
		return bson.M{mogMaxKey: true}
	}

	switch t := v.(type) {
	case bson.ObjectId:
		return t.Hex()
	case bson.D:
		m := bson.M{}
		for _, e := range t {
			if e.Name == "" {
				continue
			}
			m[e.Name] = e.Value
		}
		return normalizeValueForStorage(m)
	case []byte:
		// Many drivers decode BSON binary into raw []byte when unmarshalling into interface{}.
		// Store it as a JSON-safe wrapper so it can round-trip and be eligible for BLOB offload.
		return bson.M{
			mogBinKey:     base64.StdEncoding.EncodeToString(t),
			mogBinKindKey: int32(0),
		}
	case bson.Binary:
		// Keep binary data representable inside OBJECT(DYNAMIC) without requiring backend-specific
		// binary handling.
		return bson.M{
			mogBinKey:     base64.StdEncoding.EncodeToString(t.Data),
			mogBinKindKey: int32(t.Kind),
		}
	case bson.JavaScript:
		return bson.M{
			mogCodeKey:  t.Code,
			mogScopeKey: normalizeValueForStorage(t.Scope),
		}
	case bson.DBPointer:
		return bson.M{
			mogDBPtrKey: bson.M{
				mogDBPtrNSKey: t.Namespace,
				mogDBPtrIDKey: t.Id.Hex(),
			},
		}
	case bson.RegEx:
		return bson.M{
			mogRegexKey: bson.M{
				mogRegexPatKey: t.Pattern,
				mogRegexOptKey: t.Options,
			},
		}
	case bson.Raw:
		// mgo/bson uses Raw for kinds it doesn't natively understand (e.g. Decimal128).
		// Represent these as plain JSON so storage doesn't fail.
		if t.Kind == 0x13 && len(t.Data) == 16 { // Decimal128 per BSON spec
			return bson.M{mogDec128Key: hex.EncodeToString(t.Data)}
		}
		return bson.M{
			mogRawKey: bson.M{
				mogRawKindKey: int32(t.Kind),
				mogRawDataKey: base64.StdEncoding.EncodeToString(t.Data),
			},
		}
	case bson.M:
		if vv, ok := coerceExtendedJSONValue(t, false); ok {
			return normalizeValueForStorage(vv)
		}
		out := bson.M{}
		for k, vv := range t {
			if k == "" {
				continue
			}
			out[encodeMongoKey(k)] = normalizeValueForStorage(vv)
		}
		return out
	case map[string]interface{}:
		m := bson.M(t)
		if vv, ok := coerceExtendedJSONValue(m, false); ok {
			return normalizeValueForStorage(vv)
		}
		out := bson.M{}
		for k, vv := range m {
			if k == "" {
				continue
			}
			out[encodeMongoKey(k)] = normalizeValueForStorage(vv)
		}
		return out
	case []interface{}:
		out := make([]interface{}, 0, len(t))
		for i := range t {
			out = append(out, normalizeValueForStorage(t[i]))
		}
		if isMixedMonkArray(out) {
			wrapped := make([]interface{}, 0, len(out))
			for _, el := range out {
				// Wrap every element (including nil) so the backend sees a homogeneous list type.
				wrapped = append(wrapped, bson.M{mogMixedArrayWrap: el})
			}
			return wrapped
		}
		return out
	default:
		return v
	}
}

// normalizeValueForReply is a helper used by the adapter.
func normalizeValueForReply(key string, v interface{}) interface{} {
	switch t := v.(type) {
	case bson.M:
		if vv, ok := coerceMogWrapperForReply(t); ok {
			return vv
		}
		if vv, ok := coerceExtendedJSONValue(t, true); ok {
			return vv
		}
		normalizeDocForReply(t)
		return t
	case bson.D:
		m := bson.M{}
		for _, e := range t {
			if e.Name == "" {
				continue
			}
			m[e.Name] = e.Value
		}
		return normalizeValueForReply(key, m)
	case map[string]interface{}:
		m := bson.M(t)
		if vv, ok := coerceMogWrapperForReply(m); ok {
			return vv
		}
		if vv, ok := coerceExtendedJSONValue(m, true); ok {
			return vv
		}
		normalizeDocForReply(m)
		return m
	case []interface{}:
		out := make([]interface{}, 0, len(t))
		for _, el := range t {
			out = append(out, normalizeValueForReply("", el))
		}
		if unwrapped, ok := unwrapMixedArray(out); ok {
			return unwrapped
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
		// Normalize ObjectId-like fields stored as hex strings back into BSON ObjectIds.
		if looksLikeObjectIDKey(key) && len(t) == 24 && bson.IsObjectIdHex(t) {
			return bson.ObjectIdHex(t)
		}
		// Rehydrate RFC3339 timestamps stored as strings inside JSON payloads (e.g. arrays).
		if looksLikeTimeKey(key) {
			if ts, ok := parseRFC3339Time(t); ok {
				return ts
			}
		}
		// Normalize _id stored as a hex string into a BSON ObjectId for driver compatibility.
		if key == "_id" && len(t) == 24 && bson.IsObjectIdHex(t) {
			return bson.ObjectIdHex(t)
		}
		return t
	default:
		return v
	}
}

// coerceMogWrapperForReply is a helper used by the adapter.
func coerceMogWrapperForReply(m bson.M) (interface{}, bool) {
	// Only unwrap our own sentinel objects when they have an exact expected shape,
	// to reduce the risk of clobbering user data.

	// __mog_v__ wrapper for mixed arrays is handled at the array level.

	if len(m) == 1 {
		if _, ok := m[mogMinKey]; ok {
			return bson.MinKey, true
		}
		if _, ok := m[mogMaxKey]; ok {
			return bson.MaxKey, true
		}
		if raw, ok := m[mogDec128Key]; ok {
			// Return as Extended JSON marker object for client visibility (not native Decimal128
			// since mgo/bson doesn't expose that type).
			return bson.M{"$numberDecimal": fmt.Sprint(raw)}, true
		}
		if raw, ok := m[mogRawKey]; ok {
			// Preserve as a plain object in replies.
			return bson.M{mogRawKey: raw}, true
		}
	}

	if b64, ok := m[mogBinKey]; ok {
		if kindV, ok2 := m[mogBinKindKey]; ok2 {
			kind := byte(0)
			switch kk := kindV.(type) {
			case int:
				kind = byte(kk)
			case int32:
				kind = byte(kk)
			case int64:
				kind = byte(kk)
			case float64:
				kind = byte(int64(kk))
			}
			if s, ok3 := b64.(string); ok3 {
				if data, err := base64.StdEncoding.DecodeString(s); err == nil {
					return bson.Binary{Kind: kind, Data: data}, true
				}
			}
		}
	}

	if code, ok := m[mogCodeKey]; ok {
		if codeS, ok2 := code.(string); ok2 {
			scope := m[mogScopeKey]
			return bson.JavaScript{Code: codeS, Scope: scope}, true
		}
	}

	if ptr, ok := m[mogDBPtrKey]; ok {
		if mm, ok2 := ptr.(bson.M); ok2 {
			ns, _ := mm[mogDBPtrNSKey].(string)
			idRaw := mm[mogDBPtrIDKey]
			if idS, ok3 := idRaw.(string); ok3 && bson.IsObjectIdHex(idS) {
				return bson.DBPointer{Namespace: ns, Id: bson.ObjectIdHex(idS)}, true
			}
		}
	}

	if rx, ok := m[mogRegexKey]; ok {
		if mm, ok2 := rx.(bson.M); ok2 {
			pat, _ := mm[mogRegexPatKey].(string)
			opt, _ := mm[mogRegexOptKey].(string)
			return bson.RegEx{Pattern: pat, Options: opt}, true
		}
	}

	return nil, false
}

// unwrapMixedArray is a helper used by the adapter.
func unwrapMixedArray(arr []interface{}) ([]interface{}, bool) {
	if len(arr) == 0 {
		return nil, false
	}
	out := make([]interface{}, 0, len(arr))
	for _, el := range arr {
		m, ok := el.(bson.M)
		if !ok {
			return nil, false
		}
		if len(m) != 1 {
			return nil, false
		}
		v, ok := m[mogMixedArrayWrap]
		if !ok {
			return nil, false
		}
		out = append(out, v)
	}
	return out, true
}

// isMixedMonkArray is a helper used by the adapter.
func isMixedMonkArray(arr []interface{}) bool {
	// MonkDB OBJECT lists require a homogeneous element type.
	// Consider it "mixed" if we see multiple scalar kinds, nested arrays mixed with scalars,
	// or both integer and floating numeric values.
	seenKind := ""
	seenInt := false
	seenFloat := false

	for _, el := range arr {
		if el == nil {
			// NULL elements are fine if the rest is homogeneous.
			continue
		}

		kind := ""
		switch t := el.(type) {
		case bool:
			kind = "bool"
		case int, int32, int64, int16, int8, uint8, uint16, uint32, uint64:
			kind = "number"
			seenInt = true
			_ = t
		case float32, float64:
			kind = "number"
			seenFloat = true
		case string, bson.Symbol:
			kind = "text"
		case time.Time:
			kind = "time"
		case []byte:
			kind = "bytes"
		default:
			if _, ok := shared.CoerceBsonM(el); ok {
				kind = "object"
				break
			}
			if _, ok := shared.CoerceInterfaceSlice(el); ok {
				kind = "array"
				break
			}
			kind = "other"
		}

		if seenKind == "" {
			seenKind = kind
		} else if kind != seenKind {
			return true
		}
	}

	if seenInt && seenFloat {
		return true
	}
	return false
}

// coerceExtendedJSONValue converts a subset of Mongo Extended JSON objects (as produced
// by bson.MarshalJSON) into native Go values.
//
// To reduce false positives on user documents, it only triggers when the object has
// exactly one key and that key is a recognized "$"-prefixed Extended JSON marker.
func coerceExtendedJSONValue(m bson.M, forReply bool) (interface{}, bool) {
	if len(m) != 1 {
		return nil, false
	}

	if oid, ok := m["$oid"].(string); ok && bson.IsObjectIdHex(oid) {
		if forReply {
			return bson.ObjectIdHex(oid), true
		}
		// Storage: keep as hex string.
		return oid, true
	}

	if rawDate, ok := m["$date"]; ok {
		switch dt := rawDate.(type) {
		case string:
			if ts, err := time.Parse(time.RFC3339Nano, dt); err == nil {
				return ts, true
			}
			if ts, err := time.Parse(time.RFC3339, dt); err == nil {
				return ts, true
			}
		case float64:
			// Treat as milliseconds since epoch.
			return time.Unix(0, int64(dt)*int64(time.Millisecond)).UTC(), true
		case map[string]interface{}:
			// {"$date":{"$numberLong":"..."}}
			if n, ok := dt["$numberLong"].(string); ok {
				if ms, err := parseInt64(n); err == nil {
					return time.Unix(0, ms*int64(time.Millisecond)).UTC(), true
				}
			}
		case bson.M:
			if n, ok := dt["$numberLong"].(string); ok {
				if ms, err := parseInt64(n); err == nil {
					return time.Unix(0, ms*int64(time.Millisecond)).UTC(), true
				}
			}
		}
	}

	if rawBin, ok := m["$binary"]; ok {
		switch spec := rawBin.(type) {
		case map[string]interface{}:
			if b64, ok := spec["base64"].(string); ok {
				if b, err := base64.StdEncoding.DecodeString(b64); err == nil {
					return b, true
				}
			}
		case bson.M:
			if b64, ok := spec["base64"].(string); ok {
				if b, err := base64.StdEncoding.DecodeString(b64); err == nil {
					return b, true
				}
			}
		}
	}

	if n, ok := m["$numberInt"].(string); ok {
		if i64, err := parseInt64(n); err == nil {
			if i64 >= math.MinInt32 && i64 <= math.MaxInt32 {
				return int32(i64), true
			}
			return i64, true
		}
	}
	if raw, ok := m["$numberInt"]; ok {
		switch t := raw.(type) {
		case float64:
			i64 := int64(t)
			if i64 >= math.MinInt32 && i64 <= math.MaxInt32 {
				return int32(i64), true
			}
			return i64, true
		case int:
			return int32(t), true
		case int32:
			return t, true
		case int64:
			if t >= math.MinInt32 && t <= math.MaxInt32 {
				return int32(t), true
			}
			return t, true
		}
	}
	if n, ok := m["$numberLong"].(string); ok {
		if i64, err := parseInt64(n); err == nil {
			return i64, true
		}
		// Some producers might emit raw numbers.
		if f, err := parseFloat64(n); err == nil {
			return int64(f), true
		}
	}
	if raw, ok := m["$numberLong"]; ok {
		switch t := raw.(type) {
		case float64:
			return int64(t), true
		case int:
			return int64(t), true
		case int32:
			return int64(t), true
		case int64:
			return t, true
		case uint64:
			return int64(t), true
		}
	}
	if n, ok := m["$numberDouble"].(string); ok {
		if f, err := parseFloat64(n); err == nil {
			return f, true
		}
	}
	if raw, ok := m["$numberDouble"]; ok {
		switch t := raw.(type) {
		case float64:
			return t, true
		case float32:
			return float64(t), true
		case int:
			return float64(t), true
		case int32:
			return float64(t), true
		case int64:
			return float64(t), true
		}
	}

	return nil, false
}

// encodeMongoKey is a helper used by the adapter.
func encodeMongoKey(key string) string {
	if key == "" || key == "_id" {
		return key
	}
	// MonkDB treats dots as object-path separators in OBJECT columns, so keys
	// containing '.' must be escaped to remain representable.
	key = strings.ReplaceAll(key, ".", mongoKeyDotEsc)
	// '$' keys are reserved in Mongo semantics and also collide with Extended JSON markers.
	key = strings.ReplaceAll(key, "$", mongoKeyDollarEsc)
	return key
}

// decodeMongoKey is a helper used by the adapter.
func decodeMongoKey(key string) string {
	if key == "" || key == "_id" {
		return key
	}
	key = strings.ReplaceAll(key, mongoKeyDotEsc, ".")
	key = strings.ReplaceAll(key, mongoKeyDollarEsc, "$")
	return key
}

// looksLikeObjectIDKey is a helper used by the adapter.
func looksLikeObjectIDKey(key string) bool {
	if key == "" {
		return false
	}
	if key == "_id" || key == "id" {
		return true
	}
	if strings.HasSuffix(key, "_id") || strings.HasSuffix(key, "Id") || strings.HasSuffix(key, "ID") {
		return true
	}
	return false
}

// looksLikeTimeKey is a helper used by the adapter.
func looksLikeTimeKey(key string) bool {
	if key == "" {
		return false
	}
	switch key {
	case "ts", "at", "time", "timestamp", "createdAt", "updatedAt", "deletedAt":
		return true
	}
	if strings.HasSuffix(key, "_ts") || strings.HasSuffix(key, "_at") || strings.HasSuffix(key, "At") {
		return true
	}
	return false
}

// parseRFC3339Time is a helper used by the adapter.
func parseRFC3339Time(s string) (time.Time, bool) {
	// Support the most common encodings produced by JSON marshallers and drivers.
	if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return ts, true
	}
	if ts, err := time.Parse(time.RFC3339, s); err == nil {
		return ts, true
	}
	return time.Time{}, false
}

// isIntegralFloat is a helper used by the adapter.
func isIntegralFloat(f float64) bool {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return false
	}
	return math.Trunc(f) == f
}

// parseInt64 is a helper used by the adapter.
func parseInt64(s string) (int64, error) {
	// strconv.ParseInt doesn't accept leading '+' for some cases in extjson, but it's fine.
	return strconv.ParseInt(s, 10, 64)
}

// parseFloat64 is a helper used by the adapter.
func parseFloat64(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}
