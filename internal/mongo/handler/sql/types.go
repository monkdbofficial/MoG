package sql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/handler/shared"
	mpipeline "mog/internal/mongo/pipeline"
)

// TypeForValue is a helper used by the adapter.
func TypeForValue(v interface{}) string {
	if v == nil {
		return ""
	}
	switch v.(type) {
	case bool:
		return "BOOLEAN"
	case string, bson.Symbol:
		return "TEXT"
	case bson.ObjectId:
		// Stored as hex strings for SQL compatibility and stable equality semantics.
		return "TEXT"
	case int8, uint8:
		return "BYTE"
	case int16, uint16:
		return "SHORT"
	case int32, uint32:
		return "INTEGER"
	case int, int64, uint64:
		return "LONG"
	case float32:
		return "FLOAT"
	case float64:
		return "DOUBLE PRECISION"
	case time.Time:
		// Prefer explicit TZ support when available; Mongo dates are UTC instants.
		return "TIMESTAMP WITH TIME ZONE"
	case bson.MongoTimestamp:
		return "LONG"
	default:
		if _, ok := shared.CoerceBsonM(v); ok {
			return "OBJECT(DYNAMIC)"
		}
		if arr, ok := shared.CoerceInterfaceSlice(v); ok {
			// Prefer typed arrays when we can infer a stable element type.
			if elemType, ok := arrayElementType(arr); ok {
				return fmt.Sprintf("ARRAY(%s)", elemType)
			}
			// Fallback: store as JSON in TEXT columns so it can be rehydrated on reads.
			return "TEXT"
		}
		return "OBJECT(DYNAMIC)"
	}
}

// arrayElementType is a helper used by the adapter.
func arrayElementType(arr []interface{}) (string, bool) {
	if len(arr) == 0 {
		return "", false
	}

	seenNonNil := false

	// Numeric widening state.
	hasFloat := false
	hasDouble := false
	intRank := 0 // 0 none, 1 BYTE, 2 SHORT, 3 INTEGER, 4 LONG

	elemKind := "" // "numeric" | "text" | "bool" | "time" | "object"

	setKind := func(k string) bool {
		if elemKind == "" {
			elemKind = k
			return true
		}
		return elemKind == k
	}

	for _, el := range arr {
		if el == nil {
			continue
		}
		seenNonNil = true

		// Nested arrays: keep permissive fallback semantics.
		if _, ok := shared.CoerceInterfaceSlice(el); ok {
			return "", false
		}

		switch t := el.(type) {
		case bool:
			if !setKind("bool") {
				return "", false
			}
		case time.Time:
			if !setKind("time") {
				return "", false
			}
		case string, bson.Symbol:
			if !setKind("text") {
				return "", false
			}
		case bson.ObjectId:
			// Represent as hex strings.
			if !setKind("text") {
				return "", false
			}
		case bson.M, bson.D, map[string]interface{}:
			if !setKind("object") {
				return "", false
			}
		case int8, uint8:
			if !setKind("numeric") {
				return "", false
			}
			if intRank < 1 {
				intRank = 1
			}
		case int16, uint16:
			if !setKind("numeric") {
				return "", false
			}
			if intRank < 2 {
				intRank = 2
			}
		case int32, uint32:
			if !setKind("numeric") {
				return "", false
			}
			if intRank < 3 {
				intRank = 3
			}
		case int, int64, uint64:
			if !setKind("numeric") {
				return "", false
			}
			if intRank < 4 {
				intRank = 4
			}
		case float32:
			if !setKind("numeric") {
				return "", false
			}
			if math.Trunc(float64(t)) != float64(t) {
				hasFloat = true
			} else if intRank < 4 {
				// Integral float32: treat as integer (but widen to LONG for safety).
				intRank = 4
			}
		case float64:
			if !setKind("numeric") {
				return "", false
			}
			if math.Trunc(t) != t {
				hasDouble = true
			} else if intRank < 4 {
				// Integral float64: treat as integer (but widen to LONG for safety).
				intRank = 4
			}
		default:
			// Unknown element type: keep permissive fallback semantics.
			return "", false
		}
	}

	if !seenNonNil {
		return "", false
	}

	switch elemKind {
	case "bool":
		return "BOOLEAN", true
	case "time":
		return "TIMESTAMP WITH TIME ZONE", true
	case "text":
		return "TEXT", true
	case "object":
		return "OBJECT(DYNAMIC)", true
	case "numeric":
		if hasDouble {
			return "DOUBLE PRECISION", true
		}
		if hasFloat {
			return "FLOAT", true
		}
		switch intRank {
		case 1:
			return "BYTE", true
		case 2:
			return "SHORT", true
		case 3:
			return "INTEGER", true
		default:
			return "LONG", true
		}
	default:
		return "", false
	}
}

// coerceFloat64Slice is a helper used by the adapter.
func coerceFloat64Slice(arr []interface{}) ([]float64, bool) {
	out := make([]float64, 0, len(arr))
	for _, el := range arr {
		if el == nil {
			return nil, false
		}
		f, ok := mpipeline.ToFloat64Match(el)
		if !ok {
			return nil, false
		}
		out = append(out, f)
	}
	return out, true
}

// FloatVectorLiteral is a helper used by the adapter.
func FloatVectorLiteral(v interface{}) (string, int, bool) {
	arr, ok := shared.CoerceInterfaceSlice(v)
	if !ok {
		return "", 0, false
	}
	floats, ok := coerceFloat64Slice(arr)
	if !ok {
		return "", 0, false
	}
	parts := make([]string, 0, len(floats))
	for _, f := range floats {
		parts = append(parts, strconv.FormatFloat(f, 'f', -1, 64))
	}
	return "[" + strings.Join(parts, ", ") + "]", len(floats), true
}
