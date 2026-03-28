package mongo

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"
)

func sqlTypeForValue(v interface{}) string {
	if v == nil {
		return ""
	}
	switch v.(type) {
	case bool:
		return "BOOLEAN"
	case string, bson.Symbol:
		return "TEXT"
	case int, int32, int64, float32, float64:
		return "DOUBLE PRECISION"
	case time.Time:
		// MonkDB/Crate-style backends are not always PostgreSQL-compatible here.
		// Prefer a broadly-supported type over `TIMESTAMPTZ` to avoid "illegal syntax".
		return "TIMESTAMP"
	default:
		if _, ok := coerceBsonM(v); ok {
			return "OBJECT(DYNAMIC)"
		}
		if arr, ok := coerceInterfaceSlice(v); ok {
			// Heuristic: numeric arrays are treated as vectors.
			if floats, ok := coerceFloat64Slice(arr); ok && len(floats) > 0 && len(floats) <= 2048 {
				return fmt.Sprintf("FLOAT_VECTOR(%d)", len(floats))
			}
			// Store arrays as JSON in TEXT columns so they can be rehydrated on reads
			// (normalizeRowValue parses JSON arrays/objects from TEXT).
			return "TEXT"
		}
		return "OBJECT(DYNAMIC)"
	}
}

func coerceFloat64Slice(arr []interface{}) ([]float64, bool) {
	out := make([]float64, 0, len(arr))
	for _, el := range arr {
		if el == nil {
			return nil, false
		}
		f, ok := toFloat64Match(el)
		if !ok {
			return nil, false
		}
		out = append(out, f)
	}
	return out, true
}

func floatVectorLiteral(v interface{}) (string, int, bool) {
	arr, ok := coerceInterfaceSlice(v)
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
