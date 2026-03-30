package update

import (
	"math"
	"strings"

	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/pipeline"
)

func isFloatLike(v interface{}) bool {
	switch v.(type) {
	case float32, float64:
		return true
	default:
		return false
	}
}

func int64FromNumber(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		if x > math.MaxInt64 {
			return 0, false
		}
		return int64(x), true
	default:
		return 0, false
	}
}

func castInt64ToLike(src interface{}, v int64) (interface{}, bool) {
	switch src.(type) {
	case int8:
		if v < math.MinInt8 || v > math.MaxInt8 {
			return nil, false
		}
		return int8(v), true
	case int16:
		if v < math.MinInt16 || v > math.MaxInt16 {
			return nil, false
		}
		return int16(v), true
	case int32:
		if v < math.MinInt32 || v > math.MaxInt32 {
			return nil, false
		}
		return int32(v), true
	case int64:
		return v, true
	case int:
		// Assume 64-bit for typical builds; still validate conservatively.
		if strconvIntSize() == 32 {
			if v < math.MinInt32 || v > math.MaxInt32 {
				return nil, false
			}
		}
		return int(v), true
	case uint8:
		if v < 0 || v > math.MaxUint8 {
			return nil, false
		}
		return uint8(v), true
	case uint16:
		if v < 0 || v > math.MaxUint16 {
			return nil, false
		}
		return uint16(v), true
	case uint32:
		if v < 0 || v > math.MaxUint32 {
			return nil, false
		}
		return uint32(v), true
	case uint64:
		if v < 0 {
			return nil, false
		}
		return uint64(v), true
	default:
		return nil, false
	}
}

func strconvIntSize() int {
	// Go doesn't expose int size directly; infer from constant.
	// This is evaluated at compile time.
	if ^uint(0)>>32 == 0 {
		return 32
	}
	return 64
}

func applySet(doc bson.M, spec bson.M) []Warning {
	var warnings []Warning
	for path, v := range spec {
		if path == "_id" || strings.HasPrefix(path, "_id.") {
			warnings = append(warnings, Warning{Op: "$set", Path: path, Message: "cannot update _id"})
			continue
		}
		pipeline.SetPathValue(doc, path, pipeline.DeepClone(v))
	}
	return warnings
}

func applyUnset(doc bson.M, spec bson.M) []Warning {
	var warnings []Warning
	for path := range spec {
		if path == "_id" || strings.HasPrefix(path, "_id.") {
			warnings = append(warnings, Warning{Op: "$unset", Path: path, Message: "cannot update _id"})
			continue
		}
		pipeline.UnsetPathValue(doc, path)
	}
	return warnings
}

func applyInc(doc bson.M, spec bson.M) []Warning {
	var warnings []Warning
	for path, rawDelta := range spec {
		if path == "_id" || strings.HasPrefix(path, "_id.") {
			warnings = append(warnings, Warning{Op: "$inc", Path: path, Message: "cannot update _id"})
			continue
		}
		deltaF, ok := pipeline.ToFloat64Match(rawDelta)
		if !ok {
			warnings = append(warnings, Warning{Op: "$inc", Path: path, Message: "delta is not numeric"})
			continue
		}

		cur := pipeline.GetPathValue(doc, path)

		// Mongo-like type behavior (and crucial for SQL typed columns):
		// - If either operand is a float, compute a float result.
		// - If both are integers (or field is missing), preserve an integer result (prefer the
		//   existing field's concrete integer type when possible).
		if isFloatLike(rawDelta) || isFloatLike(cur) {
			var curNum float64
			if cur != nil {
				if f, ok := pipeline.ToFloat64Match(cur); ok {
					curNum = f
				} else {
					warnings = append(warnings, Warning{Op: "$inc", Path: path, Message: "target is not numeric; treating as 0"})
				}
			}
			pipeline.SetPathValue(doc, path, curNum+deltaF)
			continue
		}

		deltaI, okI := int64FromNumber(rawDelta)
		if !okI {
			// Non-float numeric types are treated as float in ToFloat64Match; if we can't
			// classify it as an integer here, fall back to float behavior.
			var curNum float64
			if cur != nil {
				if f, ok := pipeline.ToFloat64Match(cur); ok {
					curNum = f
				}
			}
			pipeline.SetPathValue(doc, path, curNum+deltaF)
			continue
		}

		var curI int64
		var like interface{} = rawDelta
		if cur != nil {
			like = cur
			if ci, ok := int64FromNumber(cur); ok {
				curI = ci
			} else if f, ok := pipeline.ToFloat64Match(cur); ok {
				// cur is numeric but not an integer kind (rare here); keep float behavior.
				pipeline.SetPathValue(doc, path, f+deltaF)
				continue
			} else {
				warnings = append(warnings, Warning{Op: "$inc", Path: path, Message: "target is not numeric; treating as 0"})
			}
		}

		sum := curI + deltaI
		if cast, ok := castInt64ToLike(like, sum); ok {
			pipeline.SetPathValue(doc, path, cast)
		} else {
			// Best-effort: widen to int64.
			warnings = append(warnings, Warning{Op: "$inc", Path: path, Message: "integer overflow; widening to int64"})
			pipeline.SetPathValue(doc, path, sum)
		}
	}
	return warnings
}

func applyPush(doc bson.M, spec bson.M) []Warning {
	var warnings []Warning
	for path, v := range spec {
		if path == "_id" || strings.HasPrefix(path, "_id.") {
			warnings = append(warnings, Warning{Op: "$push", Path: path, Message: "cannot update _id"})
			continue
		}
		cur := pipeline.GetPathValue(doc, path)
		switch {
		case cur == nil:
			pipeline.SetPathValue(doc, path, []interface{}{pipeline.DeepClone(v)})
		case isSliceLike(cur):
			arr, _ := pipeline.CoerceInterfaceSlice(cur)
			arr = append(arr, pipeline.DeepClone(v))
			pipeline.SetPathValue(doc, path, arr)
		default:
			// Safe behavior: convert scalar/object to array by wrapping.
			warnings = append(warnings, Warning{Op: "$push", Path: path, Message: "target is not array; converting to array"})
			pipeline.SetPathValue(doc, path, []interface{}{pipeline.DeepClone(cur), pipeline.DeepClone(v)})
		}
	}
	return warnings
}

func applyAddToSet(doc bson.M, spec bson.M) []Warning {
	var warnings []Warning
	for path, v := range spec {
		if path == "_id" || strings.HasPrefix(path, "_id.") {
			warnings = append(warnings, Warning{Op: "$addToSet", Path: path, Message: "cannot update _id"})
			continue
		}
		cur := pipeline.GetPathValue(doc, path)
		if cur == nil {
			pipeline.SetPathValue(doc, path, []interface{}{pipeline.DeepClone(v)})
			continue
		}
		if !isSliceLike(cur) {
			warnings = append(warnings, Warning{Op: "$addToSet", Path: path, Message: "target is not array; converting to array"})
			cur = []interface{}{cur}
		}
		arr, _ := pipeline.CoerceInterfaceSlice(cur)
		if !arrayContains(arr, v) {
			arr = append(arr, pipeline.DeepClone(v))
		}
		pipeline.SetPathValue(doc, path, arr)
	}
	return warnings
}
