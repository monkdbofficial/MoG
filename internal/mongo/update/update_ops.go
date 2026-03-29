package update

import (
	"strings"

	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/pipeline"
)

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
		delta, ok := pipeline.ToFloat64Match(rawDelta)
		if !ok {
			warnings = append(warnings, Warning{Op: "$inc", Path: path, Message: "delta is not numeric"})
			continue
		}

		cur := pipeline.GetPathValue(doc, path)
		var curNum float64
		if cur != nil {
			if f, ok := pipeline.ToFloat64Match(cur); ok {
				curNum = f
			} else {
				// Safe behavior: treat non-numeric as 0.
				warnings = append(warnings, Warning{Op: "$inc", Path: path, Message: "target is not numeric; treating as 0"})
			}
		}
		pipeline.SetPathValue(doc, path, curNum+delta)
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
