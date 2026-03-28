package mongo

import (
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/mgo.v2/bson"
)

type UpdateWarning struct {
	Op      string
	Path    string
	Message string
}

// ApplyUpdate applies a MongoDB-style update document to an input document and returns the updated copy.
// It never mutates the input doc (copy-update strategy for safety).
func ApplyUpdate(doc bson.M, update bson.M) (bson.M, []UpdateWarning) {
	out := deepCloneDoc(doc)
	var warnings []UpdateWarning

	// Replacement document (no operators): treat as full replace but keep _id stable.
	hasOp := false
	for k := range update {
		if strings.HasPrefix(k, "$") {
			hasOp = true
			break
		}
	}
	if !hasOp {
		repl := deepCloneDoc(update)
		// Preserve existing _id if replacement doesn't set it (or tries to change it).
		if oldID, ok := out["_id"]; ok {
			if newID, has := repl["_id"]; !has || fmt.Sprint(newID) != fmt.Sprint(oldID) {
				repl["_id"] = oldID
			}
		}
		return repl, warnings
	}

	for op, raw := range update {
		if !strings.HasPrefix(op, "$") {
			continue
		}
		spec, ok := coerceBsonM(raw)
		if !ok {
			warnings = append(warnings, UpdateWarning{Op: op, Message: "operator spec must be an object"})
			continue
		}
		switch op {
		case "$set":
			warnings = append(warnings, applySet(out, spec)...)
		case "$unset":
			warnings = append(warnings, applyUnset(out, spec)...)
		case "$inc":
			warnings = append(warnings, applyInc(out, spec)...)
		case "$push":
			warnings = append(warnings, applyPush(out, spec)...)
		case "$addToSet":
			warnings = append(warnings, applyAddToSet(out, spec)...)
		default:
			warnings = append(warnings, UpdateWarning{Op: op, Message: "unsupported update operator"})
		}
	}
	return out, warnings
}

// buildUpsertBaseDoc creates a base document for upserts using simple equality terms from the filter.
// Operator filters (e.g. {"age":{"$gte":1}}) are ignored.
func buildUpsertBaseDoc(filter bson.M) bson.M {
	doc := bson.M{}
	for path, v := range filter {
		if m, ok := coerceBsonM(v); ok && docHasOperatorKeys(m) {
			continue
		}
		setPathValue(doc, path, deepClone(v))
	}
	return doc
}

func applySet(doc bson.M, spec bson.M) []UpdateWarning {
	var warnings []UpdateWarning
	for path, v := range spec {
		if path == "_id" || strings.HasPrefix(path, "_id.") {
			warnings = append(warnings, UpdateWarning{Op: "$set", Path: path, Message: "cannot update _id"})
			continue
		}
		setPathValue(doc, path, deepClone(v))
	}
	return warnings
}

func applyUnset(doc bson.M, spec bson.M) []UpdateWarning {
	var warnings []UpdateWarning
	for path := range spec {
		if path == "_id" || strings.HasPrefix(path, "_id.") {
			warnings = append(warnings, UpdateWarning{Op: "$unset", Path: path, Message: "cannot update _id"})
			continue
		}
		unsetPathValue(doc, path)
	}
	return warnings
}

func applyInc(doc bson.M, spec bson.M) []UpdateWarning {
	var warnings []UpdateWarning
	for path, rawDelta := range spec {
		if path == "_id" || strings.HasPrefix(path, "_id.") {
			warnings = append(warnings, UpdateWarning{Op: "$inc", Path: path, Message: "cannot update _id"})
			continue
		}
		delta, ok := toFloat64Match(rawDelta)
		if !ok {
			warnings = append(warnings, UpdateWarning{Op: "$inc", Path: path, Message: "delta is not numeric"})
			continue
		}

		cur := getPathValue(doc, path)
		var curNum float64
		if cur != nil {
			if f, ok := toFloat64Match(cur); ok {
				curNum = f
			} else {
				// Safe behavior: treat non-numeric as 0.
				warnings = append(warnings, UpdateWarning{Op: "$inc", Path: path, Message: "target is not numeric; treating as 0"})
			}
		}
		setPathValue(doc, path, curNum+delta)
	}
	return warnings
}

func applyPush(doc bson.M, spec bson.M) []UpdateWarning {
	var warnings []UpdateWarning
	for path, v := range spec {
		if path == "_id" || strings.HasPrefix(path, "_id.") {
			warnings = append(warnings, UpdateWarning{Op: "$push", Path: path, Message: "cannot update _id"})
			continue
		}
		cur := getPathValue(doc, path)
		switch {
		case cur == nil:
			setPathValue(doc, path, []interface{}{deepClone(v)})
		case isSliceLike(cur):
			arr, _ := coerceInterfaceSlice(cur)
			arr = append(arr, deepClone(v))
			setPathValue(doc, path, arr)
		default:
			// Safe behavior: convert scalar/object to array by wrapping.
			warnings = append(warnings, UpdateWarning{Op: "$push", Path: path, Message: "target is not array; converting to array"})
			setPathValue(doc, path, []interface{}{deepClone(cur), deepClone(v)})
		}
	}
	return warnings
}

func applyAddToSet(doc bson.M, spec bson.M) []UpdateWarning {
	var warnings []UpdateWarning
	for path, v := range spec {
		if path == "_id" || strings.HasPrefix(path, "_id.") {
			warnings = append(warnings, UpdateWarning{Op: "$addToSet", Path: path, Message: "cannot update _id"})
			continue
		}
		cur := getPathValue(doc, path)
		if cur == nil {
			setPathValue(doc, path, []interface{}{deepClone(v)})
			continue
		}
		if !isSliceLike(cur) {
			warnings = append(warnings, UpdateWarning{Op: "$addToSet", Path: path, Message: "target is not array; converting to array"})
			cur = []interface{}{cur}
		}
		arr, _ := coerceInterfaceSlice(cur)
		if !arrayContains(arr, v) {
			arr = append(arr, deepClone(v))
		}
		setPathValue(doc, path, arr)
	}
	return warnings
}

func unsetPathValue(doc bson.M, path string) {
	if path == "" {
		return
	}
	parts := strings.Split(path, ".")
	if len(parts) == 1 {
		delete(doc, parts[0])
		return
	}
	cur := interface{}(doc)
	for _, p := range parts[:len(parts)-1] {
		m, ok := coerceBsonM(cur)
		if !ok {
			return
		}
		child, exists := m[p]
		if !exists || child == nil {
			return
		}
		cur = child
	}
	m, ok := coerceBsonM(cur)
	if !ok {
		return
	}
	delete(m, parts[len(parts)-1])
}

func isSliceLike(v interface{}) bool {
	if v == nil {
		return false
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return false
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return false
	}
	// Exclude []byte.
	if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
		return false
	}
	return true
}

func arrayContains(arr []interface{}, v interface{}) bool {
	for _, el := range arr {
		if reflect.DeepEqual(el, v) || fmt.Sprint(el) == fmt.Sprint(v) {
			return true
		}
	}
	return false
}
