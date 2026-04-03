package pipeline

import (
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/mgo.v2/bson"
)

// $match/$project/$addFields expression plumbing and helpers.
func applyMatch(docs []bson.M, filter bson.M) []bson.M {
	return applyMatchWithVars(docs, filter, nil)
}

func applyMatchWithVars(docs []bson.M, filter bson.M, vars map[string]interface{}) []bson.M {
	var out []bson.M
	for _, d := range docs {
		if matchDocWithVars(d, filter, vars) {
			out = append(out, d)
		}
	}
	return out
}

func matchDoc(doc bson.M, filter bson.M) bool { return matchDocWithVars(doc, filter, nil) }

func matchDocWithVars(doc bson.M, filter bson.M, vars map[string]interface{}) bool {
	// Support $expr at the top level of a $match document.
	//
	// Note: this matcher is intentionally limited; it focuses on the subset of
	// MongoDB semantics used by MoG's in-memory pipeline evaluator.
	if rawExpr, ok := filter["$expr"]; ok {
		opts := evalOpts{sizeNonArrayZero: false, vars: stageVars(doc, vars)}
		v, err := evalComputedWithOpts(doc, rawExpr, opts)
		if err != nil || !isTruthy(v) {
			return false
		}
	}

	for k, v := range filter {
		if k == "$expr" {
			continue
		}
		fieldVal := getPathValue(doc, k)
		// Treat missing/null as None => condition fails.
		if fieldVal == nil {
			return false
		}

		if cond, ok := coerceBsonM(v); ok && docHasOperatorKeys(cond) {
			if !matchOps(fieldVal, cond) {
				return false
			}
			continue
		}

		if !matchEquals(fieldVal, v) {
			return false
		}
	}
	return true
}

func docHasOperatorKeys(m bson.M) bool {
	for k := range m {
		if strings.HasPrefix(k, "$") {
			return true
		}
	}
	return false
}

func matchOps(fieldVal interface{}, cond bson.M) bool {
	for op, opVal := range cond {
		switch op {
		case "$gt":
			if !cmpNumberMatch(fieldVal, opVal, func(a, b float64) bool { return a > b }) {
				return false
			}
		case "$lt":
			if !cmpNumberMatch(fieldVal, opVal, func(a, b float64) bool { return a < b }) {
				return false
			}
		case "$gte":
			if !cmpNumberMatch(fieldVal, opVal, func(a, b float64) bool { return a >= b }) {
				return false
			}
		case "$lte":
			if !cmpNumberMatch(fieldVal, opVal, func(a, b float64) bool { return a <= b }) {
				return false
			}
		case "$ne":
			if matchEquals(fieldVal, opVal) {
				return false
			}
		case "$in":
			list, ok := coerceInterfaceSlice(opVal)
			if !ok {
				return false
			}

			// Scalar field: match if scalar is in list.
			if arr, ok := coerceInterfaceSlice(fieldVal); ok {
				// Array field: match if any element is in list.
				if !anyIn(arr, list) {
					return false
				}
			} else {
				if !scalarIn(fieldVal, list) {
					return false
				}
			}
		default:
			return false
		}
	}
	return true
}

func matchEquals(a, b interface{}) bool {
	// Per spec: missing fields are treated as None and fail; matchDoc already filtered nil fieldVal.
	if a == nil || b == nil {
		return false
	}
	if af, aok := toFloat64Match(a); aok {
		if bf, bok := toFloat64Match(b); bok {
			return af == bf
		}
	}
	return reflect.DeepEqual(a, b) || fmt.Sprint(a) == fmt.Sprint(b)
}

func cmpNumberMatch(a, b interface{}, fn func(float64, float64) bool) bool {
	if a == nil || b == nil {
		return false
	}
	af, ok := toFloat64Match(a)
	if !ok {
		return false
	}
	bf, ok := toFloat64Match(b)
	if !ok {
		return false
	}
	return fn(af, bf)
}

func toFloat64Match(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case float32:
		return float64(x), true
	case float64:
		return x, true
	default:
		return 0, false
	}
}

func coerceInterfaceSlice(v interface{}) ([]interface{}, bool) {
	switch t := v.(type) {
	case []interface{}:
		return t, true
	case []string:
		out := make([]interface{}, 0, len(t))
		for _, s := range t {
			out = append(out, s)
		}
		return out, true
	case []bson.M:
		out := make([]interface{}, 0, len(t))
		for _, m := range t {
			out = append(out, m)
		}
		return out, true
	default:
		rv := reflect.ValueOf(v)
		if !rv.IsValid() {
			return nil, false
		}
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			return nil, false
		}
		// Don't treat raw bytes as an array for $unwind/$in semantics.
		if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
			return nil, false
		}
		out := make([]interface{}, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out = append(out, rv.Index(i).Interface())
		}
		return out, true
	}
}

func scalarIn(v interface{}, list []interface{}) bool {
	for _, item := range list {
		if fmt.Sprint(v) == fmt.Sprint(item) {
			return true
		}
	}
	return false
}

func anyIn(arr []interface{}, list []interface{}) bool {
	for _, el := range arr {
		if scalarIn(el, list) {
			return true
		}
	}
	return false
}

func applyProject(docs []bson.M, proj bson.M) ([]bson.M, error) {
	return applyProjectWithVars(docs, proj, nil)
}

func applyProjectWithVars(docs []bson.M, proj bson.M, vars map[string]interface{}) ([]bson.M, error) {
	include := map[string]bool{}
	computed := map[string]interface{}{}
	for k, v := range proj {
		switch vv := v.(type) {
		case int:
			if vv == 1 {
				include[k] = true
			}
		case bool:
			if vv {
				include[k] = true
			}
		case int32:
			if vv == 1 {
				include[k] = true
			}
		case int64:
			if vv == 1 {
				include[k] = true
			}
		case float64:
			if vv == 1 {
				include[k] = true
			}
		default:
			// Treat anything else as a computed expression (e.g. "$field", "$$var",
			// constants, or operator documents).
			computed[k] = vv
		}
	}

	var out []bson.M
	for _, d := range docs {
		nd := bson.M{}
		for k := range include {
			if strings.Contains(k, ".") {
				if projected, ok := projectIncludeValue(d, strings.Split(k, ".")); ok {
					mergeProjectedDoc(nd, projected)
				}
				continue
			}
			if v, ok := d[k]; ok {
				nd[k] = deepClone(v)
			}
		}
		for k, expr := range computed {
			val, err := evalComputedWithOpts(d, expr, evalOpts{sizeNonArrayZero: true, vars: stageVars(d, vars)})
			if err != nil {
				return nil, err
			}
			nd[k] = val
		}
		out = append(out, nd)
	}
	return out, nil
}

func projectIncludeValue(root interface{}, parts []string) (interface{}, bool) {
	if len(parts) == 0 {
		return deepClone(root), true
	}
	if arr, ok := coerceInterfaceSlice(root); ok {
		out := make([]interface{}, 0, len(arr))
		for _, item := range arr {
			projected, ok := projectIncludeValue(item, parts)
			if ok {
				out = append(out, projected)
			}
		}
		return out, true
	}
	doc, ok := coerceBsonM(root)
	if !ok {
		return nil, false
	}
	next, ok := doc[parts[0]]
	if !ok {
		return nil, false
	}
	if len(parts) == 1 {
		return bson.M{parts[0]: deepClone(next)}, true
	}
	projected, ok := projectIncludeValue(next, parts[1:])
	if !ok {
		return nil, false
	}
	return bson.M{parts[0]: projected}, true
}

func mergeProjectedDoc(dst bson.M, projected interface{}) {
	doc, ok := coerceBsonM(projected)
	if !ok {
		return
	}
	for k, v := range doc {
		if existing, exists := dst[k]; exists {
			dst[k] = mergeProjectedValue(existing, v)
			continue
		}
		dst[k] = deepClone(v)
	}
}

func mergeProjectedValue(existing, projected interface{}) interface{} {
	existingDoc, existingIsDoc := coerceBsonM(existing)
	projectedDoc, projectedIsDoc := coerceBsonM(projected)
	if existingIsDoc && projectedIsDoc {
		out := deepCloneDoc(existingDoc)
		for k, v := range projectedDoc {
			if cur, ok := out[k]; ok {
				out[k] = mergeProjectedValue(cur, v)
				continue
			}
			out[k] = deepClone(v)
		}
		return out
	}
	existingArr, existingIsArr := coerceInterfaceSlice(existing)
	projectedArr, projectedIsArr := coerceInterfaceSlice(projected)
	if existingIsArr && projectedIsArr {
		out := make([]interface{}, 0, max(len(existingArr), len(projectedArr)))
		for idx := 0; idx < len(existingArr) || idx < len(projectedArr); idx++ {
			switch {
			case idx < len(existingArr) && idx < len(projectedArr):
				out = append(out, mergeProjectedValue(existingArr[idx], projectedArr[idx]))
			case idx < len(existingArr):
				out = append(out, deepClone(existingArr[idx]))
			default:
				out = append(out, deepClone(projectedArr[idx]))
			}
		}
		return out
	}
	return deepClone(projected)
}

func applyAddFields(docs []bson.M, spec bson.M) ([]bson.M, error) {
	return applyAddFieldsWithVars(docs, spec, nil)
}

func applyAddFieldsWithVars(docs []bson.M, spec bson.M, vars map[string]interface{}) ([]bson.M, error) {
	out := make([]bson.M, 0, len(docs))
	for _, d := range docs {
		// Don't mutate original documents.
		nd := bson.M{}
		for k, v := range d {
			nd[k] = v
		}

		for field, rawExpr := range spec {
			val, err := evalAddFieldsValueWithVars(d, rawExpr, vars)
			if err != nil {
				return nil, err
			}

			// Support dot paths for nested fields.
			if strings.Contains(field, ".") {
				nd = cloneDocForSetPath(nd, field)
				setPathValue(nd, field, val)
				continue
			}
			nd[field] = val
		}
		out = append(out, nd)
	}
	return out, nil
}

func evalAddFieldsValue(doc bson.M, expr interface{}) (interface{}, error) {
	return evalAddFieldsValueWithVars(doc, expr, nil)
}

func evalAddFieldsValueWithVars(doc bson.M, expr interface{}, vars map[string]interface{}) (interface{}, error) {
	return evalComputedWithOpts(doc, expr, evalOpts{sizeNonArrayZero: false, vars: stageVars(doc, vars)})
}

func isArrayValue(v interface{}) bool {
	if v == nil {
		return false
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return false
	}
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		// Don't treat raw bytes as an array.
		if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
			return false
		}
		return true
	}
	return false
}

func isTruthy(v interface{}) bool {
	if v == nil {
		return false
	}
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return t != ""
	case int:
		return t != 0
	case int8:
		return t != 0
	case int16:
		return t != 0
	case int32:
		return t != 0
	case int64:
		return t != 0
	case uint:
		return t != 0
	case uint8:
		return t != 0
	case uint16:
		return t != 0
	case uint32:
		return t != 0
	case uint64:
		return t != 0
	case float32:
		return t != 0
	case float64:
		return t != 0
	default:
		return true
	}
}

func evalValue(doc bson.M, v interface{}) (interface{}, error) {
	return evalValueWithVars(doc, v, nil)
}

func evalValueWithVars(doc bson.M, v interface{}, vars map[string]interface{}) (interface{}, error) {
	// Historically evalValue supported only "$field" and $$ROOT/$$CURRENT; switch to
	// the shared expression evaluator so $lookup "let" variables behave like MongoDB.
	return evalComputedWithOpts(doc, v, evalOpts{sizeNonArrayZero: false, vars: stageVars(doc, vars)})
}
