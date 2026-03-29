package pipeline

import (
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/mgo.v2/bson"
)

// $match/$project/$addFields expression plumbing and helpers.
func applyMatch(docs []bson.M, filter bson.M) []bson.M {
	var out []bson.M
	for _, d := range docs {
		if matchDoc(d, filter) {
			out = append(out, d)
		}
	}
	return out
}

func matchDoc(doc bson.M, filter bson.M) bool {
	for k, v := range filter {
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
	include := map[string]bool{}
	computed := map[string]bson.M{}
	for k, v := range proj {
		switch vv := v.(type) {
		case int:
			if vv == 1 {
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
		case bson.M:
			computed[k] = vv
		default:
			// ignore unsupported projections for now
		}
	}

	var out []bson.M
	for _, d := range docs {
		nd := bson.M{}
		for k := range include {
			if v, ok := d[k]; ok {
				nd[k] = v
			}
		}
		for k, expr := range computed {
			val, err := evalComputedWithOpts(d, expr, evalOpts{sizeNonArrayZero: true, vars: map[string]interface{}{"ROOT": d, "CURRENT": d}})
			if err != nil {
				return nil, err
			}
			nd[k] = val
		}
		out = append(out, nd)
	}
	return out, nil
}

func applyAddFields(docs []bson.M, spec bson.M) ([]bson.M, error) {
	out := make([]bson.M, 0, len(docs))
	for _, d := range docs {
		// Don't mutate original documents.
		nd := bson.M{}
		for k, v := range d {
			nd[k] = v
		}

		for field, rawExpr := range spec {
			val, err := evalAddFieldsValue(d, rawExpr)
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
	return evalComputedWithOpts(doc, expr, evalOpts{sizeNonArrayZero: false, vars: map[string]interface{}{"ROOT": doc, "CURRENT": doc}})
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
	if s, ok := v.(string); ok && strings.HasPrefix(s, "$$") {
		key := strings.TrimPrefix(s, "$$")
		varName := key
		rest := ""
		if i := strings.IndexByte(key, '.'); i >= 0 {
			varName = key[:i]
			rest = key[i+1:]
		}
		switch varName {
		case "ROOT", "CURRENT":
			if rest == "" {
				return doc, nil
			}
			return getPathValue(doc, rest), nil
		default:
			return nil, nil
		}
	}
	if s, ok := v.(string); ok && len(s) > 1 && s[0] == '$' {
		f := s[1:]
		return getPathValue(doc, f), nil
	}
	return v, nil
}
