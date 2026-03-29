package pipeline

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"
)

// Expression evaluation for aggregation stages.
type evalOpts struct {
	sizeNonArrayZero bool
	vars             map[string]interface{}
}

func evalComputedWithOpts(doc bson.M, expr interface{}, opts evalOpts) (interface{}, error) {
	// Field reference: "$field.subfield"
	if s, ok := expr.(string); ok {
		// Variables: "$$this", "$$value", "$$x.y"
		if strings.HasPrefix(s, "$$") {
			key := strings.TrimPrefix(s, "$$")
			varName := key
			rest := ""
			if i := strings.IndexByte(key, '.'); i >= 0 {
				varName = key[:i]
				rest = key[i+1:]
			}
			if opts.vars == nil {
				return nil, nil
			}
			v, ok := opts.vars[varName]
			if !ok {
				return nil, nil
			}
			if rest == "" {
				return v, nil
			}
			return getPathValueAny(v, rest), nil
		}
		if len(s) > 1 && s[0] == '$' {
			return getPathValue(doc, strings.TrimPrefix(s, "$")), nil
		}
		return s, nil
	}

	// Operator expression.
	if m, ok := coerceBsonM(expr); ok {
		if len(m) == 1 && docHasOperatorKeys(m) {
			for op, arg := range m {
				switch op {
				// Misc
				case "$literal":
					return arg, nil
				case "$rand":
					return rand.Float64(), nil
				case "$meta":
					if s, ok := arg.(string); ok {
						switch s {
						case "vectorSearchScore":
							if doc == nil {
								return nil, nil
							}
							return doc[mogVectorSearchScoreKey], nil
						default:
							return nil, fmt.Errorf("unsupported $meta: %s", s)
						}
					}
					return nil, fmt.Errorf("$meta must be a string")

				// Boolean
				case "$and":
					args, err := evalArrayArgs(doc, arg, 1, opts)
					if err != nil {
						return nil, err
					}
					for _, a := range args {
						if !isTruthy(a) {
							return false, nil
						}
					}
					return true, nil
				case "$or":
					args, err := evalArrayArgs(doc, arg, 1, opts)
					if err != nil {
						return nil, err
					}
					for _, a := range args {
						if isTruthy(a) {
							return true, nil
						}
					}
					return false, nil
				case "$not":
					// Mongo uses array form, but accept scalar too.
					if arr, ok := coerceInterfaceSlice(arg); ok {
						if len(arr) != 1 {
							return nil, fmt.Errorf("$not must be a 1-arg array")
						}
						v, err := evalComputedWithOpts(doc, arr[0], opts)
						if err != nil {
							return nil, err
						}
						return !isTruthy(v), nil
					}
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					return !isTruthy(v), nil

				// Conditional
				case "$cond":
					// Form 1: { $cond: [ <if>, <then>, <else> ] }
					if arr, ok := coerceInterfaceSlice(arg); ok {
						if len(arr) != 3 {
							return nil, fmt.Errorf("$cond must be a 3-arg array")
						}
						condVal, err := evalComputedWithOpts(doc, arr[0], opts)
						if err != nil {
							return nil, err
						}
						if isTruthy(condVal) {
							return evalComputedWithOpts(doc, arr[1], opts)
						}
						return evalComputedWithOpts(doc, arr[2], opts)
					}
					// Form 2: { $cond: { if: <expr>, then: <expr>, else: <expr> } }
					if spec, ok := coerceBsonM(arg); ok {
						ifExpr, hasIf := spec["if"]
						thenExpr, hasThen := spec["then"]
						elseExpr, hasElse := spec["else"]
						if !hasIf || !hasThen || !hasElse {
							return nil, fmt.Errorf("$cond object form requires if/then/else")
						}
						condVal, err := evalComputedWithOpts(doc, ifExpr, opts)
						if err != nil {
							return nil, err
						}
						if isTruthy(condVal) {
							return evalComputedWithOpts(doc, thenExpr, opts)
						}
						return evalComputedWithOpts(doc, elseExpr, opts)
					}
					return nil, fmt.Errorf("$cond must be an array or document")
				case "$ifNull":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					if args[0] == nil {
						return args[1], nil
					}
					return args[0], nil
				case "$switch":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$switch must be a document")
					}
					rawBranches, ok := spec["branches"]
					if !ok {
						return nil, fmt.Errorf("$switch requires branches")
					}
					branchesArr, ok := coerceInterfaceSlice(rawBranches)
					if !ok {
						return nil, fmt.Errorf("$switch branches must be an array")
					}
					for _, br := range branchesArr {
						brDoc, ok := coerceBsonM(br)
						if !ok {
							return nil, fmt.Errorf("$switch branch must be a document")
						}
						caseExpr, okC := brDoc["case"]
						thenExpr, okT := brDoc["then"]
						if !okC || !okT {
							return nil, fmt.Errorf("$switch branch requires case/then")
						}
						cv, err := evalComputedWithOpts(doc, caseExpr, opts)
						if err != nil {
							return nil, err
						}
						if isTruthy(cv) {
							return evalComputedWithOpts(doc, thenExpr, opts)
						}
					}
					if def, ok := spec["default"]; ok {
						return evalComputedWithOpts(doc, def, opts)
					}
					return nil, nil
				case "$let":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$let must be a document")
					}
					rawVars, okV := spec["vars"]
					inExpr, okIn := spec["in"]
					if !okV || !okIn {
						return nil, fmt.Errorf("$let requires vars/in")
					}
					varsDoc, ok := coerceBsonM(rawVars)
					if !ok {
						return nil, fmt.Errorf("$let.vars must be a document")
					}
					child := opts
					child.vars = cloneVars(opts.vars)
					for name, raw := range varsDoc {
						v, err := evalComputedWithOpts(doc, raw, opts)
						if err != nil {
							return nil, err
						}
						child.vars[name] = v
					}
					return evalComputedWithOpts(doc, inExpr, child)

				// Comparison
				case "$cmp":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					return int64(cmp3(args[0], args[1])), nil
				case "$eq":
					return evalCompareOp(doc, arg, func(c int) bool { return c == 0 }, opts)
				case "$ne":
					return evalCompareOp(doc, arg, func(c int) bool { return c != 0 }, opts)
				case "$gt":
					return evalCompareOp(doc, arg, func(c int) bool { return c > 0 }, opts)
				case "$gte":
					return evalCompareOp(doc, arg, func(c int) bool { return c >= 0 }, opts)
				case "$lt":
					return evalCompareOp(doc, arg, func(c int) bool { return c < 0 }, opts)
				case "$lte":
					return evalCompareOp(doc, arg, func(c int) bool { return c <= 0 }, opts)

				// Array
				case "$isArray":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					return isArrayValue(v), nil
				case "$size":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					if v == nil {
						if opts.sizeNonArrayZero {
							return int64(0), nil
						}
						return nil, nil
					}
					rv := reflect.ValueOf(v)
					if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
						if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
							if opts.sizeNonArrayZero {
								return int64(0), nil
							}
							return nil, nil
						}
						return int64(rv.Len()), nil
					}
					if opts.sizeNonArrayZero {
						return int64(0), nil
					}
					return nil, nil
				case "$in":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					list, ok := coerceInterfaceSlice(args[1])
					if !ok {
						return false, nil
					}
					if arr, ok := coerceInterfaceSlice(args[0]); ok {
						return anyIn(arr, list), nil
					}
					return scalarIn(args[0], list), nil
				case "$allElementsTrue":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					arr, ok := coerceInterfaceSlice(v)
					if !ok {
						return nil, nil
					}
					for _, el := range arr {
						if !isTruthy(el) {
							return false, nil
						}
					}
					return true, nil
				case "$anyElementTrue":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					arr, ok := coerceInterfaceSlice(v)
					if !ok {
						return nil, nil
					}
					for _, el := range arr {
						if isTruthy(el) {
							return true, nil
						}
					}
					return false, nil
				case "$arrayElemAt":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					arr, ok := coerceInterfaceSlice(args[0])
					if !ok {
						return nil, nil
					}
					idx64, ok := toInt64IfIntegral(args[1])
					if !ok {
						return nil, nil
					}
					idx := int(idx64)
					if idx < 0 {
						idx = len(arr) + idx
					}
					if idx < 0 || idx >= len(arr) {
						return nil, nil
					}
					return arr[idx], nil
				case "$concatArrays":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					out := make([]interface{}, 0)
					for _, a := range args {
						arr, ok := coerceInterfaceSlice(a)
						if !ok {
							return nil, nil
						}
						out = append(out, arr...)
					}
					return out, nil
				case "$first":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					arr, ok := coerceInterfaceSlice(v)
					if !ok || len(arr) == 0 {
						return nil, nil
					}
					return arr[0], nil
				case "$last":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					arr, ok := coerceInterfaceSlice(v)
					if !ok || len(arr) == 0 {
						return nil, nil
					}
					return arr[len(arr)-1], nil
				case "$slice":
					arr, ok := coerceInterfaceSlice(arg)
					if !ok || (len(arr) != 2 && len(arr) != 3) {
						return nil, fmt.Errorf("$slice must be a 2-arg or 3-arg array")
					}
					e0, err := evalComputedWithOpts(doc, arr[0], opts)
					if err != nil {
						return nil, err
					}
					list, ok := coerceInterfaceSlice(e0)
					if !ok {
						return nil, nil
					}
					if len(arr) == 2 {
						n64, ok := toInt64IfIntegralMust(evalComputedWithOpts(doc, arr[1], opts))
						if !ok {
							return nil, nil
						}
						n := int(n64)
						if n >= 0 {
							if n > len(list) {
								n = len(list)
							}
							return list[:n], nil
						}
						// negative => last n elements
						n = -n
						if n > len(list) {
							n = len(list)
						}
						return list[len(list)-n:], nil
					}
					pos64, ok := toInt64IfIntegralMust(evalComputedWithOpts(doc, arr[1], opts))
					if !ok {
						return nil, nil
					}
					n64, ok := toInt64IfIntegralMust(evalComputedWithOpts(doc, arr[2], opts))
					if !ok {
						return nil, nil
					}
					pos := int(pos64)
					n := int(n64)
					if pos < 0 {
						pos = len(list) + pos
					}
					if pos < 0 {
						pos = 0
					}
					if pos > len(list) {
						pos = len(list)
					}
					end := pos + n
					if end > len(list) {
						end = len(list)
					}
					if end < pos {
						end = pos
					}
					return list[pos:end], nil
				case "$reverseArray":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					arr, ok := coerceInterfaceSlice(v)
					if !ok {
						return nil, nil
					}
					out := make([]interface{}, len(arr))
					for i := range arr {
						out[i] = arr[len(arr)-1-i]
					}
					return out, nil
				case "$range":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					start, ok := toInt64IfIntegral(args[0])
					if !ok {
						return nil, nil
					}
					end, ok := toInt64IfIntegral(args[1])
					if !ok {
						return nil, nil
					}
					step := int64(1)
					if len(args) >= 3 {
						if s, ok := toInt64IfIntegral(args[2]); ok && s != 0 {
							step = s
						}
					}
					out := []interface{}{}
					if step > 0 {
						for i := start; i < end; i += step {
							out = append(out, i)
						}
					} else {
						for i := start; i > end; i += step {
							out = append(out, i)
						}
					}
					return out, nil
				case "$setUnion":
					args, err := evalArrayArgs(doc, arg, 1, opts)
					if err != nil {
						return nil, err
					}
					seen := map[string]struct{}{}
					out := []interface{}{}
					for _, a := range args {
						arr, ok := coerceInterfaceSlice(a)
						if !ok {
							return nil, nil
						}
						for _, el := range arr {
							k := fmt.Sprint(el)
							if _, ok := seen[k]; ok {
								continue
							}
							seen[k] = struct{}{}
							out = append(out, el)
						}
					}
					return out, nil
				case "$setIntersection":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					a0, ok := coerceInterfaceSlice(args[0])
					if !ok {
						return nil, nil
					}
					cur := map[string]interface{}{}
					for _, el := range a0 {
						cur[fmt.Sprint(el)] = el
					}
					for _, a := range args[1:] {
						arr, ok := coerceInterfaceSlice(a)
						if !ok {
							return nil, nil
						}
						next := map[string]interface{}{}
						for _, el := range arr {
							k := fmt.Sprint(el)
							if v, ok := cur[k]; ok {
								next[k] = v
							}
						}
						cur = next
					}
					out := []interface{}{}
					for _, v := range cur {
						out = append(out, v)
					}
					sort.SliceStable(out, func(i, j int) bool { return fmt.Sprint(out[i]) < fmt.Sprint(out[j]) })
					return out, nil
				case "$setDifference":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					a0, ok := coerceInterfaceSlice(args[0])
					if !ok {
						return nil, nil
					}
					a1, ok := coerceInterfaceSlice(args[1])
					if !ok {
						return nil, nil
					}
					rm := map[string]struct{}{}
					for _, el := range a1 {
						rm[fmt.Sprint(el)] = struct{}{}
					}
					out := []interface{}{}
					for _, el := range a0 {
						if _, ok := rm[fmt.Sprint(el)]; ok {
							continue
						}
						out = append(out, el)
					}
					return out, nil
				case "$setEquals":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					a0, ok := coerceInterfaceSlice(args[0])
					if !ok {
						return nil, nil
					}
					a1, ok := coerceInterfaceSlice(args[1])
					if !ok {
						return nil, nil
					}
					m0 := map[string]struct{}{}
					m1 := map[string]struct{}{}
					for _, el := range a0 {
						m0[fmt.Sprint(el)] = struct{}{}
					}
					for _, el := range a1 {
						m1[fmt.Sprint(el)] = struct{}{}
					}
					if len(m0) != len(m1) {
						return false, nil
					}
					for k := range m0 {
						if _, ok := m1[k]; !ok {
							return false, nil
						}
					}
					return true, nil
				case "$setIsSubset":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					a0, ok := coerceInterfaceSlice(args[0])
					if !ok {
						return nil, nil
					}
					a1, ok := coerceInterfaceSlice(args[1])
					if !ok {
						return nil, nil
					}
					sup := map[string]struct{}{}
					for _, el := range a1 {
						sup[fmt.Sprint(el)] = struct{}{}
					}
					for _, el := range a0 {
						if _, ok := sup[fmt.Sprint(el)]; !ok {
							return false, nil
						}
					}
					return true, nil

				case "$map":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$map must be a document")
					}
					inRaw, okI := spec["input"]
					exprRaw, okE := spec["in"]
					if !okI || !okE {
						return nil, fmt.Errorf("$map requires input/in")
					}
					as := "this"
					if rawAs, ok := spec["as"].(string); ok && rawAs != "" {
						as = rawAs
					}
					input, err := evalComputedWithOpts(doc, inRaw, opts)
					if err != nil {
						return nil, err
					}
					arr, ok := coerceInterfaceSlice(input)
					if !ok {
						return nil, nil
					}
					out := make([]interface{}, 0, len(arr))
					for i, el := range arr {
						child := opts
						child.vars = cloneVars(opts.vars)
						child.vars["this"] = el
						child.vars[as] = el
						child.vars["index"] = int64(i)
						v, err := evalComputedWithOpts(doc, exprRaw, child)
						if err != nil {
							return nil, err
						}
						out = append(out, v)
					}
					return out, nil
				case "$filter":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$filter must be a document")
					}
					inRaw, okI := spec["input"]
					condRaw, okC := spec["cond"]
					if !okI || !okC {
						return nil, fmt.Errorf("$filter requires input/cond")
					}
					as := "this"
					if rawAs, ok := spec["as"].(string); ok && rawAs != "" {
						as = rawAs
					}
					input, err := evalComputedWithOpts(doc, inRaw, opts)
					if err != nil {
						return nil, err
					}
					arr, ok := coerceInterfaceSlice(input)
					if !ok {
						return nil, nil
					}
					limit := -1
					if rawLim, ok := spec["limit"]; ok && rawLim != nil {
						if n, err := asInt(rawLim); err == nil {
							limit = n
						}
					}
					out := make([]interface{}, 0, len(arr))
					for i, el := range arr {
						child := opts
						child.vars = cloneVars(opts.vars)
						child.vars["this"] = el
						child.vars[as] = el
						child.vars["index"] = int64(i)
						cv, err := evalComputedWithOpts(doc, condRaw, child)
						if err != nil {
							return nil, err
						}
						if isTruthy(cv) {
							out = append(out, el)
							if limit >= 0 && len(out) >= limit {
								break
							}
						}
					}
					return out, nil
				case "$reduce":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$reduce must be a document")
					}
					inRaw, okI := spec["input"]
					initRaw, okInit := spec["initialValue"]
					inExpr, okIn := spec["in"]
					if !okI || !okInit || !okIn {
						return nil, fmt.Errorf("$reduce requires input/initialValue/in")
					}
					input, err := evalComputedWithOpts(doc, inRaw, opts)
					if err != nil {
						return nil, err
					}
					arr, ok := coerceInterfaceSlice(input)
					if !ok {
						return nil, nil
					}
					acc, err := evalComputedWithOpts(doc, initRaw, opts)
					if err != nil {
						return nil, err
					}
					for i, el := range arr {
						child := opts
						child.vars = cloneVars(opts.vars)
						child.vars["this"] = el
						child.vars["value"] = acc
						child.vars["index"] = int64(i)
						next, err := evalComputedWithOpts(doc, inExpr, child)
						if err != nil {
							return nil, err
						}
						acc = next
					}
					return acc, nil
				case "$sortArray":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$sortArray must be a document")
					}
					inRaw, okI := spec["input"]
					if !okI {
						return nil, fmt.Errorf("$sortArray requires input")
					}
					inVal, err := evalComputedWithOpts(doc, inRaw, opts)
					if err != nil {
						return nil, err
					}
					arr, ok := coerceInterfaceSlice(inVal)
					if !ok {
						return nil, nil
					}
					// Default: ascending scalar sort.
					dir := 1
					var field string
					if rawSortBy, ok := spec["sortBy"]; ok && rawSortBy != nil {
						if n, err := asInt(rawSortBy); err == nil {
							if n != 0 {
								dir = n
							}
						} else if m, ok := coerceBsonM(rawSortBy); ok && len(m) > 0 {
							for k, v := range m {
								field = k
								if n, err := asInt(v); err == nil && n != 0 {
									dir = n
								}
								break
							}
						}
					}
					out := make([]interface{}, 0, len(arr))
					out = append(out, arr...)
					sort.SliceStable(out, func(i, j int) bool {
						ai := out[i]
						aj := out[j]
						if field != "" {
							ai = getPathValueAny(ai, field)
							aj = getPathValueAny(aj, field)
						}
						c := cmp3(ai, aj)
						if dir < 0 {
							return c > 0
						}
						return c < 0
					})
					return out, nil
				case "$zip":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$zip must be a document")
					}
					rawInputs, ok := spec["inputs"]
					if !ok {
						return nil, fmt.Errorf("$zip requires inputs")
					}
					inputsArr, ok := coerceInterfaceSlice(rawInputs)
					if !ok || len(inputsArr) == 0 {
						return nil, nil
					}
					inputs := make([][]interface{}, 0, len(inputsArr))
					for _, it := range inputsArr {
						v, err := evalComputedWithOpts(doc, it, opts)
						if err != nil {
							return nil, err
						}
						a, ok := coerceInterfaceSlice(v)
						if !ok {
							return nil, nil
						}
						inputs = append(inputs, a)
					}
					useLongest := false
					if v, ok := spec["useLongestLength"]; ok {
						if b, ok := v.(bool); ok {
							useLongest = b
						}
					}
					defaults := []interface{}{}
					if rawDefs, ok := spec["defaults"]; ok {
						if arr, ok := coerceInterfaceSlice(rawDefs); ok {
							defaults = arr
						}
					}
					n := 0
					if useLongest {
						for _, a := range inputs {
							if len(a) > n {
								n = len(a)
							}
						}
					} else {
						n = len(inputs[0])
						for _, a := range inputs[1:] {
							if len(a) < n {
								n = len(a)
							}
						}
					}
					zipped := make([]interface{}, 0, n)
					for i := 0; i < n; i++ {
						row := make([]interface{}, 0, len(inputs))
						for j, a := range inputs {
							if i < len(a) {
								row = append(row, a[i])
								continue
							}
							if j < len(defaults) {
								row = append(row, defaults[j])
							} else {
								row = append(row, nil)
							}
						}
						zipped = append(zipped, row)
					}
					return zipped, nil

				// Arithmetic
				case "$add":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					// Best-effort date compatibility: if exactly one argument is a date,
					// treat other numeric args as milliseconds and return a date.
					var haveDate bool
					var base time.Time
					ms := 0.0
					for _, a := range args {
						if a == nil {
							return nil, nil
						}
						if tm, ok := coerceExplicitTime(a); ok {
							if haveDate {
								return nil, fmt.Errorf("$add supports at most one date")
							}
							haveDate = true
							base = tm.UTC()
							continue
						}
						f, ok := toFloat64(a)
						if !ok {
							return nil, nil
						}
						ms += f
					}
					if haveDate {
						return base.Add(time.Duration(int64(ms * float64(time.Millisecond)))), nil
					}
					sum := 0.0
					for _, a := range args {
						f, ok := toFloat64(a)
						if !ok {
							return nil, nil
						}
						sum += f
					}
					return sum, nil
				case "$subtract":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					if args[0] == nil || args[1] == nil {
						return nil, nil
					}
					if at, ok := coerceExplicitTime(args[0]); ok {
						at = at.UTC()
						if bt, ok := coerceExplicitTime(args[1]); ok {
							return int64(at.Sub(bt.UTC()).Milliseconds()), nil
						}
						ms, ok := toFloat64(args[1])
						if !ok {
							return nil, nil
						}
						return at.Add(-time.Duration(int64(ms * float64(time.Millisecond)))), nil
					}
					a, ok := toFloat64(args[0])
					if !ok {
						return nil, nil
					}
					b, ok := toFloat64(args[1])
					if !ok {
						return nil, nil
					}
					return a - b, nil
				case "$multiply":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					prod := 1.0
					for _, a := range args {
						if a == nil {
							return nil, nil
						}
						f, ok := toFloat64(a)
						if !ok {
							return nil, nil
						}
						prod *= f
					}
					return prod, nil
				case "$divide":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					a, ok := toFloat64(args[0])
					if !ok || args[0] == nil || args[1] == nil {
						return nil, nil
					}
					b, ok := toFloat64(args[1])
					if !ok || b == 0 {
						return nil, nil
					}
					return a / b, nil
				case "$mod":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					a, ok := toFloat64(args[0])
					if !ok || args[0] == nil || args[1] == nil {
						return nil, nil
					}
					b, ok := toFloat64(args[1])
					if !ok || b == 0 {
						return nil, nil
					}
					return math.Mod(a, b), nil
				case "$abs":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					f, ok := toFloat64(v)
					if !ok || v == nil {
						return nil, nil
					}
					return math.Abs(f), nil
				case "$ceil":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					f, ok := toFloat64(v)
					if !ok || v == nil {
						return nil, nil
					}
					return math.Ceil(f), nil
				case "$floor":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					f, ok := toFloat64(v)
					if !ok || v == nil {
						return nil, nil
					}
					return math.Floor(f), nil
				case "$sqrt":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					f, ok := toFloat64(v)
					if !ok || v == nil || f < 0 {
						return nil, nil
					}
					return math.Sqrt(f), nil
				case "$pow":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					a, ok := toFloat64(args[0])
					if !ok || args[0] == nil || args[1] == nil {
						return nil, nil
					}
					b, ok := toFloat64(args[1])
					if !ok {
						return nil, nil
					}
					return math.Pow(a, b), nil
				case "$round":
					args, err := evalArrayArgs(doc, arg, 1, opts)
					if err != nil {
						return nil, err
					}
					f, ok := toFloat64(args[0])
					if !ok || args[0] == nil {
						return nil, nil
					}
					place := int64(0)
					if len(args) >= 2 {
						if p, ok := toInt64IfIntegral(args[1]); ok {
							place = p
						}
					}
					scale := math.Pow10(int(place))
					return math.Round(f*scale) / scale, nil
				case "$trunc":
					args, err := evalArrayArgs(doc, arg, 1, opts)
					if err != nil {
						return nil, err
					}
					f, ok := toFloat64(args[0])
					if !ok || args[0] == nil {
						return nil, nil
					}
					place := int64(0)
					if len(args) >= 2 {
						if p, ok := toInt64IfIntegral(args[1]); ok {
							place = p
						}
					}
					scale := math.Pow10(int(place))
					return math.Trunc(f*scale) / scale, nil
				case "$exp":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					f, ok := toFloat64(v)
					if !ok || v == nil {
						return nil, nil
					}
					return math.Exp(f), nil
				case "$ln":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					f, ok := toFloat64(v)
					if !ok || v == nil || f <= 0 {
						return nil, nil
					}
					return math.Log(f), nil
				case "$log10":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					f, ok := toFloat64(v)
					if !ok || v == nil || f <= 0 {
						return nil, nil
					}
					return math.Log10(f), nil
				case "$log":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					a, ok := toFloat64(args[0])
					if !ok || args[0] == nil || args[1] == nil || a <= 0 {
						return nil, nil
					}
					base, ok := toFloat64(args[1])
					if !ok || base <= 0 || base == 1 {
						return nil, nil
					}
					return math.Log(a) / math.Log(base), nil

				// String
				case "$concat":
					args, err := evalArrayArgs(doc, arg, 1, opts)
					if err != nil {
						return nil, err
					}
					var b strings.Builder
					for _, a := range args {
						if a == nil {
							return nil, nil
						}
						b.WriteString(fmt.Sprint(a))
					}
					return b.String(), nil
				case "$toLower":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					if v == nil {
						return nil, nil
					}
					return strings.ToLower(fmt.Sprint(v)), nil
				case "$toUpper":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					if v == nil {
						return nil, nil
					}
					return strings.ToUpper(fmt.Sprint(v)), nil
				case "$split":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					if args[0] == nil || args[1] == nil {
						return nil, nil
					}
					s := fmt.Sprint(args[0])
					sep := fmt.Sprint(args[1])
					parts := strings.Split(s, sep)
					out := make([]interface{}, 0, len(parts))
					for _, p := range parts {
						out = append(out, p)
					}
					return out, nil
				case "$substr", "$substrBytes", "$substrCP":
					args, err := evalArrayArgs(doc, arg, 3, opts)
					if err != nil {
						return nil, err
					}
					if args[0] == nil || args[1] == nil || args[2] == nil {
						return nil, nil
					}
					s := fmt.Sprint(args[0])
					start, ok := toInt64IfIntegral(args[1])
					if !ok {
						return nil, nil
					}
					n, ok := toInt64IfIntegral(args[2])
					if !ok {
						return nil, nil
					}
					if start < 0 {
						start = 0
					}
					if n < 0 {
						return "", nil
					}
					if op == "$substrCP" {
						r := []rune(s)
						if int(start) > len(r) {
							return "", nil
						}
						end := int(start + n)
						if end > len(r) {
							end = len(r)
						}
						return string(r[start:end]), nil
					}
					// bytes (and $substr alias)
					b := []byte(s)
					if int(start) > len(b) {
						return "", nil
					}
					end := int(start + n)
					if end > len(b) {
						end = len(b)
					}
					return string(b[start:end]), nil
				case "$strLenCP":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					if v == nil {
						return nil, nil
					}
					return int64(len([]rune(fmt.Sprint(v)))), nil
				case "$indexOfBytes", "$indexOfCP":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					if args[0] == nil || args[1] == nil {
						return nil, nil
					}
					s := fmt.Sprint(args[0])
					sub := fmt.Sprint(args[1])
					start := int64(0)
					end := int64(-1)
					if len(args) >= 3 {
						if v, ok := toInt64IfIntegral(args[2]); ok {
							start = v
						}
					}
					if len(args) >= 4 {
						if v, ok := toInt64IfIntegral(args[3]); ok {
							end = v
						}
					}
					if start < 0 {
						start = 0
					}
					if op == "$indexOfCP" {
						runes := []rune(s)
						if int(start) > len(runes) {
							return int64(-1), nil
						}
						hay := string(runes[start:])
						if end >= 0 && int(end-start) < len([]rune(hay)) {
							hay = string([]rune(hay)[:end-start])
						}
						idx := strings.Index(hay, sub)
						if idx < 0 {
							return int64(-1), nil
						}
						return start + int64(len([]rune(hay[:idx]))), nil
					}
					b := []byte(s)
					if int(start) > len(b) {
						return int64(-1), nil
					}
					b = b[start:]
					if end >= 0 && int(end-start) < len(b) {
						b = b[:end-start]
					}
					idx := bytesIndex(b, []byte(sub))
					if idx < 0 {
						return int64(-1), nil
					}
					return start + int64(idx), nil
				case "$regexMatch":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$regexMatch must be a document")
					}
					inRaw, okI := spec["input"]
					reRaw, okR := spec["regex"]
					if !okI || !okR {
						return nil, fmt.Errorf("$regexMatch requires input/regex")
					}
					in, err := evalComputedWithOpts(doc, inRaw, opts)
					if err != nil {
						return nil, err
					}
					if in == nil {
						return nil, nil
					}
					pat, reOpts, err := coerceRegex(reRaw, spec["options"])
					if err != nil {
						return nil, err
					}
					rx, err := regexp.Compile(applyRegexOptions(pat, reOpts))
					if err != nil {
						return nil, err
					}
					return rx.MatchString(fmt.Sprint(in)), nil
				case "$regexFind", "$regexFindAll":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("%s must be a document", op)
					}
					inRaw, okI := spec["input"]
					reRaw, okR := spec["regex"]
					if !okI || !okR {
						return nil, fmt.Errorf("%s requires input/regex", op)
					}
					in, err := evalComputedWithOpts(doc, inRaw, opts)
					if err != nil {
						return nil, err
					}
					if in == nil {
						return nil, nil
					}
					pat, reOpts, err := coerceRegex(reRaw, spec["options"])
					if err != nil {
						return nil, err
					}
					rx, err := regexp.Compile(applyRegexOptions(pat, reOpts))
					if err != nil {
						return nil, err
					}
					s := fmt.Sprint(in)
					if op == "$regexFind" {
						loc := rx.FindStringIndex(s)
						if loc == nil {
							return nil, nil
						}
						return bson.M{"match": s[loc[0]:loc[1]], "idx": int64(loc[0])}, nil
					}
					locs := rx.FindAllStringIndex(s, -1)
					out := make([]interface{}, 0, len(locs))
					for _, loc := range locs {
						out = append(out, bson.M{"match": s[loc[0]:loc[1]], "idx": int64(loc[0])})
					}
					return out, nil
				case "$strcasecmp":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					if args[0] == nil || args[1] == nil {
						return nil, nil
					}
					a := strings.ToLower(fmt.Sprint(args[0]))
					b := strings.ToLower(fmt.Sprint(args[1]))
					if a < b {
						return int64(-1), nil
					}
					if a > b {
						return int64(1), nil
					}
					return int64(0), nil
				case "$replaceOne", "$replaceAll":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("%s must be a document", op)
					}
					inRaw, okI := spec["input"]
					findRaw, okF := spec["find"]
					repRaw, okR := spec["replacement"]
					if !okI || !okF || !okR {
						return nil, fmt.Errorf("%s requires input/find/replacement", op)
					}
					in, err := evalComputedWithOpts(doc, inRaw, opts)
					if err != nil {
						return nil, err
					}
					find, err := evalComputedWithOpts(doc, findRaw, opts)
					if err != nil {
						return nil, err
					}
					rep, err := evalComputedWithOpts(doc, repRaw, opts)
					if err != nil {
						return nil, err
					}
					if in == nil || find == nil || rep == nil {
						return nil, nil
					}
					s := fmt.Sprint(in)
					f := fmt.Sprint(find)
					r := fmt.Sprint(rep)
					if f == "" {
						return s, nil
					}
					if op == "$replaceOne" {
						return strings.Replace(s, f, r, 1), nil
					}
					return strings.ReplaceAll(s, f, r), nil
				case "$strLenBytes":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					if v == nil {
						return nil, nil
					}
					return int64(len([]byte(fmt.Sprint(v)))), nil
				case "$trim", "$ltrim", "$rtrim":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("%s must be a document", op)
					}
					inRaw, okI := spec["input"]
					if !okI {
						return nil, fmt.Errorf("%s requires input", op)
					}
					in, err := evalComputedWithOpts(doc, inRaw, opts)
					if err != nil {
						return nil, err
					}
					if in == nil {
						return nil, nil
					}
					s := fmt.Sprint(in)
					chars := ""
					if rawChars, ok := spec["chars"]; ok {
						cv, err := evalComputedWithOpts(doc, rawChars, opts)
						if err != nil {
							return nil, err
						}
						if cv != nil {
							chars = fmt.Sprint(cv)
						}
					}
					switch op {
					case "$trim":
						if chars == "" {
							return strings.TrimSpace(s), nil
						}
						return strings.Trim(s, chars), nil
					case "$ltrim":
						if chars == "" {
							return strings.TrimLeftFunc(s, func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' }), nil
						}
						return strings.TrimLeft(s, chars), nil
					default:
						if chars == "" {
							return strings.TrimRightFunc(s, func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' }), nil
						}
						return strings.TrimRight(s, chars), nil
					}

				// Date
				case "$toDate":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					tm, ok := coerceTime(v)
					if !ok {
						return nil, nil
					}
					return tm, nil
				case "$year":
					return datePart(doc, arg, func(t time.Time) int64 { return int64(t.Year()) }, opts)
				case "$month":
					return datePart(doc, arg, func(t time.Time) int64 { return int64(t.Month()) }, opts)
				case "$dayOfMonth":
					return datePart(doc, arg, func(t time.Time) int64 { return int64(t.Day()) }, opts)
				case "$dayOfYear":
					return datePart(doc, arg, func(t time.Time) int64 { return int64(t.YearDay()) }, opts)
				case "$hour":
					return datePart(doc, arg, func(t time.Time) int64 { return int64(t.Hour()) }, opts)
				case "$minute":
					return datePart(doc, arg, func(t time.Time) int64 { return int64(t.Minute()) }, opts)
				case "$second":
					return datePart(doc, arg, func(t time.Time) int64 { return int64(t.Second()) }, opts)
				case "$millisecond":
					return datePart(doc, arg, func(t time.Time) int64 { return int64(t.Nanosecond() / 1_000_000) }, opts)
				case "$dayOfWeek":
					return datePart(doc, arg, func(t time.Time) int64 { return int64(t.Weekday()) + 1 }, opts)
				case "$week":
					return datePart(doc, arg, func(t time.Time) int64 { return int64(mongoWeek(t)) }, opts)
				case "$dateAdd", "$dateSubtract":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("%s must be a document", op)
					}
					startRaw, okS := spec["startDate"]
					unit, _ := spec["unit"].(string)
					rawAmt, okA := spec["amount"]
					if !okS || !okA || unit == "" {
						return nil, fmt.Errorf("%s requires startDate/unit/amount", op)
					}
					startVal, err := evalComputedWithOpts(doc, startRaw, opts)
					if err != nil {
						return nil, err
					}
					tm, ok := coerceTime(startVal)
					if !ok {
						return nil, nil
					}
					amtVal, err := evalComputedWithOpts(doc, rawAmt, opts)
					if err != nil {
						return nil, err
					}
					amt, ok := toInt64IfIntegral(amtVal)
					if !ok {
						return nil, nil
					}
					if op == "$dateSubtract" {
						amt = -amt
					}
					return dateAdd(tm, unit, amt), nil
				case "$dateTrunc":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$dateTrunc must be a document")
					}
					dateRaw, okD := spec["date"]
					unit, _ := spec["unit"].(string)
					if !okD || unit == "" {
						return nil, fmt.Errorf("$dateTrunc requires date/unit")
					}
					dateVal, err := evalComputedWithOpts(doc, dateRaw, opts)
					if err != nil {
						return nil, err
					}
					tm, ok := coerceTime(dateVal)
					if !ok {
						return nil, nil
					}
					binSize := int64(1)
					if rawBin, ok := spec["binSize"]; ok {
						if n, ok := toInt64IfIntegral(rawBin); ok && n > 0 {
							binSize = n
						}
					}
					startOfWeek := "Sunday"
					if s, ok := spec["startOfWeek"].(string); ok && s != "" {
						startOfWeek = s
					}
					return dateTrunc(tm, unit, binSize, startOfWeek), nil
				case "$dateToString":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$dateToString must be a document")
					}
					dateRaw, okD := spec["date"]
					if !okD {
						return nil, fmt.Errorf("$dateToString requires date")
					}
					dateVal, err := evalComputedWithOpts(doc, dateRaw, opts)
					if err != nil {
						return nil, err
					}
					tm, ok := coerceTime(dateVal)
					if !ok {
						return nil, nil
					}
					format := "%Y-%m-%dT%H:%M:%S.%LZ"
					if f, ok := spec["format"].(string); ok && f != "" {
						format = f
					}
					layout, err := mongoDateFormatToGo(format)
					if err != nil {
						return nil, err
					}
					return tm.UTC().Format(layout), nil
				case "$dateFromString":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$dateFromString must be a document")
					}
					dsRaw, okD := spec["dateString"]
					if !okD {
						return nil, fmt.Errorf("$dateFromString requires dateString")
					}
					dsVal, err := evalComputedWithOpts(doc, dsRaw, opts)
					if err != nil {
						return nil, err
					}
					if dsVal == nil {
						return nil, nil
					}
					ds := fmt.Sprint(dsVal)
					if f, ok := spec["format"].(string); ok && f != "" {
						layout, err := mongoDateFormatToGo(f)
						if err != nil {
							return nil, err
						}
						tm, err := time.Parse(layout, ds)
						if err != nil {
							return nil, nil
						}
						return tm.UTC(), nil
					}
					tm, ok := coerceTime(ds)
					if ok {
						return tm.UTC(), nil
					}
					return nil, nil
				case "$dateToParts":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$dateToParts must be a document")
					}
					dateRaw, okD := spec["date"]
					if !okD {
						return nil, fmt.Errorf("$dateToParts requires date")
					}
					dateVal, err := evalComputedWithOpts(doc, dateRaw, opts)
					if err != nil {
						return nil, err
					}
					tm, ok := coerceTime(dateVal)
					if !ok {
						return nil, nil
					}
					tm = tm.UTC()
					return bson.M{
						"year":        int64(tm.Year()),
						"month":       int64(tm.Month()),
						"day":         int64(tm.Day()),
						"hour":        int64(tm.Hour()),
						"minute":      int64(tm.Minute()),
						"second":      int64(tm.Second()),
						"millisecond": int64(tm.Nanosecond() / 1_000_000),
					}, nil
				case "$dateFromParts":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$dateFromParts must be a document")
					}
					yearRaw, okY := spec["year"]
					if !okY {
						return nil, fmt.Errorf("$dateFromParts requires year")
					}
					yv, err := evalComputedWithOpts(doc, yearRaw, opts)
					if err != nil {
						return nil, err
					}
					year, ok := toInt64IfIntegral(yv)
					if !ok {
						return nil, nil
					}
					month := int64(1)
					day := int64(1)
					hour := int64(0)
					minute := int64(0)
					second := int64(0)
					millisecond := int64(0)
					if v, ok := spec["month"]; ok {
						if vv, err := evalComputedWithOpts(doc, v, opts); err == nil {
							if n, ok := toInt64IfIntegral(vv); ok {
								month = n
							}
						}
					}
					if v, ok := spec["day"]; ok {
						if vv, err := evalComputedWithOpts(doc, v, opts); err == nil {
							if n, ok := toInt64IfIntegral(vv); ok {
								day = n
							}
						}
					}
					if v, ok := spec["hour"]; ok {
						if vv, err := evalComputedWithOpts(doc, v, opts); err == nil {
							if n, ok := toInt64IfIntegral(vv); ok {
								hour = n
							}
						}
					}
					if v, ok := spec["minute"]; ok {
						if vv, err := evalComputedWithOpts(doc, v, opts); err == nil {
							if n, ok := toInt64IfIntegral(vv); ok {
								minute = n
							}
						}
					}
					if v, ok := spec["second"]; ok {
						if vv, err := evalComputedWithOpts(doc, v, opts); err == nil {
							if n, ok := toInt64IfIntegral(vv); ok {
								second = n
							}
						}
					}
					if v, ok := spec["millisecond"]; ok {
						if vv, err := evalComputedWithOpts(doc, v, opts); err == nil {
							if n, ok := toInt64IfIntegral(vv); ok {
								millisecond = n
							}
						}
					}
					return time.Date(int(year), time.Month(month), int(day), int(hour), int(minute), int(second), int(millisecond)*1_000_000, time.UTC), nil
				case "$dateDiff":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$dateDiff must be a document")
					}
					startRaw, okS := spec["startDate"]
					endRaw, okE := spec["endDate"]
					unit, _ := spec["unit"].(string)
					if !okS || !okE || unit == "" {
						return nil, fmt.Errorf("$dateDiff requires startDate/endDate/unit")
					}
					sv, err := evalComputedWithOpts(doc, startRaw, opts)
					if err != nil {
						return nil, err
					}
					ev, err := evalComputedWithOpts(doc, endRaw, opts)
					if err != nil {
						return nil, err
					}
					st, ok := coerceTime(sv)
					if !ok {
						return nil, nil
					}
					et, ok := coerceTime(ev)
					if !ok {
						return nil, nil
					}
					d := et.UTC().Sub(st.UTC())
					switch strings.ToLower(unit) {
					case "millisecond":
						return int64(d / time.Millisecond), nil
					case "second":
						return int64(d / time.Second), nil
					case "minute":
						return int64(d / time.Minute), nil
					case "hour":
						return int64(d / time.Hour), nil
					case "day":
						return int64(d / (24 * time.Hour)), nil
					case "week":
						return int64(d / (7 * 24 * time.Hour)), nil
					case "month":
						return dateDiffMonths(st, et), nil
					case "quarter":
						return dateDiffMonths(st, et) / 3, nil
					case "year":
						return dateDiffYears(st, et), nil
					default:
						return nil, fmt.Errorf("$dateDiff unsupported unit: %s", unit)
					}
				case "$isoWeek":
					return datePart(doc, arg, func(t time.Time) int64 {
						_, w := t.ISOWeek()
						return int64(w)
					}, opts)
				case "$isoWeekYear":
					return datePart(doc, arg, func(t time.Time) int64 {
						y, _ := t.ISOWeek()
						return int64(y)
					}, opts)
				case "$isoDayOfWeek":
					return datePart(doc, arg, func(t time.Time) int64 {
						// Monday=1 .. Sunday=7
						wd := int64(t.Weekday())
						if wd == 0 {
							return 7
						}
						return wd
					}, opts)

				// Object
				case "$getField":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$getField must be a document")
					}
					fieldRaw, okF := spec["field"]
					inputRaw, okI := spec["input"]
					if !okF || !okI {
						return nil, fmt.Errorf("$getField requires field/input")
					}
					fv, err := evalComputedWithOpts(doc, fieldRaw, opts)
					if err != nil {
						return nil, err
					}
					iv, err := evalComputedWithOpts(doc, inputRaw, opts)
					if err != nil {
						return nil, err
					}
					if fv == nil || iv == nil {
						return nil, nil
					}
					m, ok := coerceBsonM(iv)
					if !ok {
						return nil, nil
					}
					return m[fmt.Sprint(fv)], nil
				case "$setField":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$setField must be a document")
					}
					fieldRaw, okF := spec["field"]
					inputRaw, okI := spec["input"]
					valRaw, okV := spec["value"]
					if !okF || !okI || !okV {
						return nil, fmt.Errorf("$setField requires field/input/value")
					}
					fv, err := evalComputedWithOpts(doc, fieldRaw, opts)
					if err != nil {
						return nil, err
					}
					iv, err := evalComputedWithOpts(doc, inputRaw, opts)
					if err != nil {
						return nil, err
					}
					vv, err := evalComputedWithOpts(doc, valRaw, opts)
					if err != nil {
						return nil, err
					}
					if fv == nil || iv == nil {
						return nil, nil
					}
					m, ok := coerceBsonM(iv)
					if !ok {
						return nil, nil
					}
					out := bson.M{}
					for k, v := range m {
						out[k] = v
					}
					out[fmt.Sprint(fv)] = vv
					return out, nil
				case "$unsetField":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$unsetField must be a document")
					}
					fieldRaw, okF := spec["field"]
					inputRaw, okI := spec["input"]
					if !okF || !okI {
						return nil, fmt.Errorf("$unsetField requires field/input")
					}
					fv, err := evalComputedWithOpts(doc, fieldRaw, opts)
					if err != nil {
						return nil, err
					}
					iv, err := evalComputedWithOpts(doc, inputRaw, opts)
					if err != nil {
						return nil, err
					}
					if fv == nil || iv == nil {
						return nil, nil
					}
					m, ok := coerceBsonM(iv)
					if !ok {
						return nil, nil
					}
					out := bson.M{}
					for k, v := range m {
						out[k] = v
					}
					delete(out, fmt.Sprint(fv))
					return out, nil
				case "$mergeObjects":
					args, err := evalArrayArgs(doc, arg, 1, opts)
					if err != nil {
						return nil, err
					}
					out := bson.M{}
					for _, it := range args {
						if it == nil {
							continue
						}
						m, ok := coerceBsonM(it)
						if !ok {
							continue
						}
						for k, v := range m {
							out[k] = v
						}
					}
					return out, nil

				// Object/Array conversion
				case "$objectToArray":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					m, ok := coerceBsonM(v)
					if !ok {
						return nil, nil
					}
					out := make([]interface{}, 0, len(m))
					for k, vv := range m {
						out = append(out, bson.M{"k": k, "v": vv})
					}
					return out, nil
				case "$arrayToObject":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					arr, ok := coerceInterfaceSlice(v)
					if !ok {
						return nil, nil
					}
					out := bson.M{}
					for _, it := range arr {
						if m, ok := coerceBsonM(it); ok {
							k, okK := m["k"]
							vv, okV := m["v"]
							if okK && okV {
								out[fmt.Sprint(k)] = vv
							}
							continue
						}
						if pair, ok := coerceInterfaceSlice(it); ok && len(pair) == 2 {
							out[fmt.Sprint(pair[0])] = pair[1]
						}
					}
					return out, nil
				case "$indexOfArray":
					args, err := evalArrayArgs(doc, arg, 2, opts)
					if err != nil {
						return nil, err
					}
					arr, ok := coerceInterfaceSlice(args[0])
					if !ok {
						return int64(-1), nil
					}
					search := fmt.Sprint(args[1])
					start := int64(0)
					end := int64(len(arr))
					if len(args) >= 3 {
						if v, ok := toInt64IfIntegral(args[2]); ok {
							start = v
						}
					}
					if len(args) >= 4 {
						if v, ok := toInt64IfIntegral(args[3]); ok {
							end = v
						}
					}
					if start < 0 {
						start = 0
					}
					if end > int64(len(arr)) {
						end = int64(len(arr))
					}
					for i := start; i < end; i++ {
						if fmt.Sprint(arr[i]) == search {
							return i, nil
						}
					}
					return int64(-1), nil

				// Type conversion
				case "$toString":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					if v == nil {
						return nil, nil
					}
					if tm, ok := coerceTime(v); ok {
						return tm.UTC().Format(time.RFC3339Nano), nil
					}
					return fmt.Sprint(v), nil
				case "$toInt", "$toLong":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					if v == nil {
						return nil, nil
					}
					if i, ok := toInt64IfIntegral(v); ok {
						return i, nil
					}
					if f, ok := toFloat64(v); ok {
						return int64(f), nil
					}
					return nil, nil
				case "$toDouble":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					if v == nil {
						return nil, nil
					}
					if f, ok := toFloat64(v); ok {
						return f, nil
					}
					return nil, nil
				case "$toBool":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					if v == nil {
						return nil, nil
					}
					switch x := v.(type) {
					case bool:
						return x, nil
					case string:
						return x != "", nil
					default:
						if f, ok := toFloat64(v); ok {
							return f != 0, nil
						}
						return isTruthy(v), nil
					}
				case "$type":
					v, err := evalComputedWithOpts(doc, arg, opts)
					if err != nil {
						return nil, err
					}
					return mongoTypeOf(v), nil
				case "$convert":
					spec, ok := coerceBsonM(arg)
					if !ok {
						return nil, fmt.Errorf("$convert must be a document")
					}
					inRaw, okI := spec["input"]
					toRaw, okT := spec["to"]
					if !okI || !okT {
						return nil, fmt.Errorf("$convert requires input/to")
					}
					inVal, err := evalComputedWithOpts(doc, inRaw, opts)
					if err != nil {
						return nil, err
					}
					if inVal == nil {
						if onNull, ok := spec["onNull"]; ok {
							return evalComputedWithOpts(doc, onNull, opts)
						}
						return nil, nil
					}
					toVal, err := evalComputedWithOpts(doc, toRaw, opts)
					if err != nil {
						return nil, err
					}
					to := strings.ToLower(fmt.Sprint(toVal))
					converted, ok := convertTo(to, inVal)
					if ok {
						return converted, nil
					}
					if onErr, ok := spec["onError"]; ok {
						return evalComputedWithOpts(doc, onErr, opts)
					}
					return nil, nil

				default:
					return nil, fmt.Errorf("unsupported computed expression: %v", m)
				}
			}
		}

		// Literal document (constant).
		return m, nil
	}

	// Constant (number/bool/null/array/etc).
	return expr, nil
}

func evalArrayArgs(doc bson.M, raw interface{}, min int, opts evalOpts) ([]interface{}, error) {
	arr, ok := coerceInterfaceSlice(raw)
	if !ok || len(arr) < min {
		return nil, fmt.Errorf("expression requires an array with at least %d args", min)
	}
	out := make([]interface{}, 0, len(arr))
	for _, it := range arr {
		v, err := evalComputedWithOpts(doc, it, opts)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func evalCompareOp(doc bson.M, raw interface{}, pred func(int) bool, opts evalOpts) (bool, error) {
	args, err := evalArrayArgs(doc, raw, 2, opts)
	if err != nil {
		return false, err
	}
	return pred(cmp3(args[0], args[1])), nil
}

func cmp3(a, b interface{}) int {
	if af, ok := toFloat64(a); ok {
		if bf, ok := toFloat64(b); ok {
			if af < bf {
				return -1
			}
			if af > bf {
				return 1
			}
			return 0
		}
	}
	as := fmt.Sprint(a)
	bs := fmt.Sprint(b)
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}

func toInt64IfIntegralMust(v interface{}, err error) (int64, bool) {
	if err != nil {
		return 0, false
	}
	return toInt64IfIntegral(v)
}

func datePart(doc bson.M, raw interface{}, fn func(time.Time) int64, opts evalOpts) (interface{}, error) {
	v, err := evalComputedWithOpts(doc, raw, opts)
	if err != nil {
		return nil, err
	}
	tm, ok := coerceTime(v)
	if !ok {
		return nil, nil
	}
	return fn(tm.UTC()), nil
}

func mongoWeek(t time.Time) int {
	t = t.UTC()
	year := t.Year()
	jan1 := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
	// Go: Sunday=0 ... Saturday=6
	offset := (7 - int(jan1.Weekday())) % 7
	firstSunday := jan1.AddDate(0, 0, offset)
	if t.Before(firstSunday) {
		return 0
	}
	days := int(t.Sub(firstSunday).Hours() / 24)
	return 1 + (days / 7)
}

func dateDiffMonths(start, end time.Time) int64 {
	start = start.UTC()
	end = end.UTC()
	sign := int64(1)
	if end.Before(start) {
		start, end = end, start
		sign = -1
	}
	y1, m1, _ := start.Date()
	y2, m2, _ := end.Date()
	months := (y2-y1)*12 + int(m2-m1)
	// Adjust using AddDate to approximate Mongo's "whole units" behavior.
	for months > 0 && start.AddDate(0, months, 0).After(end) {
		months--
	}
	for start.AddDate(0, months+1, 0).Before(end) || start.AddDate(0, months+1, 0).Equal(end) {
		months++
	}
	return int64(months) * sign
}

func dateDiffYears(start, end time.Time) int64 {
	start = start.UTC()
	end = end.UTC()
	sign := int64(1)
	if end.Before(start) {
		start, end = end, start
		sign = -1
	}
	years := end.Year() - start.Year()
	for years > 0 && start.AddDate(years, 0, 0).After(end) {
		years--
	}
	for start.AddDate(years+1, 0, 0).Before(end) || start.AddDate(years+1, 0, 0).Equal(end) {
		years++
	}
	return int64(years) * sign
}

func cloneVars(in map[string]interface{}) map[string]interface{} {
	if in == nil {
		return map[string]interface{}{}
	}
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func getPathValueAny(root interface{}, path string) interface{} {
	if path == "" {
		return root
	}
	cur := root
	for _, p := range strings.Split(path, ".") {
		m, ok := coerceBsonM(cur)
		if !ok {
			return nil
		}
		v, ok := m[p]
		if !ok {
			return nil
		}
		cur = v
	}
	return cur
}

func bytesIndex(b, sub []byte) int {
	// tiny helper to avoid importing bytes just for Index
	if len(sub) == 0 {
		return 0
	}
	for i := 0; i+len(sub) <= len(b); i++ {
		if string(b[i:i+len(sub)]) == string(sub) {
			return i
		}
	}
	return -1
}

func coerceRegex(raw interface{}, rawOptions interface{}) (pattern string, options string, err error) {
	switch t := raw.(type) {
	case string:
		pattern = t
	case bson.RegEx:
		pattern = t.Pattern
		options = t.Options
	default:
		if m, ok := coerceBsonM(raw); ok {
			// Extended JSON shape: { $regex: "...", $options: "i" }
			if r, ok := m["$regex"].(string); ok {
				pattern = r
			}
			if o, ok := m["$options"].(string); ok {
				options = o
			}
		}
	}
	if o, ok := rawOptions.(string); ok && o != "" {
		options = o
	}
	if pattern == "" {
		return "", "", fmt.Errorf("regex must be a string")
	}
	return pattern, options, nil
}

func applyRegexOptions(pattern string, options string) string {
	// Support a small subset of Mongo options.
	prefix := ""
	if strings.Contains(options, "i") {
		prefix += "(?i)"
	}
	if strings.Contains(options, "m") {
		prefix += "(?m)"
	}
	if strings.Contains(options, "s") {
		prefix += "(?s)"
	}
	return prefix + pattern
}

func mongoDateFormatToGo(fmtStr string) (string, error) {
	// Minimal token mapping. Unsupported tokens return an error.
	out := fmtStr
	repls := []struct{ from, to string }{
		{"%Y", "2006"},
		{"%m", "01"},
		{"%d", "02"},
		{"%H", "15"},
		{"%M", "04"},
		{"%S", "05"},
		{"%L", "000"},
		{"%z", "-0700"},
	}
	for _, r := range repls {
		out = strings.ReplaceAll(out, r.from, r.to)
	}
	if strings.Contains(out, "%") {
		return "", fmt.Errorf("unsupported date format token in %q", fmtStr)
	}
	return out, nil
}

func dateAdd(t time.Time, unit string, amount int64) time.Time {
	t = t.UTC()
	switch strings.ToLower(unit) {
	case "millisecond":
		return t.Add(time.Duration(amount) * time.Millisecond)
	case "second":
		return t.Add(time.Duration(amount) * time.Second)
	case "minute":
		return t.Add(time.Duration(amount) * time.Minute)
	case "hour":
		return t.Add(time.Duration(amount) * time.Hour)
	case "day":
		return t.AddDate(0, 0, int(amount))
	case "week":
		return t.AddDate(0, 0, int(amount*7))
	case "month":
		return t.AddDate(0, int(amount), 0)
	case "year":
		return t.AddDate(int(amount), 0, 0)
	default:
		return t
	}
}

func dateTrunc(t time.Time, unit string, binSize int64, startOfWeek string) time.Time {
	t = t.UTC()
	if binSize <= 0 {
		binSize = 1
	}
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	switch strings.ToLower(unit) {
	case "second":
		sec := (t.Unix() / binSize) * binSize
		return time.Unix(sec, 0).UTC()
	case "minute":
		sec := (t.Unix() / 60 / binSize) * 60 * binSize
		return time.Unix(sec, 0).UTC()
	case "hour":
		sec := (t.Unix() / 3600 / binSize) * 3600 * binSize
		return time.Unix(sec, 0).UTC()
	case "day":
		d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		days := int64(d.Sub(epoch).Hours() / 24)
		tdays := (days / binSize) * binSize
		return epoch.AddDate(0, 0, int(tdays))
	case "week":
		// Minimal: Sunday-based weeks by default.
		wd := int(t.Weekday()) // Sunday=0
		start := strings.ToLower(startOfWeek)
		shift := 0
		if start == "monday" {
			shift = 1
		}
		// Compute start of week.
		d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		d = d.AddDate(0, 0, -((wd - shift + 7) % 7))
		weeks := int64(d.Sub(epoch).Hours() / 24 / 7)
		tweeks := (weeks / binSize) * binSize
		return epoch.AddDate(0, 0, int(tweeks*7))
	case "month":
		m := int((int64(t.Month())-1)/binSize*binSize) + 1
		return time.Date(t.Year(), time.Month(m), 1, 0, 0, 0, 0, time.UTC)
	case "year":
		y := int(int64(t.Year()) / binSize * binSize)
		return time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC)
	default:
		return t
	}
}

func coerceTime(v interface{}) (time.Time, bool) {
	if v == nil {
		return time.Time{}, false
	}
	switch t := v.(type) {
	case time.Time:
		return t, true
	case *time.Time:
		if t == nil {
			return time.Time{}, false
		}
		return *t, true
	case bson.MongoTimestamp:
		// High 32 bits are seconds since epoch.
		sec := int64(uint64(t) >> 32)
		return time.Unix(sec, 0).UTC(), true
	case int64:
		// Heuristic: treat large values as milliseconds since epoch.
		if t > 1_000_000_000_000 {
			return time.UnixMilli(t).UTC(), true
		}
		return time.Unix(t, 0).UTC(), true
	case int32:
		return time.Unix(int64(t), 0).UTC(), true
	case int:
		return time.Unix(int64(t), 0).UTC(), true
	case float64:
		sec := int64(t)
		return time.Unix(sec, 0).UTC(), true
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return time.Time{}, false
		}
		if tm, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return tm, true
		}
		if tm, err := time.Parse(time.RFC3339, s); err == nil {
			return tm, true
		}
		if tm, err := time.Parse("2006-01-02", s); err == nil {
			return tm, true
		}
		return time.Time{}, false
	default:
		return time.Time{}, false
	}
}

func coerceExplicitTime(v interface{}) (time.Time, bool) {
	if v == nil {
		return time.Time{}, false
	}
	switch t := v.(type) {
	case time.Time:
		return t, true
	case *time.Time:
		if t == nil {
			return time.Time{}, false
		}
		return *t, true
	case bson.MongoTimestamp:
		sec := int64(uint64(t) >> 32)
		return time.Unix(sec, 0).UTC(), true
	default:
		return time.Time{}, false
	}
}

type groupState struct {
	id             interface{}
	sum            map[string]float64
	count          map[string]int64
	sumOnlyFloat   map[string]float64
	sumOnlyInt     map[string]int64
	sumOnlyIsFloat map[string]bool
	sumInt         map[string]int64
	max            map[string]interface{}
	min            map[string]interface{}
	maxSet         map[string]bool
	minSet         map[string]bool
	first          map[string]interface{}
	firstSet       map[string]bool
	last           map[string]interface{}
	lastSet        map[string]bool
	push           map[string][]interface{}
	addToSetKeys   map[string]map[string]struct{}
	addToSetVals   map[string][]interface{}
}
