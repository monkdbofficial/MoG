package pipeline

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"time"

	"gopkg.in/mgo.v2/bson"
)

// $group stage and shared type/sort helpers.
func applyGroup(docs []bson.M, spec bson.M) ([]bson.M, error) {
	rawID, ok := spec["_id"]
	if !ok {
		return nil, fmt.Errorf("$group requires _id")
	}

	accSpecs := map[string]bson.M{}
	for k, v := range spec {
		if k == "_id" {
			continue
		}
		m, ok := v.(bson.M)
		if !ok {
			return nil, fmt.Errorf("$group accumulator %q must be a document", k)
		}
		accSpecs[k] = m
	}

	states := map[string]*groupState{}
	order := []string{}

	for _, d := range docs {
		id, err := evalGroupID(d, rawID)
		if err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%T:%v", id, id)
		st := states[key]
		if st == nil {
			st = &groupState{
				id:             id,
				sum:            map[string]float64{},
				count:          map[string]int64{},
				sumOnlyFloat:   map[string]float64{},
				sumOnlyInt:     map[string]int64{},
				sumOnlyIsFloat: map[string]bool{},
				sumInt:         map[string]int64{},
				max:            map[string]interface{}{},
				min:            map[string]interface{}{},
				maxSet:         map[string]bool{},
				minSet:         map[string]bool{},
				first:          map[string]interface{}{},
				firstSet:       map[string]bool{},
				last:           map[string]interface{}{},
				lastSet:        map[string]bool{},
				push:           map[string][]interface{}{},
				addToSetKeys:   map[string]map[string]struct{}{},
				addToSetVals:   map[string][]interface{}{},
			}
			states[key] = st
			order = append(order, key)
		}

		for outField, acc := range accSpecs {
			if avgArg, ok := acc["$avg"]; ok {
				val, err := evalValue(d, avgArg)
				if err != nil {
					return nil, err
				}
				f, ok := toFloat64(val)
				if !ok {
					continue
				}
				st.sum[outField] += f
				st.count[outField]++
				continue
			}
			if sumArg, ok := acc["$sum"]; ok {
				// Fast-path: $sum: 1 is used for counts in a lot of drivers (including count_documents).
				if isNumericOne(sumArg) {
					st.sumInt[outField]++
					continue
				}

				val, err := evalValue(d, sumArg)
				if err != nil {
					return nil, err
				}

				if i64, ok := toInt64IfIntegral(val); ok && !st.sumOnlyIsFloat[outField] {
					st.sumOnlyInt[outField] += i64
					continue
				}

				f, ok := toFloat64(val)
				if !ok {
					continue
				}
				if !st.sumOnlyIsFloat[outField] {
					st.sumOnlyIsFloat[outField] = true
					st.sumOnlyFloat[outField] = float64(st.sumOnlyInt[outField])
				}
				st.sumOnlyFloat[outField] += f
				continue
			}
			if addArg, ok := acc["$addToSet"]; ok {
				val, err := evalValue(d, addArg)
				if err != nil {
					return nil, err
				}
				k := addToSetKey(val)
				if st.addToSetKeys[outField] == nil {
					st.addToSetKeys[outField] = map[string]struct{}{}
				}
				if _, exists := st.addToSetKeys[outField][k]; !exists {
					st.addToSetKeys[outField][k] = struct{}{}
					st.addToSetVals[outField] = append(st.addToSetVals[outField], deepClone(val))
				}
				continue
			}
			if maxArg, ok := acc["$max"]; ok {
				val, err := evalValue(d, maxArg)
				if err != nil {
					return nil, err
				}
				if val == nil {
					continue
				}
				if !st.maxSet[outField] || lessValue(st.max[outField], val) {
					st.max[outField] = deepClone(val)
					st.maxSet[outField] = true
				}
				continue
			}
			if minArg, ok := acc["$min"]; ok {
				val, err := evalValue(d, minArg)
				if err != nil {
					return nil, err
				}
				if val == nil {
					continue
				}
				if !st.minSet[outField] || lessValue(val, st.min[outField]) {
					st.min[outField] = deepClone(val)
					st.minSet[outField] = true
				}
				continue
			}
			if firstArg, ok := acc["$first"]; ok {
				if st.firstSet[outField] {
					continue
				}
				val, err := evalValue(d, firstArg)
				if err != nil {
					return nil, err
				}
				st.first[outField] = deepClone(val)
				st.firstSet[outField] = true
				continue
			}
			if lastArg, ok := acc["$last"]; ok {
				val, err := evalValue(d, lastArg)
				if err != nil {
					return nil, err
				}
				st.last[outField] = deepClone(val)
				st.lastSet[outField] = true
				continue
			}
			if pushArg, ok := acc["$push"]; ok {
				val, err := evalValue(d, pushArg)
				if err != nil {
					return nil, err
				}
				st.push[outField] = append(st.push[outField], deepClone(val))
				continue
			}
			return nil, fmt.Errorf("unsupported $group accumulator: %v", acc)
		}
	}

	var out []bson.M
	for _, k := range order {
		st := states[k]
		doc := bson.M{"_id": st.id}
		for outField := range accSpecs {
			if c, ok := st.count[outField]; ok {
				if c == 0 {
					doc[outField] = float64(0)
				} else {
					doc[outField] = st.sum[outField] / float64(c)
				}
				continue
			}
			if si, ok := st.sumInt[outField]; ok {
				doc[outField] = si
				continue
			}
			if vals, ok := st.addToSetVals[outField]; ok {
				if vals == nil {
					doc[outField] = []interface{}{}
				} else {
					doc[outField] = vals
				}
				continue
			}
			if st.maxSet[outField] {
				doc[outField] = st.max[outField]
				continue
			}
			if st.minSet[outField] {
				doc[outField] = st.min[outField]
				continue
			}
			if st.firstSet[outField] {
				doc[outField] = st.first[outField]
				continue
			}
			if st.lastSet[outField] {
				doc[outField] = st.last[outField]
				continue
			}
			if vals, ok := st.push[outField]; ok {
				doc[outField] = vals
				continue
			}
			if st.sumOnlyIsFloat[outField] {
				doc[outField] = st.sumOnlyFloat[outField]
				continue
			}
			if s, ok := st.sumOnlyInt[outField]; ok {
				doc[outField] = s
			}
		}
		out = append(out, doc)
	}
	return out, nil
}

func addToSetKey(v interface{}) string {
	if v == nil {
		return "nil"
	}
	if oid, ok := v.(bson.ObjectId); ok {
		return "oid:" + oid.Hex()
	}
	if i64, ok := toInt64IfIntegral(v); ok {
		return fmt.Sprintf("i:%d", i64)
	}
	if f64, ok := toFloat64(v); ok {
		return fmt.Sprintf("f:%g", f64)
	}
	if s, ok := v.(string); ok {
		return "s:" + s
	}
	if b, ok := v.(bool); ok {
		if b {
			return "b:true"
		}
		return "b:false"
	}
	if str, err := marshalObject(v); err == nil {
		return "j:" + str
	}
	return fmt.Sprintf("%T:%v", v, v)
}

func evalGroupID(doc bson.M, rawID interface{}) (interface{}, error) {
	// Field path: "$age"
	if s, ok := rawID.(string); ok && len(s) > 1 && s[0] == '$' {
		return getPathValue(doc, s[1:]), nil
	}
	// Constant (null/number/string/etc)
	return rawID, nil
}

func isNumericOne(v interface{}) bool {
	switch x := v.(type) {
	case int:
		return x == 1
	case int32:
		return x == 1
	case int64:
		return x == 1
	case float64:
		return x == 1
	case float32:
		return x == 1
	default:
		return false
	}
}

func applySort(docs []bson.M, spec bson.M) []bson.M {
	type keyDir struct {
		key string
		dir int
	}
	var keys []keyDir
	for k, v := range spec {
		dir, err := asInt(v)
		if err != nil {
			continue
		}
		if dir == 0 {
			dir = 1
		}
		keys = append(keys, keyDir{key: k, dir: dir})
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].key < keys[j].key })

	sort.SliceStable(docs, func(i, j int) bool {
		a := docs[i]
		b := docs[j]
		for _, kd := range keys {
			av := getPathValue(a, kd.key)
			bv := getPathValue(b, kd.key)

			if af, ok := toFloat64(av); ok {
				if bf, ok := toFloat64(bv); ok {
					if af == bf {
						continue
					}
					if kd.dir < 0 {
						return af > bf
					}
					return af < bf
				}
			}

			as := fmt.Sprint(av)
			bs := fmt.Sprint(bv)
			if as == bs {
				continue
			}
			if kd.dir < 0 {
				return as > bs
			}
			return as < bs
		}
		return false
	})
	return docs
}

func cmpNumber(a, b interface{}, fn func(float64, float64) bool) bool {
	af, ok := toFloat64(a)
	if !ok {
		return false
	}
	bf, ok := toFloat64(b)
	if !ok {
		return false
	}
	return fn(af, bf)
}

func lessValue(a, b interface{}) bool {
	if af, ok := toFloat64(a); ok {
		if bf, ok := toFloat64(b); ok {
			return af < bf
		}
	}
	return fmt.Sprint(a) < fmt.Sprint(b)
}

func mongoTypeOf(v interface{}) interface{} {
	if v == nil {
		return "null"
	}
	switch v.(type) {
	case float32, float64:
		return "double"
	case int, int8, int16, int32:
		return "int"
	case int64, uint, uint8, uint16, uint32, uint64:
		return "long"
	case string:
		return "string"
	case bool:
		return "bool"
	case time.Time:
		return "date"
	case bson.M, map[string]interface{}:
		return "object"
	default:
		rv := reflect.ValueOf(v)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
			return "array"
		}
		return "unknown"
	}
}

func convertTo(to string, v interface{}) (interface{}, bool) {
	switch to {
	case "string":
		if v == nil {
			return nil, true
		}
		if tm, ok := coerceTime(v); ok {
			return tm.UTC().Format(time.RFC3339Nano), true
		}
		return fmt.Sprint(v), true
	case "int", "long":
		if i, ok := toInt64IfIntegral(v); ok {
			return i, true
		}
		if f, ok := toFloat64(v); ok {
			return int64(f), true
		}
		return nil, false
	case "double":
		if f, ok := toFloat64(v); ok {
			return f, true
		}
		return nil, false
	case "bool":
		if v == nil {
			return nil, true
		}
		switch x := v.(type) {
		case bool:
			return x, true
		case string:
			return x != "", true
		default:
			if f, ok := toFloat64(v); ok {
				return f != 0, true
			}
			return isTruthy(v), true
		}
	case "date":
		tm, ok := coerceTime(v)
		if !ok {
			return nil, false
		}
		return tm.UTC(), true
	default:
		return nil, false
	}
}

func toInt64IfIntegral(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case float32:
		f := float64(x)
		if math.Trunc(f) == f {
			return int64(f), true
		}
		return 0, false
	case float64:
		if math.Trunc(x) == x {
			return int64(x), true
		}
		return 0, false
	case string:
		// Only treat as integral if it parses cleanly as int64.
		i, err := strconv.ParseInt(x, 10, 64)
		if err == nil {
			return i, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func toFloat64(v interface{}) (float64, bool) {
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
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func asInt(v interface{}) (int, error) {
	switch x := v.(type) {
	case int:
		return x, nil
	case int32:
		return int(x), nil
	case int64:
		return int(x), nil
	case float64:
		return int(x), nil
	case string:
		i, err := strconv.Atoi(x)
		if err != nil {
			return 0, err
		}
		return i, nil
	default:
		return 0, fmt.Errorf("not int")
	}
}
