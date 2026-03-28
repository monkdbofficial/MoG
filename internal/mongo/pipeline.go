package mongo

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"
)

type lookupResolver func(from string) ([]bson.M, error)

const mogVectorSearchScoreKey = "__mog_vectorSearchScore"

func applyPipeline(docs []bson.M, pipeline []bson.M) ([]bson.M, error) {
	return applyPipelineWithLookup(docs, pipeline, nil)
}

func applyPipelineWithLookup(docs []bson.M, pipeline []bson.M, resolve lookupResolver) ([]bson.M, error) {
	out := docs
	for _, stage := range pipeline {
		switch {
		case stage["$vectorSearch"] != nil:
			spec, ok := coerceBsonM(stage["$vectorSearch"])
			if !ok {
				return nil, fmt.Errorf("$vectorSearch stage must be a document")
			}
			var err error
			out, err = applyVectorSearch(out, spec)
			if err != nil {
				return nil, err
			}
		case stage["$match"] != nil:
			m, ok := stage["$match"].(bson.M)
			if !ok {
				return nil, fmt.Errorf("$match stage must be a document")
			}
			out = applyMatch(out, m)
		case stage["$project"] != nil:
			p, ok := stage["$project"].(bson.M)
			if !ok {
				return nil, fmt.Errorf("$project stage must be a document")
			}
			var err error
			out, err = applyProject(out, p)
			if err != nil {
				return nil, err
			}
		case stage["$addFields"] != nil:
			spec, ok := coerceBsonM(stage["$addFields"])
			if !ok {
				return nil, fmt.Errorf("$addFields stage must be a document")
			}
			var err error
			out, err = applyAddFields(out, spec)
			if err != nil {
				return nil, err
			}
		case stage["$unset"] != nil:
			paths, err := parseUnsetStage(stage["$unset"])
			if err != nil {
				return nil, err
			}
			out = applyUnsetStage(out, paths)
		case stage["$set"] != nil:
			// MongoDB treats $set as an alias for $addFields in aggregation pipelines.
			spec, ok := coerceBsonM(stage["$set"])
			if !ok {
				return nil, fmt.Errorf("$set stage must be a document")
			}
			var err error
			out, err = applyAddFields(out, spec)
			if err != nil {
				return nil, err
			}
		case stage["$lookup"] != nil:
			if resolve == nil {
				return nil, fmt.Errorf("$lookup requires resolver")
			}
			spec, ok := coerceBsonM(stage["$lookup"])
			if !ok {
				return nil, fmt.Errorf("$lookup stage must be a document")
			}
			var err error
			out, err = applyLookup(out, spec, resolve)
			if err != nil {
				return nil, err
			}
		case stage["$unwind"] != nil:
			path, preserve, err := parseUnwindStage(stage["$unwind"])
			if err != nil {
				return nil, err
			}
			out, err = applyUnwind(out, path, preserve)
			if err != nil {
				return nil, err
			}
		case stage["$group"] != nil:
			g, ok := stage["$group"].(bson.M)
			if !ok {
				return nil, fmt.Errorf("$group stage must be a document")
			}
			var err error
			out, err = applyGroup(out, g)
			if err != nil {
				return nil, err
			}
		case stage["$count"] != nil:
			field, ok := stage["$count"].(string)
			if !ok || field == "" {
				return nil, fmt.Errorf("$count stage must be a non-empty string")
			}
			out = []bson.M{{field: int64(len(out))}}
		case stage["$sort"] != nil:
			s, ok := stage["$sort"].(bson.M)
			if !ok {
				return nil, fmt.Errorf("$sort stage must be a document")
			}
			out = applySort(out, s)
		case stage["$limit"] != nil:
			lim, err := asInt(stage["$limit"])
			if err != nil {
				return nil, fmt.Errorf("$limit stage must be an integer")
			}
			if lim < 0 {
				lim = 0
			}
			if lim < len(out) {
				out = out[:lim]
			}
		case stage["$sample"] != nil:
			spec, ok := coerceBsonM(stage["$sample"])
			if !ok {
				return nil, fmt.Errorf("$sample stage must be a document")
			}
			size, err := parseSampleSize(spec)
			if err != nil {
				return nil, err
			}
			out = applySample(out, size)
		default:
			return nil, fmt.Errorf("unsupported aggregation stage: %v", stage)
		}
	}
	for _, d := range out {
		delete(d, mogVectorSearchScoreKey)
	}
	return out, nil
}

func applyVectorSearch(docs []bson.M, spec bson.M) ([]bson.M, error) {
	path, _ := spec["path"].(string)
	if path == "" {
		return nil, fmt.Errorf("$vectorSearch requires non-empty path")
	}
	rawQuery, ok := spec["queryVector"]
	if !ok || rawQuery == nil {
		return nil, fmt.Errorf("$vectorSearch requires queryVector")
	}
	rawArr, ok := coerceInterfaceSlice(rawQuery)
	if !ok || len(rawArr) == 0 {
		return nil, fmt.Errorf("$vectorSearch queryVector must be a non-empty array")
	}
	query, ok := coerceFloat64Slice(rawArr)
	if !ok {
		return nil, fmt.Errorf("$vectorSearch queryVector must be numeric")
	}
	limit := 0
	if rawLim, ok := spec["limit"]; ok {
		if n, err := asInt(rawLim); err == nil {
			limit = n
		}
	}
	if limit <= 0 {
		limit = 10
	}

	qnorm := l2Norm(query)
	if qnorm == 0 {
		return []bson.M{}, nil
	}

	type scored struct {
		doc   bson.M
		score float64
	}
	scoredDocs := make([]scored, 0, len(docs))
	for _, d := range docs {
		rawV := getPathValue(d, path)
		arr, ok := coerceInterfaceSlice(rawV)
		if !ok {
			continue
		}
		vec, ok := coerceFloat64Slice(arr)
		if !ok || len(vec) != len(query) {
			continue
		}
		vnorm := l2Norm(vec)
		if vnorm == 0 {
			continue
		}
		dot := 0.0
		for i := range vec {
			dot += vec[i] * query[i]
		}
		score := dot / (vnorm * qnorm)

		nd := bson.M{}
		for k, vv := range d {
			nd[k] = vv
		}
		nd[mogVectorSearchScoreKey] = score
		scoredDocs = append(scoredDocs, scored{doc: nd, score: score})
	}

	sort.SliceStable(scoredDocs, func(i, j int) bool {
		return scoredDocs[i].score > scoredDocs[j].score
	})
	if limit > len(scoredDocs) {
		limit = len(scoredDocs)
	}
	out := make([]bson.M, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, scoredDocs[i].doc)
	}
	return out, nil
}

func l2Norm(v []float64) float64 {
	sum := 0.0
	for _, x := range v {
		sum += x * x
	}
	return math.Sqrt(sum)
}

func parseUnsetStage(v interface{}) ([]string, error) {
	switch t := v.(type) {
	case string:
		p := strings.TrimPrefix(t, "$")
		if p == "" {
			return nil, fmt.Errorf("$unset stage must be a non-empty string or array of strings")
		}
		return []string{p}, nil
	case []string:
		out := make([]string, 0, len(t))
		for _, raw := range t {
			p := strings.TrimPrefix(raw, "$")
			if p == "" {
				return nil, fmt.Errorf("$unset stage paths cannot be empty")
			}
			out = append(out, p)
		}
		return out, nil
	case bson.M:
		// For compatibility, allow update-like syntax: {"$unset": {"a": "", "b.c": 1}}
		out := make([]string, 0, len(t))
		for k := range t {
			p := strings.TrimPrefix(k, "$")
			if p == "" {
				return nil, fmt.Errorf("$unset stage paths cannot be empty")
			}
			out = append(out, p)
		}
		sort.Strings(out)
		return out, nil
	default:
		arr, ok := coerceInterfaceSlice(v)
		if !ok {
			return nil, fmt.Errorf("$unset stage must be a string or array of strings")
		}
		out := make([]string, 0, len(arr))
		for _, el := range arr {
			s, ok := el.(string)
			if !ok {
				return nil, fmt.Errorf("$unset stage must be a string or array of strings")
			}
			p := strings.TrimPrefix(s, "$")
			if p == "" {
				return nil, fmt.Errorf("$unset stage paths cannot be empty")
			}
			out = append(out, p)
		}
		return out, nil
	}
}

func applyUnsetStage(docs []bson.M, paths []string) []bson.M {
	if len(paths) == 0 {
		return docs
	}
	out := make([]bson.M, 0, len(docs))
	for _, d := range docs {
		nd := deepCloneDoc(d)
		for _, p := range paths {
			unsetPathValue(nd, p)
		}
		out = append(out, nd)
	}
	return out
}

func parseSampleSize(spec bson.M) (int, error) {
	raw, ok := spec["size"]
	if !ok {
		return 0, fmt.Errorf("$sample.size is required")
	}
	n, err := asInt(raw)
	if err != nil {
		return 0, fmt.Errorf("$sample.size must be an integer")
	}
	if n < 0 {
		n = 0
	}
	return n, nil
}

func applySample(docs []bson.M, size int) []bson.M {
	if size <= 0 || len(docs) == 0 {
		return nil
	}
	if size >= len(docs) {
		// Return a shuffled copy.
		out := make([]bson.M, len(docs))
		copy(out, docs)
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := len(out) - 1; i > 0; i-- {
			j := rng.Intn(i + 1)
			out[i], out[j] = out[j], out[i]
		}
		return out
	}

	// Reservoir sampling: O(n) time, O(k) memory.
	out := make([]bson.M, 0, size)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i, d := range docs {
		if i < size {
			out = append(out, d)
			continue
		}
		j := rng.Intn(i + 1)
		if j < size {
			out[j] = d
		}
	}
	// Shuffle the reservoir for better randomness of output order.
	for i := len(out) - 1; i > 0; i-- {
		j := rng.Intn(i + 1)
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func parseUnwindStage(v interface{}) (path string, preserve bool, err error) {
	switch t := v.(type) {
	case string:
		path = t
	default:
		m, ok := coerceBsonM(v)
		if !ok {
			return "", false, fmt.Errorf("$unwind stage must be a string or document")
		}
		p, _ := m["path"].(string)
		if p == "" {
			return "", false, fmt.Errorf("$unwind.path must be a string")
		}
		path = p
		if b, ok := m["preserveNullAndEmptyArrays"].(bool); ok {
			preserve = b
		}
	}

	path = strings.TrimPrefix(path, "$")
	if path == "" {
		return "", false, fmt.Errorf("$unwind path cannot be empty")
	}
	return path, preserve, nil
}

func applyUnwind(docs []bson.M, path string, preserve bool) ([]bson.M, error) {
	var out []bson.M
	for _, d := range docs {
		val := getPathValue(d, path)

		if val == nil {
			if preserve {
				nd := deepCloneDoc(d)
				setPathValue(nd, path, nil)
				out = append(out, nd)
			}
			continue
		}

		if arr, ok := coerceInterfaceSlice(val); ok {
			if len(arr) == 0 {
				if preserve {
					nd := deepCloneDoc(d)
					setPathValue(nd, path, nil)
					out = append(out, nd)
				}
				continue
			}

			for _, el := range arr {
				nd := deepCloneDoc(d)
				setPathValue(nd, path, deepClone(el))
				out = append(out, nd)
			}
			continue
		}

		// Scalar/object values: Mongo keeps the doc (treat as single element).
		nd := deepCloneDoc(d)
		setPathValue(nd, path, deepClone(val))
		out = append(out, nd)
	}
	return out, nil
}

func deepCloneDoc(doc bson.M) bson.M {
	out := bson.M{}
	for k, v := range doc {
		out[k] = deepClone(v)
	}
	return out
}

func deepClone(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case bson.M:
		m := bson.M{}
		for k, vv := range t {
			m[k] = deepClone(vv)
		}
		return m
	case map[string]interface{}:
		m := bson.M{}
		for k, vv := range t {
			m[k] = deepClone(vv)
		}
		return m
	case bson.D:
		m := bson.M{}
		for _, e := range t {
			m[e.Name] = deepClone(e.Value)
		}
		return m
	case []interface{}:
		out := make([]interface{}, len(t))
		for i := range t {
			out[i] = deepClone(t[i])
		}
		return out
	case []bson.M:
		out := make([]bson.M, len(t))
		for i := range t {
			out[i] = deepCloneDoc(t[i])
		}
		return out
	case []string:
		out := make([]string, len(t))
		copy(out, t)
		return out
	default:
		rv := reflect.ValueOf(v)
		if rv.IsValid() && rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() != reflect.Uint8 {
			n := reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
			elemType := rv.Type().Elem()
			for i := 0; i < rv.Len(); i++ {
				origEl := rv.Index(i)
				cloned := deepClone(origEl.Interface())
				cv := reflect.ValueOf(cloned)

				if !cv.IsValid() {
					if elemType.Kind() == reflect.Interface {
						n.Index(i).Set(reflect.Zero(elemType))
						continue
					}
					n.Index(i).Set(origEl)
					continue
				}

				if cv.Type().AssignableTo(elemType) {
					n.Index(i).Set(cv)
					continue
				}
				if cv.Type().ConvertibleTo(elemType) {
					n.Index(i).Set(cv.Convert(elemType))
					continue
				}
				n.Index(i).Set(origEl)
			}
			return n.Interface()
		}
		return v
	}
}

func applyLookup(docs []bson.M, spec bson.M, resolve lookupResolver) ([]bson.M, error) {
	from, _ := spec["from"].(string)
	localField, _ := spec["localField"].(string)
	foreignField, _ := spec["foreignField"].(string)
	as, _ := spec["as"].(string)
	if from == "" || localField == "" || foreignField == "" || as == "" {
		return nil, fmt.Errorf("$lookup requires from/localField/foreignField/as")
	}

	foreignDocs, err := resolve(from)
	if err != nil {
		return nil, err
	}

	out := make([]bson.M, 0, len(docs))
	for _, d := range docs {
		// Don't mutate original documents.
		nd := bson.M{}
		for k, v := range d {
			nd[k] = v
		}

		localVal := getPathValue(d, localField)
		var matches []bson.M
		for _, fd := range foreignDocs {
			foreignVal := getPathValue(fd, foreignField)
			if lookupValuesEqual(localVal, foreignVal) {
				matches = append(matches, fd)
			}
		}
		if matches == nil {
			matches = []bson.M{}
		}
		nd[as] = matches
		out = append(out, nd)
	}
	return out, nil
}

func getPathValue(doc bson.M, path string) interface{} {
	if path == "" {
		return nil
	}
	cur := interface{}(doc)
	parts := strings.Split(path, ".")
	for _, p := range parts {
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

func cloneDocForSetPath(doc bson.M, path string) bson.M {
	nd := bson.M{}
	for k, v := range doc {
		nd[k] = v
	}
	// Ensure we don't mutate shared nested maps on the path by cloning the chain.
	parts := strings.Split(path, ".")
	if len(parts) < 2 {
		return nd
	}
	cur := nd
	for _, p := range parts[:len(parts)-1] {
		child, exists := cur[p]
		if !exists || child == nil {
			nm := bson.M{}
			cur[p] = nm
			cur = nm
			continue
		}
		m, ok := coerceBsonM(child)
		if !ok {
			nm := bson.M{}
			cur[p] = nm
			cur = nm
			continue
		}
		cm := bson.M{}
		for kk, vv := range m {
			cm[kk] = vv
		}
		cur[p] = cm
		cur = cm
	}
	return nd
}

func setPathValue(doc bson.M, path string, value interface{}) {
	if path == "" {
		return
	}
	parts := strings.Split(path, ".")
	if len(parts) == 1 {
		doc[parts[0]] = value
		return
	}

	cur := doc
	for _, p := range parts[:len(parts)-1] {
		child, exists := cur[p]
		if !exists || child == nil {
			nm := bson.M{}
			cur[p] = nm
			cur = nm
			continue
		}
		m, ok := coerceBsonM(child)
		if !ok {
			nm := bson.M{}
			cur[p] = nm
			cur = nm
			continue
		}
		// If the existing value is not a bson.M, coerceBsonM made a new map; ensure we keep it.
		if bm, ok := child.(bson.M); ok {
			cur = bm
		} else {
			nm := bson.M{}
			for kk, vv := range m {
				nm[kk] = vv
			}
			cur[p] = nm
			cur = nm
		}
	}
	cur[parts[len(parts)-1]] = value
}

func lookupValuesEqual(a, b interface{}) bool {
	// Treat missing localField as null (nil). Missing foreignField also becomes nil, so they match.
	if a == nil && b == nil {
		return true
	}

	// Array semantics: if either side is an array, match if any element matches.
	if aa, ok := coerceInterfaceSlice(a); ok {
		for _, el := range aa {
			if lookupValuesEqual(el, b) {
				return true
			}
		}
		return false
	}
	if bb, ok := coerceInterfaceSlice(b); ok {
		for _, el := range bb {
			if lookupValuesEqual(a, el) {
				return true
			}
		}
		return false
	}

	// Numeric normalization: allow int32/int64/float64 to compare equal when integral.
	if ai, aok := toInt64IfIntegral(a); aok {
		if bi, bok := toInt64IfIntegral(b); bok {
			return ai == bi
		}
	}
	if af, aok := toFloat64(a); aok {
		if bf, bok := toFloat64(b); bok {
			return af == bf
		}
	}

	// BSON ObjectId normalization.
	if aoid, ok := a.(bson.ObjectId); ok {
		a = aoid.Hex()
	}
	if boid, ok := b.(bson.ObjectId); ok {
		b = boid.Hex()
	}

	// Fallback.
	return reflect.DeepEqual(a, b) || fmt.Sprint(a) == fmt.Sprint(b)
}

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
			val, err := evalExpr(d, expr)
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
	// Field reference: "$field.subfield"
	if s, ok := expr.(string); ok {
		if len(s) > 1 && s[0] == '$' {
			return getPathValue(doc, strings.TrimPrefix(s, "$")), nil
		}
		return s, nil
	}

	// Operator expression (for now): { "$size": <expr> }
	if m, ok := coerceBsonM(expr); ok {
		// Treat a single-key document whose key starts with "$" as an operator expression.
		if len(m) == 1 {
			for op, arg := range m {
				if strings.HasPrefix(op, "$") {
					switch op {
					case "$cond":
						// Form 1: { $cond: [ <if>, <then>, <else> ] }
						if arr, ok := arg.([]interface{}); ok {
							if len(arr) != 3 {
								return nil, fmt.Errorf("$cond must be a 3-arg array")
							}
							condVal, err := evalAddFieldsValue(doc, arr[0])
							if err != nil {
								return nil, err
							}
							if isTruthy(condVal) {
								return evalAddFieldsValue(doc, arr[1])
							}
							return evalAddFieldsValue(doc, arr[2])
						}

						// Form 2: { $cond: { if: <expr>, then: <expr>, else: <expr> } }
						if spec, ok := coerceBsonM(arg); ok {
							ifExpr, hasIf := spec["if"]
							thenExpr, hasThen := spec["then"]
							elseExpr, hasElse := spec["else"]
							if !hasIf || !hasThen || !hasElse {
								return nil, fmt.Errorf("$cond object form requires if/then/else")
							}
							condVal, err := evalAddFieldsValue(doc, ifExpr)
							if err != nil {
								return nil, err
							}
							if isTruthy(condVal) {
								return evalAddFieldsValue(doc, thenExpr)
							}
							return evalAddFieldsValue(doc, elseExpr)
						}

						return nil, fmt.Errorf("$cond must be an array or document")
					case "$isArray":
						v, err := evalAddFieldsValue(doc, arg)
						if err != nil {
							return nil, err
						}
						return isArrayValue(v), nil
					case "$size":
						v, err := evalAddFieldsValue(doc, arg)
						if err != nil {
							return nil, err
						}
						if v == nil {
							return nil, nil
						}
						rv := reflect.ValueOf(v)
						if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
							return int64(rv.Len()), nil
						}
						return nil, nil
					default:
						return nil, fmt.Errorf("unsupported $addFields expression: %v", m)
					}
				}
			}
		}
		// Literal document (constant).
		return m, nil
	}

	// Constant (number/bool/null/array/etc).
	return expr, nil
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

func evalExpr(doc bson.M, expr bson.M) (interface{}, error) {
	if metaArg, ok := expr["$meta"]; ok {
		if s, ok := metaArg.(string); ok {
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
	}
	if sizeArg, ok := expr["$size"]; ok {
		val, err := evalValue(doc, sizeArg)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return int64(0), nil
		}

		rv := reflect.ValueOf(val)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
			// Don't treat raw bytes as an array for $size.
			if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
				return int64(0), nil
			}
			return int64(rv.Len()), nil
		}
		// Safe mode: non-array => 0.
		return int64(0), nil
	}
	if mul, ok := expr["$multiply"]; ok {
		args, ok := mul.([]interface{})
		if !ok || len(args) != 2 {
			return nil, fmt.Errorf("$multiply must be a 2-arg array")
		}

		a, err := evalValue(doc, args[0])
		if err != nil {
			return nil, err
		}
		b, err := evalValue(doc, args[1])
		if err != nil {
			return nil, err
		}
		af, ok := toFloat64(a)
		if !ok {
			return nil, fmt.Errorf("$multiply arg is not numeric: %v", a)
		}
		bf, ok := toFloat64(b)
		if !ok {
			return nil, fmt.Errorf("$multiply arg is not numeric: %v", b)
		}
		return af * bf, nil
	}
	return nil, fmt.Errorf("unsupported computed expression: %v", expr)
}

func evalValue(doc bson.M, v interface{}) (interface{}, error) {
	if s, ok := v.(string); ok && len(s) > 1 && s[0] == '$' {
		f := s[1:]
		return getPathValue(doc, f), nil
	}
	return v, nil
}

type groupState struct {
	id             interface{}
	sum            map[string]float64
	count          map[string]int64
	sumOnlyFloat   map[string]float64
	sumOnlyInt     map[string]int64
	sumOnlyIsFloat map[string]bool
	sumInt         map[string]int64
	addToSetKeys   map[string]map[string]struct{}
	addToSetVals   map[string][]interface{}
}

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
