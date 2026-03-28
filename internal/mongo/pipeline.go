package mongo

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"regexp"
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
		case stage["$graphLookup"] != nil:
			if resolve == nil {
				return nil, fmt.Errorf("$graphLookup requires resolver")
			}
			spec, ok := coerceBsonM(stage["$graphLookup"])
			if !ok {
				return nil, fmt.Errorf("$graphLookup stage must be a document")
			}
			var err error
			out, err = applyGraphLookup(out, spec, resolve)
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
		case stage["$facet"] != nil:
			spec, ok := coerceBsonM(stage["$facet"])
			if !ok {
				return nil, fmt.Errorf("$facet stage must be a document")
			}
			var err error
			out, err = applyFacet(out, spec, resolve)
			if err != nil {
				return nil, err
			}
		case stage["$setWindowFields"] != nil:
			spec, ok := coerceBsonM(stage["$setWindowFields"])
			if !ok {
				return nil, fmt.Errorf("$setWindowFields stage must be a document")
			}
			var err error
			out, err = applySetWindowFields(out, spec)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported aggregation stage: %v", stage)
		}
	}
	for _, d := range out {
		delete(d, mogVectorSearchScoreKey)
	}
	return out, nil
}

func applyFacet(docs []bson.M, spec bson.M, resolve lookupResolver) ([]bson.M, error) {
	out := bson.M{}
	for facetName, rawPipeline := range spec {
		arr, ok := coerceInterfaceSlice(rawPipeline)
		if !ok {
			return nil, fmt.Errorf("$facet %q pipeline must be an array", facetName)
		}
		pipeline := make([]bson.M, 0, len(arr))
		for _, rawStage := range arr {
			stageDoc, ok := coerceBsonM(rawStage)
			if !ok {
				return nil, fmt.Errorf("$facet %q stage must be a document", facetName)
			}
			pipeline = append(pipeline, stageDoc)
		}
		facetOut, err := applyPipelineWithLookup(cloneDocsShallow(docs), pipeline, resolve)
		if err != nil {
			return nil, err
		}
		out[facetName] = facetOut
	}
	return []bson.M{out}, nil
}

func applyGraphLookup(docs []bson.M, spec bson.M, resolve lookupResolver) ([]bson.M, error) {
	from, _ := spec["from"].(string)
	as, _ := spec["as"].(string)
	connectFromField, _ := spec["connectFromField"].(string)
	connectToField, _ := spec["connectToField"].(string)
	startWith := spec["startWith"]

	if from == "" || as == "" || connectFromField == "" || connectToField == "" {
		return nil, fmt.Errorf("$graphLookup requires from/as/connectFromField/connectToField")
	}
	if startWith == nil {
		return nil, fmt.Errorf("$graphLookup requires startWith")
	}

	maxDepth := 0
	if v, ok := spec["maxDepth"]; ok && v != nil {
		if n, err := asInt(v); err == nil && n >= 0 {
			maxDepth = n
		}
	}

	foreign, err := resolve(from)
	if err != nil {
		return nil, err
	}

	out := make([]bson.M, 0, len(docs))
	for _, d := range docs {
		nd := bson.M{}
		for k, v := range d {
			nd[k] = v
		}

		startVal, err := evalValue(d, startWith)
		if err != nil {
			return nil, err
		}
		starts := []interface{}{}
		if arr, ok := coerceInterfaceSlice(startVal); ok {
			starts = append(starts, arr...)
		} else {
			starts = append(starts, startVal)
		}

		type node struct {
			val   interface{}
			depth int
		}
		q := make([]node, 0, len(starts))
		seen := map[string]struct{}{}
		for _, s := range starts {
			k := fmt.Sprintf("%T:%v", s, s)
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			q = append(q, node{val: s, depth: 0})
		}

		results := make([]bson.M, 0)
		for len(q) > 0 {
			cur := q[0]
			q = q[1:]
			if cur.depth > maxDepth {
				continue
			}

			// Find matching foreign docs where connectToField == cur.val
			for _, fd := range foreign {
				toVal := getPathValue(fd, connectToField)
				if fmt.Sprint(toVal) != fmt.Sprint(cur.val) {
					continue
				}
				results = append(results, fd)

				// Expand using connectFromField.
				next := getPathValue(fd, connectFromField)
				nextVals := []interface{}{}
				if arr, ok := coerceInterfaceSlice(next); ok {
					nextVals = append(nextVals, arr...)
				} else {
					nextVals = append(nextVals, next)
				}
				for _, nv := range nextVals {
					if nv == nil {
						continue
					}
					k := fmt.Sprintf("%T:%v", nv, nv)
					if _, ok := seen[k]; ok {
						continue
					}
					seen[k] = struct{}{}
					q = append(q, node{val: nv, depth: cur.depth + 1})
				}
			}
		}

		nd[as] = results
		out = append(out, nd)
	}
	return out, nil
}

func cloneDocsShallow(docs []bson.M) []bson.M {
	out := make([]bson.M, 0, len(docs))
	for _, d := range docs {
		nd := bson.M{}
		for k, v := range d {
			nd[k] = v
		}
		out = append(out, nd)
	}
	return out
}

func applySetWindowFields(docs []bson.M, spec bson.M) ([]bson.M, error) {
	// This is intentionally a minimal, compatibility-focused implementation.
	// Supported (common) subset:
	// - partitionBy: field reference (e.g. "$age") or omitted
	// - sortBy: single field with 1/-1
	// - output:
	//   - $avg: "$field" with window documents ["unbounded","current"] (or omitted => treated the same)
	//   - $rank / $denseRank / $documentNumber: {}
	//   - $shift: { output: <expr>, by: <int>, default: <expr> }

	var partitionBy interface{} = nil
	if v, ok := spec["partitionBy"]; ok {
		partitionBy = v
	}

	sortBy, ok := coerceBsonM(spec["sortBy"])
	if !ok || len(sortBy) == 0 {
		return nil, fmt.Errorf("$setWindowFields requires sortBy")
	}
	var sortField string
	sortDir := 1
	for k, v := range sortBy {
		sortField = k
		if dir, err := asInt(v); err == nil && dir != 0 {
			sortDir = dir
		}
		break
	}
	if sortField == "" {
		return nil, fmt.Errorf("$setWindowFields requires a non-empty sortBy field")
	}

	outputSpec, ok := coerceBsonM(spec["output"])
	if !ok || len(outputSpec) == 0 {
		return nil, fmt.Errorf("$setWindowFields requires output")
	}

	type outOp struct {
		outField string
		kind     string // "avg", "rank", "denseRank", "documentNumber", "shift"
		arg      interface{}
		by       int
		def      interface{}
	}
	ops := make([]outOp, 0, len(outputSpec))
	for outField, raw := range outputSpec {
		m, ok := coerceBsonM(raw)
		if !ok {
			return nil, fmt.Errorf("$setWindowFields output %q must be a document", outField)
		}
		if avgArg, ok := m["$avg"]; ok {
			// Best-effort validate window shape if provided.
			if w, ok := coerceBsonM(m["window"]); ok {
				if docsSpec, ok := w["documents"]; ok {
					if arr, ok := coerceInterfaceSlice(docsSpec); ok && len(arr) == 2 {
						// accept "unbounded"/"current" (case-insensitive) only
						a := strings.ToLower(fmt.Sprint(arr[0]))
						b := strings.ToLower(fmt.Sprint(arr[1]))
						if a != "unbounded" || b != "current" {
							return nil, fmt.Errorf("$setWindowFields only supports window documents [unbounded,current]")
						}
					} else {
						return nil, fmt.Errorf("$setWindowFields window.documents must be [unbounded,current]")
					}
				}
			}
			ops = append(ops, outOp{outField: outField, kind: "avg", arg: avgArg})
			continue
		}
		if _, ok := m["$rank"]; ok {
			ops = append(ops, outOp{outField: outField, kind: "rank"})
			continue
		}
		if _, ok := m["$denseRank"]; ok {
			ops = append(ops, outOp{outField: outField, kind: "denseRank"})
			continue
		}
		if _, ok := m["$documentNumber"]; ok {
			ops = append(ops, outOp{outField: outField, kind: "documentNumber"})
			continue
		}
		if shiftRaw, ok := m["$shift"]; ok {
			shiftSpec, ok := coerceBsonM(shiftRaw)
			if !ok {
				return nil, fmt.Errorf("$shift must be a document")
			}
			outExpr, ok := shiftSpec["output"]
			if !ok {
				return nil, fmt.Errorf("$shift requires output")
			}
			by := 0
			if rawBy, ok := shiftSpec["by"]; ok {
				if n, err := asInt(rawBy); err == nil {
					by = n
				}
			}
			def := shiftSpec["default"]
			ops = append(ops, outOp{outField: outField, kind: "shift", arg: outExpr, by: by, def: def})
			continue
		}
		return nil, fmt.Errorf("unsupported $setWindowFields output operator: %v", m)
	}

	// Output retains input order; we compute window results by sorting indices per partition.
	out := cloneDocsShallow(docs)

	partitions := map[string][]int{}
	for i, d := range docs {
		var key interface{} = nil
		if partitionBy != nil {
			v, err := evalValue(d, partitionBy)
			if err != nil {
				return nil, err
			}
			key = v
		}
		k := fmt.Sprintf("%T:%v", key, key)
		partitions[k] = append(partitions[k], i)
	}

	for _, idxs := range partitions {
		// Sort partition indices by sortBy.
		sort.SliceStable(idxs, func(i, j int) bool {
			a := docs[idxs[i]]
			b := docs[idxs[j]]
			av := getPathValue(a, sortField)
			bv := getPathValue(b, sortField)
			if af, ok := toFloat64(av); ok {
				if bf, ok := toFloat64(bv); ok {
					if af == bf {
						return false
					}
					if sortDir < 0 {
						return af > bf
					}
					return af < bf
				}
			}
			as := fmt.Sprint(av)
			bs := fmt.Sprint(bv)
			if as == bs {
				return false
			}
			if sortDir < 0 {
				return as > bs
			}
			return as < bs
		})

		// Pre-compute ranks/numbers for this partition if needed.
		needRank := false
		needDense := false
		needDocNum := false
		for _, op := range ops {
			if op.kind == "rank" {
				needRank = true
			}
			if op.kind == "denseRank" {
				needDense = true
			}
			if op.kind == "documentNumber" {
				needDocNum = true
			}
		}

		ranks := map[int]int64{}
		denseRanks := map[int]int64{}
		docNums := map[int]int64{}
		if needRank || needDense || needDocNum {
			var prev interface{} = nil
			var havePrev bool
			var rank int64
			var dense int64
			for pos, idx := range idxs {
				cur := getPathValue(docs[idx], sortField)
				changed := !havePrev || fmt.Sprint(cur) != fmt.Sprint(prev)
				if changed {
					rank = int64(pos + 1) // with gaps
					dense++               // without gaps
					prev = cur
					havePrev = true
				}
				if needRank {
					ranks[idx] = rank
				}
				if needDense {
					denseRanks[idx] = dense
				}
				if needDocNum {
					docNums[idx] = int64(pos + 1)
				}
			}
		}

		// Compute per-op window outputs in sorted order.
		stageOpts := evalOpts{sizeNonArrayZero: false, vars: nil}
		for _, op := range ops {
			switch op.kind {
			case "rank":
				for _, idx := range idxs {
					out[idx][op.outField] = ranks[idx]
				}
			case "denseRank":
				for _, idx := range idxs {
					out[idx][op.outField] = denseRanks[idx]
				}
			case "documentNumber":
				for _, idx := range idxs {
					out[idx][op.outField] = docNums[idx]
				}
			case "avg":
				sum := 0.0
				n := int64(0)
				for _, idx := range idxs {
					val, err := evalComputedWithOpts(docs[idx], op.arg, stageOpts)
					if err != nil {
						return nil, err
					}
					if f, ok := toFloat64(val); ok {
						sum += f
						n++
					}
					if n == 0 {
						out[idx][op.outField] = nil
					} else {
						out[idx][op.outField] = sum / float64(n)
					}
				}
			case "shift":
				for pos, idx := range idxs {
					target := pos + op.by
					if target < 0 || target >= len(idxs) {
						if op.def == nil {
							out[idx][op.outField] = nil
							continue
						}
						v, err := evalComputedWithOpts(docs[idx], op.def, stageOpts)
						if err != nil {
							return nil, err
						}
						out[idx][op.outField] = v
						continue
					}
					shiftedDoc := docs[idxs[target]]
					v, err := evalComputedWithOpts(shiftedDoc, op.arg, stageOpts)
					if err != nil {
						return nil, err
					}
					out[idx][op.outField] = v
				}
			}
		}
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
			val, err := evalComputedWithOpts(d, expr, evalOpts{sizeNonArrayZero: true})
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
	return evalComputedWithOpts(doc, expr, evalOpts{sizeNonArrayZero: false})
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
	if s, ok := v.(string); ok && len(s) > 1 && s[0] == '$' {
		f := s[1:]
		return getPathValue(doc, f), nil
	}
	return v, nil
}

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
					sum := 0.0
					for _, a := range args {
						if a == nil {
							return nil, nil
						}
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
					a, ok := toFloat64(args[0])
					if !ok || args[0] == nil || args[1] == nil {
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
					amt, ok := toInt64IfIntegral(rawAmt)
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
				max:            map[string]interface{}{},
				min:            map[string]interface{}{},
				maxSet:         map[string]bool{},
				minSet:         map[string]bool{},
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
