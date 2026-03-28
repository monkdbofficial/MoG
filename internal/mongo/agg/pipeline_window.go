package agg

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/mgo.v2/bson"
)

// $setWindowFields stage implementation.
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
