package pipeline

import (
	"fmt"
	"reflect"

	"gopkg.in/mgo.v2/bson"
)

// $lookup stage implementation.
func applyLookup(docs []bson.M, spec bson.M, resolve lookupResolver) ([]bson.M, error) {
	from, _ := spec["from"].(string)
	as, _ := spec["as"].(string)
	if from == "" || as == "" {
		return nil, fmt.Errorf("$lookup requires from/as")
	}

	// Pipeline form: {from, let?, pipeline, as}
	if rawPipe, hasPipe := spec["pipeline"]; hasPipe && rawPipe != nil {
		pipe, err := parsePipelineStages(rawPipe)
		if err != nil {
			return nil, err
		}
		if len(pipe) == 0 {
			return nil, fmt.Errorf("$lookup.pipeline must be a non-empty array")
		}

		var letSpec bson.M
		if rawLet, ok := spec["let"]; ok && rawLet != nil {
			m, ok := coerceBsonM(rawLet)
			if !ok {
				return nil, fmt.Errorf("$lookup.let must be a document")
			}
			letSpec = m
		}

		foreignDocs, err := resolve(from)
		if err != nil {
			return nil, err
		}

		out := make([]bson.M, 0, len(docs))
		for _, d := range docs {
			nd := bson.M{}
			for k, v := range d {
				nd[k] = v
			}

			letVars := map[string]interface{}{}
			if len(letSpec) > 0 {
				opts := evalOpts{sizeNonArrayZero: false, vars: map[string]interface{}{"ROOT": d, "CURRENT": d}}
				for name, raw := range letSpec {
					v, err := evalComputedWithOpts(d, raw, opts)
					if err != nil {
						return nil, err
					}
					letVars[name] = v
				}
			}

			matches, err := applyPipelineWithLookupVars(cloneDocsShallow(foreignDocs), pipe, resolve, letVars)
			if err != nil {
				return nil, err
			}
			if matches == nil {
				matches = []bson.M{}
			}
			nd[as] = matches
			out = append(out, nd)
		}
		return out, nil
	}

	// Simple form: {from, localField, foreignField, as}
	localField, _ := spec["localField"].(string)
	foreignField, _ := spec["foreignField"].(string)
	if localField == "" || foreignField == "" {
		return nil, fmt.Errorf("$lookup requires either localField/foreignField or pipeline")
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

func parsePipelineStages(raw interface{}) ([]bson.M, error) {
	arr, ok := coerceInterfaceSlice(raw)
	if !ok {
		return nil, fmt.Errorf("$lookup.pipeline must be an array")
	}
	out := make([]bson.M, 0, len(arr))
	for _, st := range arr {
		m, ok := coerceBsonM(st)
		if !ok {
			return nil, fmt.Errorf("$lookup.pipeline stage must be a document")
		}
		out = append(out, m)
	}
	return out, nil
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
