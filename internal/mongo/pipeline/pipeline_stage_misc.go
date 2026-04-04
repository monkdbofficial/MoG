package pipeline

import (
	"fmt"

	"gopkg.in/mgo.v2/bson"
)

// applyReplaceRoot evaluates a $replaceRoot stage.
func applyReplaceRoot(docs []bson.M, spec bson.M) ([]bson.M, error) {
	return applyReplaceRootVars(docs, spec, nil)
}

// applyReplaceRootVars is a helper used by the adapter.
func applyReplaceRootVars(docs []bson.M, spec bson.M, vars map[string]interface{}) ([]bson.M, error) {
	newRoot, ok := spec["newRoot"]
	if !ok {
		return nil, fmt.Errorf("$replaceRoot requires newRoot")
	}
	return applyReplaceWithVars(docs, newRoot, vars)
}

// applyReplaceWith is a helper used by the adapter.
func applyReplaceWith(docs []bson.M, expr interface{}) ([]bson.M, error) {
	return applyReplaceWithVars(docs, expr, nil)
}

// applyReplaceWithVars is a helper used by the adapter.
func applyReplaceWithVars(docs []bson.M, expr interface{}, vars map[string]interface{}) ([]bson.M, error) {
	out := make([]bson.M, 0, len(docs))
	for _, d := range docs {
		opts := evalOpts{sizeNonArrayZero: false, vars: stageVars(d, vars)}
		v, err := evalComputedWithOpts(d, expr, opts)
		if err != nil {
			return nil, err
		}
		m, ok := coerceBsonM(v)
		if !ok || m == nil {
			return nil, fmt.Errorf("$replaceWith must evaluate to a document")
		}
		nd := bson.M{}
		for k, vv := range m {
			nd[k] = vv
		}
		out = append(out, nd)
	}
	return out, nil
}

// applySortByCount is a helper used by the adapter.
func applySortByCount(docs []bson.M, expr interface{}) ([]bson.M, error) {
	return applySortByCountVars(docs, expr, nil)
}

// applySortByCountVars is a helper used by the adapter.
func applySortByCountVars(docs []bson.M, expr interface{}, vars map[string]interface{}) ([]bson.M, error) {
	type state struct {
		key interface{}
		n   int64
	}
	byKey := map[string]*state{}
	order := []string{}
	for _, d := range docs {
		opts := evalOpts{sizeNonArrayZero: false, vars: stageVars(d, vars)}
		k, err := evalComputedWithOpts(d, expr, opts)
		if err != nil {
			return nil, err
		}
		sk := fmt.Sprintf("%T:%v", k, k)
		st := byKey[sk]
		if st == nil {
			st = &state{key: k}
			byKey[sk] = st
			order = append(order, sk)
		}
		st.n++
	}
	out := make([]bson.M, 0, len(order))
	for _, sk := range order {
		st := byKey[sk]
		out = append(out, bson.M{"_id": st.key, "count": st.n})
	}
	out = applySort(out, bson.M{"count": -1})
	return out, nil
}

// applyUnionWith is a helper used by the adapter.
func applyUnionWith(docs []bson.M, raw interface{}, resolve lookupResolver) ([]bson.M, error) {
	coll := ""
	var pipeline []bson.M

	switch t := raw.(type) {
	case string:
		coll = t
	default:
		spec, ok := coerceBsonM(raw)
		if !ok {
			return nil, fmt.Errorf("$unionWith must be a string or document")
		}
		if c, ok := spec["coll"].(string); ok {
			coll = c
		} else if c, ok := spec["from"].(string); ok {
			coll = c
		}
		if rawPipe, ok := spec["pipeline"]; ok && rawPipe != nil {
			arr, ok := coerceInterfaceSlice(rawPipe)
			if !ok {
				return nil, fmt.Errorf("$unionWith.pipeline must be an array")
			}
			for _, st := range arr {
				m, ok := coerceBsonM(st)
				if !ok {
					return nil, fmt.Errorf("$unionWith.pipeline stage must be a document")
				}
				pipeline = append(pipeline, m)
			}
		}
	}
	if coll == "" {
		return nil, fmt.Errorf("$unionWith requires coll")
	}

	foreign, err := resolve(coll)
	if err != nil {
		return nil, err
	}
	unionDocs := foreign
	if len(pipeline) > 0 {
		unionDocs, err = applyPipelineWithLookup(cloneDocsShallow(foreign), pipeline, resolve)
		if err != nil {
			return nil, err
		}
	}
	out := make([]bson.M, 0, len(docs)+len(unionDocs))
	out = append(out, docs...)
	out = append(out, unionDocs...)
	return out, nil
}

// applyFacet is a helper used by the adapter.
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
