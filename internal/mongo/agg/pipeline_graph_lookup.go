package agg

import (
	"fmt"

	"gopkg.in/mgo.v2/bson"
)

// $graphLookup stage implementation.
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
