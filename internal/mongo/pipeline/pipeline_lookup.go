package pipeline

import (
	"fmt"
	"reflect"

	"gopkg.in/mgo.v2/bson"
)

// $lookup stage implementation.
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
