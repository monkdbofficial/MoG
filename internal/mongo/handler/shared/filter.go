package shared

import "gopkg.in/mgo.v2/bson"

// FilterHasOperator is a helper used by the adapter.
func FilterHasOperator(filter bson.M, op string) bool {
	for _, v := range filter {
		m, ok := CoerceBsonM(v)
		if !ok {
			continue
		}
		if _, ok := m[op]; ok {
			return true
		}
	}
	return false
}

// StripOperatorKeys is a helper used by the adapter.
func StripOperatorKeys(filter bson.M, op string) bson.M {
	if len(filter) == 0 {
		return filter
	}
	out := bson.M{}
	for k, v := range filter {
		m, ok := CoerceBsonM(v)
		if ok {
			if _, has := m[op]; has {
				// If a field condition uses the operator (even alongside others), drop it from pushdown.
				continue
			}
		}
		out[k] = v
	}
	return out
}
