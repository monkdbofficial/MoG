package pipeline

import (
	"reflect"
	"strings"

	"gopkg.in/mgo.v2/bson"
)

// deepCloneDoc clones a BSON document recursively.
func deepCloneDoc(doc bson.M) bson.M {
	out := bson.M{}
	for k, v := range doc {
		out[k] = deepClone(v)
	}
	return out
}

// deepClone is a helper used by the adapter.
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

// getPathValue is a helper used by the adapter.
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

// cloneDocForSetPath is a helper used by the adapter.
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

// setPathValue is a helper used by the adapter.
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
