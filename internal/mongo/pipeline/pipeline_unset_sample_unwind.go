package pipeline

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"
)

// parseUnsetStage parses a $unset stage spec into a list of field paths.
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

// applyUnsetStage is a helper used by the adapter.
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

// parseSampleSize is a helper used by the adapter.
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

// applySample is a helper used by the adapter.
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

// parseUnwindStage is a helper used by the adapter.
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

// applyUnwind is a helper used by the adapter.
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
