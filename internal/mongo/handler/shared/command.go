package shared

import (
	"sort"

	"gopkg.in/mgo.v2/bson"
)

// PrimaryCommandKey is a helper used by the adapter.
func PrimaryCommandKey(cmd bson.M) string {
	// Return the "command name" key for logging purposes.
	// Skip metadata keys that frequently appear in command documents.
	skip := map[string]bool{
		"$db":             true,
		"lsid":            true,
		"$clusterTime":    true,
		"txnNumber":       true,
		"autocommit":      true,
		"$readPreference": true,
		"readConcern":     true,
		"writeConcern":    true,
		"client":          true,
	}

	var keys []string
	for k := range cmd {
		if skip[k] {
			continue
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return ""
	}
	sort.Strings(keys)
	return keys[0]
}

// CommandDB is a helper used by the adapter.
func CommandDB(cmd bson.M) string {
	if v, ok := cmd["$db"]; ok {
		if s, ok := AsString(v); ok {
			return s
		}
	}
	return ""
}

// AsString is a helper used by the adapter.
func AsString(v interface{}) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case bson.Symbol:
		return string(t), true
	default:
		return "", false
	}
}
