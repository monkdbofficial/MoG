package shared

import (
	"encoding/base32"
	"strings"
)

func IsSafeIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if r == '_' {
			continue
		}
		if strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", r) {
			continue
		}
		return false
	}
	return true
}

// IsInternalCollectionName reports whether name is an internal, system-managed
// collection that should not be surfaced to clients (Compass/mongosh).
//
// These may be created as implementation details (e.g. $graphLookup helpers).
func IsInternalCollectionName(name string) bool {
	if name == "" {
		return false
	}
	// $graphLookup helper tables are stored in the doc schema and can otherwise
	// show up as user collections when we derive catalog state from tables.
	if strings.Contains(name, "__graph_edges__") {
		return true
	}
	if strings.Contains(name, "__graph_vertices__") {
		return true
	}
	return false
}

var FieldB32 = base32.StdEncoding.WithPadding(base32.NoPadding)

func SQLColumnNameForField(field string) string {
	// NOTE: MongoDB field names can't contain '.' (nested paths are expressed in queries), but they can
	// start with '_' which conflicts with MonkDB system column patterns. We also reserve a few
	// internal columns (id/data). Everything else is deterministically mapped to a safe SQL identifier.
	if field == "" {
		return ""
	}
	if field == "_id" {
		return "id"
	}
	if IsSafeIdentifier(field) &&
		!strings.HasPrefix(field, "_") &&
		field != "id" &&
		field != "data" &&
		!strings.HasPrefix(field, "f_") {
		return field
	}
	enc := strings.ToLower(FieldB32.EncodeToString([]byte(field)))
	return "f_" + enc
}

func MongoFieldNameForColumn(col string) string {
	if col == "" {
		return ""
	}
	// CrateDB-style "object sub-columns" show up in information_schema and SELECT *
	// result sets as column names like `meta['path']` or `data['ts']['$date']`.
	// These are not real MongoDB field names and should never be surfaced back to
	// clients (Compass/mongosh), otherwise documents look duplicated/noisy.
	if strings.Contains(col, "['") {
		return ""
	}
	if col == "id" {
		return "_id"
	}
	if strings.HasPrefix(col, "f_") {
		dec, err := FieldB32.DecodeString(strings.ToUpper(strings.TrimPrefix(col, "f_")))
		if err == nil && len(dec) > 0 {
			return string(dec)
		}
	}
	return col
}
