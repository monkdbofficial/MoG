package mongo

import (
	"encoding/base32"
	"strings"
)

func isSafeIdentifier(name string) bool {
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

var (
	fieldB32 = base32.StdEncoding.WithPadding(base32.NoPadding)
)

func sqlColumnNameForField(field string) string {
	// NOTE: MongoDB field names can't contain '.' (nested paths are expressed in queries), but they can
	// start with '_' which conflicts with MonkDB/Crate system column patterns. We also reserve a few
	// internal columns (id/data). Everything else is deterministically mapped to a safe SQL identifier.
	if field == "" {
		return ""
	}
	if field == "_id" {
		return "id"
	}
	if isSafeIdentifier(field) &&
		!strings.HasPrefix(field, "_") &&
		field != "id" &&
		field != "data" &&
		!strings.HasPrefix(field, "f_") {
		return field
	}
	enc := strings.ToLower(fieldB32.EncodeToString([]byte(field)))
	return "f_" + enc
}

func mongoFieldNameForColumn(col string) string {
	if col == "" {
		return ""
	}
	if col == "id" {
		return "_id"
	}
	if strings.HasPrefix(col, "f_") {
		dec, err := fieldB32.DecodeString(strings.ToUpper(strings.TrimPrefix(col, "f_")))
		if err == nil && len(dec) > 0 {
			return string(dec)
		}
	}
	return col
}
