package mongo

import (
	"fmt"
	"strings"

	"mog/internal/mongo/handler/sql"
)

func sqlTypeForValue(v interface{}) string { return sql.TypeForValue(v) }

func floatVectorLiteral(v interface{}) (string, int, bool) { return sql.FloatVectorLiteral(v) }

func arrayArgForSQLType(v interface{}, sqlType string) (interface{}, error) { return sql.ArrayArgForSQLType(v, sqlType) }

func sqlTypeForField(field string, v interface{}) string {
	typ := sql.TypeForValue(v)
	// FLOAT_VECTOR is reserved for "vector-like" fields (e.g. embeddings).
	// Numeric arrays that aren't vectors should be stored as ARRAY(<numeric>).
	if looksLikeVectorField(field) && strings.HasPrefix(typ, "ARRAY(") {
		if _, n, ok := sql.FloatVectorLiteral(v); ok && n > 0 && n <= 2048 {
			return fmt.Sprintf("FLOAT_VECTOR(%d)", n)
		}
	}
	return typ
}

func looksLikeVectorField(field string) bool {
	if field == "" {
		return false
	}
	f := strings.ToLower(field)
	if strings.Contains(f, "vector") || strings.Contains(f, "embedding") {
		return true
	}
	return false
}

func isUndefinedColumn(err error) bool     { return sql.IsUndefinedColumn(err) }
func isUndefinedRelation(err error) bool   { return sql.IsUndefinedRelation(err) }
func isUndefinedSchema(err error) bool     { return sql.IsUndefinedSchema(err) }
func isDuplicateColumn(err error) bool     { return sql.IsDuplicateColumn(err) }
func isDuplicateObject(err error) bool     { return sql.IsDuplicateObject(err) }
func isUniqueViolation(err error) bool     { return sql.IsUniqueViolation(err) }
func isDuplicateColumnName(err error) bool { return sql.IsDuplicateColumnName(err) }
