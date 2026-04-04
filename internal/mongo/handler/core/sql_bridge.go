package mongo

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"mog/internal/logging"
	mongopath "mog/internal/mongo"
	"mog/internal/mongo/handler/sql"

	"go.uber.org/zap"
)

// sqlTypeForValue is a helper used by the adapter.
func sqlTypeForValue(v interface{}) string { return sql.TypeForValue(v) }

// floatVectorLiteral is a helper used by the adapter.
func floatVectorLiteral(v interface{}) (string, int, bool) { return sql.FloatVectorLiteral(v) }

// arrayArgForSQLType is a helper used by the adapter.
func arrayArgForSQLType(v interface{}, sqlType string) (interface{}, error) {
	return sql.ArrayArgForSQLType(v, sqlType)
}

// sqlTypeForField is a helper used by the adapter.
func sqlTypeForField(field string, v interface{}) string {
	typ := sql.TypeForValue(v)
	// FLOAT_VECTOR is reserved for "vector-like" fields (e.g. embeddings).
	// Numeric arrays that aren't vectors should be stored as ARRAY(<numeric>).
	if looksLikeVectorField(field) && strings.HasPrefix(typ, "ARRAY(") {
		if _, n, ok := sql.FloatVectorLiteral(v); ok && n > 0 && n <= 2048 {
			return fmt.Sprintf("FLOAT_VECTOR(%d)%s", n, floatVectorSimilarityClause())
		}
	}
	return typ
}

const floatVectorSimilarityEnv = "MOG_FLOAT_VECTOR_SIMILARITY"

var (
	floatVectorSimilarityWarn sync.Once
)

// floatVectorSimilarityClause is a helper used by the adapter.
func floatVectorSimilarityClause() string {
	if sim := floatVectorSimilarityValue(); sim != "" {
		return fmt.Sprintf(" WITH (similarity = '%s')", sim)
	}
	return ""
}

// floatVectorSimilarityValue is a helper used by the adapter.
func floatVectorSimilarityValue() string {
	raw := strings.TrimSpace(os.Getenv(floatVectorSimilarityEnv))
	if raw == "" {
		return ""
	}
	if sim, ok := mongopath.NormalizeVectorSimilarity(raw); ok {
		return sim
	}
	floatVectorSimilarityWarn.Do(func() {
		if log := logging.Logger(); log != nil {
			log.Warn("invalid vector similarity in MOG_FLOAT_VECTOR_SIMILARITY, ignoring",
				zap.String("value", raw))
		}
	})
	return ""
}

// looksLikeVectorField is a helper used by the adapter.
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

// isUndefinedColumn is a helper used by the adapter.
func isUndefinedColumn(err error) bool { return sql.IsUndefinedColumn(err) }

// isUndefinedRelation is a helper used by the adapter.
func isUndefinedRelation(err error) bool { return sql.IsUndefinedRelation(err) }

// isUndefinedSchema is a helper used by the adapter.
func isUndefinedSchema(err error) bool { return sql.IsUndefinedSchema(err) }

// isDuplicateColumn is a helper used by the adapter.
func isDuplicateColumn(err error) bool { return sql.IsDuplicateColumn(err) }

// isDuplicateObject is a helper used by the adapter.
func isDuplicateObject(err error) bool { return sql.IsDuplicateObject(err) }

// isUniqueViolation is a helper used by the adapter.
func isUniqueViolation(err error) bool { return sql.IsUniqueViolation(err) }

// isDuplicateColumnName is a helper used by the adapter.
func isDuplicateColumnName(err error) bool { return sql.IsDuplicateColumnName(err) }
