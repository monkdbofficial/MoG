package sql

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"gopkg.in/mgo.v2/bson"
)

// TestTypeForValue runs the corresponding test case.
func TestTypeForValue(t *testing.T) {
	if got := TypeForValue(nil); got != "" {
		t.Fatalf("expected empty type for nil, got %q", got)
	}
	if got := TypeForValue(true); got != "BOOLEAN" {
		t.Fatalf("unexpected bool type: %q", got)
	}
	if got := TypeForValue("x"); got != "TEXT" {
		t.Fatalf("unexpected string type: %q", got)
	}
	if got := TypeForValue(bson.Symbol("x")); got != "TEXT" {
		t.Fatalf("unexpected symbol type: %q", got)
	}
	if got := TypeForValue(1); got != "LONG" {
		t.Fatalf("unexpected int type: %q", got)
	}
	if got := TypeForValue(1.5); got != "DOUBLE PRECISION" {
		t.Fatalf("unexpected float type: %q", got)
	}
	if got := TypeForValue(time.Now()); got != "TIMESTAMP WITH TIME ZONE" {
		t.Fatalf("unexpected time type: %q", got)
	}
	if got := TypeForValue(bson.M{"a": 1}); got != "OBJECT(DYNAMIC)" {
		t.Fatalf("unexpected object type: %q", got)
	}

	// arrays are stored as typed ARRAY(...) when a stable element type can be inferred
	if got := TypeForValue([]int{1, 2, 3}); got != "ARRAY(LONG)" {
		t.Fatalf("unexpected int-array type: %q", got)
	}
	if got := TypeForValue([]string{"a", "b"}); got != "ARRAY(TEXT)" {
		t.Fatalf("unexpected string-array type: %q", got)
	}
}

// TestFloatVectorLiteral runs the corresponding test case.
func TestFloatVectorLiteral(t *testing.T) {
	lit, n, ok := FloatVectorLiteral([]interface{}{1, 2.5, 3})
	if !ok {
		t.Fatalf("expected vector literal to succeed")
	}
	if n != 3 {
		t.Fatalf("unexpected vector length: %d", n)
	}
	if lit != "[1, 2.5, 3]" {
		t.Fatalf("unexpected literal: %q", lit)
	}

	if _, _, ok := FloatVectorLiteral([]byte{1, 2}); ok {
		t.Fatalf("expected []byte to be rejected")
	}
	if _, _, ok := FloatVectorLiteral([]interface{}{1, nil}); ok {
		t.Fatalf("expected nil element to be rejected")
	}
	if _, _, ok := FloatVectorLiteral([]interface{}{"x"}); ok {
		t.Fatalf("expected non-numeric element to be rejected")
	}
}

// TestArrayArgForSQLType runs the corresponding test case.
func TestArrayArgForSQLType(t *testing.T) {
	v, err := ArrayArgForSQLType([]interface{}{"a", "b"}, "ARRAY(TEXT)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := v.([]string); !ok {
		t.Fatalf("expected []string, got %T", v)
	}

	v, err = ArrayArgForSQLType([]interface{}{1, 2, 3}, "ARRAY(LONG)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := v.([]int64); !ok {
		t.Fatalf("expected []int64, got %T", v)
	}
}

// TestPgErrorClassification runs the corresponding test case.
func TestPgErrorClassification(t *testing.T) {
	wrap := func(e *pgconn.PgError) error { return fmt.Errorf("wrap: %w", e) }

	if !IsUndefinedColumn(wrap(&pgconn.PgError{Code: "42703"})) {
		t.Fatalf("expected undefined column")
	}
	if !IsUndefinedRelation(wrap(&pgconn.PgError{Code: "42P01"})) {
		t.Fatalf("expected undefined relation")
	}
	if !IsUndefinedSchema(wrap(&pgconn.PgError{Code: "3F000"})) {
		t.Fatalf("expected undefined schema")
	}
	if !IsDuplicateColumn(wrap(&pgconn.PgError{Code: "42701"})) {
		t.Fatalf("expected duplicate column")
	}
	if !IsDuplicateObject(wrap(&pgconn.PgError{Code: "42710"})) {
		t.Fatalf("expected duplicate object (42710)")
	}
	if !IsDuplicateObject(wrap(&pgconn.PgError{Code: "42P07"})) {
		t.Fatalf("expected duplicate object (42P07)")
	}
	if IsDuplicateObject(errors.New("not pg")) {
		t.Fatalf("did not expect non-pg error to match")
	}
}

// TestUniqueViolationAndDuplicateColumnName runs the corresponding test case.
func TestUniqueViolationAndDuplicateColumnName(t *testing.T) {
	if !IsUniqueViolation(&pgconn.PgError{Code: "23505"}) {
		t.Fatalf("expected unique violation by code")
	}
	if !IsUniqueViolation(&pgconn.PgError{Code: "99999", Message: "duplicate key value violates unique constraint"}) {
		t.Fatalf("expected unique violation by pg message heuristic")
	}
	if !IsUniqueViolation(errors.New("unique constraint")) {
		t.Fatalf("expected unique violation by generic error message")
	}

	if !IsDuplicateColumnName(&pgconn.PgError{Message: "table already has a column named x"}) {
		t.Fatalf("expected duplicate column name by message")
	}
	if !IsDuplicateColumnName(&pgconn.PgError{Code: "42701"}) {
		t.Fatalf("expected duplicate column name by code fallback")
	}
}
