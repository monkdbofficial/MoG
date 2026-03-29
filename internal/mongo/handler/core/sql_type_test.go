package mongo

import (
	"testing"

	"gopkg.in/mgo.v2/bson"
)

func TestSQLTypeForValue_ArrayIsNotObjectColumn(t *testing.T) {
	if got := sqlTypeForValue([]interface{}{"a", "b"}); got != "TEXT" {
		t.Fatalf("expected array to be stored as JSON in TEXT, got %q", got)
	}
}

func TestSQLTypeForValue_FloatVector(t *testing.T) {
	if got := sqlTypeForValue([]interface{}{1, 2, 3}); got != "FLOAT_VECTOR(3)" {
		t.Fatalf("expected FLOAT_VECTOR(3), got %q", got)
	}
}

func TestSQLTypeForValue_Object(t *testing.T) {
	if got := sqlTypeForValue(bson.M{"a": 1}); got != "OBJECT(DYNAMIC)" {
		t.Fatalf("expected OBJECT(DYNAMIC), got %q", got)
	}
}
