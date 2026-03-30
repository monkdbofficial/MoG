package mongo

import (
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"
)

func TestSQLTypeForValue_ArrayIsNotObjectColumn(t *testing.T) {
	if got := sqlTypeForValue([]interface{}{"a", "b"}); got != "ARRAY(TEXT)" {
		t.Fatalf("expected array to be stored as ARRAY(TEXT), got %q", got)
	}
}

func TestSQLTypeForValue_FloatVector(t *testing.T) {
	if got := sqlTypeForValue([]interface{}{1, 2, 3}); got != "ARRAY(LONG)" {
		t.Fatalf("expected ARRAY(LONG), got %q", got)
	}
}

func TestSQLTypeForValue_Object(t *testing.T) {
	if got := sqlTypeForValue(bson.M{"a": 1}); got != "OBJECT(DYNAMIC)" {
		t.Fatalf("expected OBJECT(DYNAMIC), got %q", got)
	}
}

func TestSQLTypeForValue_NumericAndTimeTypes(t *testing.T) {
	if got := sqlTypeForValue(int32(1)); got != "INTEGER" {
		t.Fatalf("expected INTEGER, got %q", got)
	}
	if got := sqlTypeForValue(int64(1)); got != "LONG" {
		t.Fatalf("expected LONG, got %q", got)
	}
	if got := sqlTypeForValue(float32(1.5)); got != "FLOAT" {
		t.Fatalf("expected FLOAT, got %q", got)
	}
	if got := sqlTypeForValue(float64(1.5)); got != "DOUBLE PRECISION" {
		t.Fatalf("expected DOUBLE PRECISION, got %q", got)
	}
	if got := sqlTypeForValue(time.Now()); got != "TIMESTAMP WITH TIME ZONE" {
		t.Fatalf("expected TIMESTAMP WITH TIME ZONE, got %q", got)
	}
}

func TestSQLTypeForValue_ObjectID(t *testing.T) {
	if got := sqlTypeForValue(bson.NewObjectId()); got != "TEXT" {
		t.Fatalf("expected TEXT, got %q", got)
	}
}
