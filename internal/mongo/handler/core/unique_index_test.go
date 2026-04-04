package mongo

import (
	"testing"

	"gopkg.in/mgo.v2/bson"
)

// TestUniqueIndexKeyTuples_Scalar runs the corresponding test case.
func TestUniqueIndexKeyTuples_Scalar(t *testing.T) {
	tuples, ok := uniqueIndexKeyTuples(bson.M{"a": 1}, []string{"a"})
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if len(tuples) != 1 || len(tuples[0]) != 1 || tuples[0][0] != 1 {
		t.Fatalf("unexpected tuples: %#v", tuples)
	}
}

// TestUniqueIndexKeyTuples_ArraySingleField runs the corresponding test case.
func TestUniqueIndexKeyTuples_ArraySingleField(t *testing.T) {
	tuples, ok := uniqueIndexKeyTuples(bson.M{"a": []interface{}{"x", "y"}}, []string{"a"})
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if len(tuples) != 2 {
		t.Fatalf("expected 2 tuples, got %#v", tuples)
	}
	if tuples[0][0] != "x" || tuples[1][0] != "y" {
		t.Fatalf("unexpected tuples: %#v", tuples)
	}
}

// TestUniqueIndexKeyTuples_CompoundWithArray runs the corresponding test case.
func TestUniqueIndexKeyTuples_CompoundWithArray(t *testing.T) {
	doc := bson.M{"a": []interface{}{1, 2}, "b": "k"}
	tuples, ok := uniqueIndexKeyTuples(doc, []string{"a", "b"})
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if len(tuples) != 2 {
		t.Fatalf("expected 2 tuples, got %#v", tuples)
	}
	if tuples[0][0] != 1 || tuples[0][1] != "k" || tuples[1][0] != 2 || tuples[1][1] != "k" {
		t.Fatalf("unexpected tuples: %#v", tuples)
	}
}

// TestUniqueIndexKeyTuplesIntersect runs the corresponding test case.
func TestUniqueIndexKeyTuplesIntersect(t *testing.T) {
	a := [][]interface{}{{"x"}}
	b := [][]interface{}{{"y"}, {"x"}}
	if !uniqueIndexKeyTuplesIntersect(a, b) {
		t.Fatalf("expected intersection")
	}
}
