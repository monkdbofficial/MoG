package mongo

import (
	"reflect"
	"testing"

	"gopkg.in/mgo.v2/bson"
)

func TestNormalizeRowValue_JSONBytesArray(t *testing.T) {
	got := normalizeRowValue([]byte(`["a","b"]`))
	arr, ok := got.([]interface{})
	if !ok {
		t.Fatalf("expected []interface{}, got %T (%v)", got, got)
	}
	if !reflect.DeepEqual(arr, []interface{}{"a", "b"}) {
		t.Fatalf("unexpected array: %#v", arr)
	}
}

func TestNormalizeRowValue_JSONBytesObject(t *testing.T) {
	got := normalizeRowValue([]byte(`{"a":1}`))
	m, ok := got.(bson.M)
	if !ok {
		t.Fatalf("expected bson.M, got %T (%v)", got, got)
	}
	if m["a"] != float64(1) {
		t.Fatalf("unexpected object: %#v", m)
	}
}
