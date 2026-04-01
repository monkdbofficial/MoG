package shared

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"gopkg.in/mgo.v2/bson"
)

func TestPrimaryCommandKey_SkipsMetadataAndSorts(t *testing.T) {
	cmd := bson.M{
		"$db":       "admin",
		"lsid":      bson.M{"id": "x"},
		"txnNumber": 1,
		"find":      "users",
		"aggregate": "orders",
		"writeConcern": bson.M{
			"w": 1,
		},
	}
	// aggregate < find lexicographically
	if got := PrimaryCommandKey(cmd); got != "aggregate" {
		t.Fatalf("unexpected command key: %q", got)
	}
}

func TestCommandDBAndAsString(t *testing.T) {
	if got := CommandDB(bson.M{"$db": "app"}); got != "app" {
		t.Fatalf("unexpected db: %q", got)
	}
	if got := CommandDB(bson.M{"$db": bson.Symbol("symdb")}); got != "symdb" {
		t.Fatalf("unexpected db for symbol: %q", got)
	}
	if got := CommandDB(bson.M{"$db": 123}); got != "" {
		t.Fatalf("expected empty db, got: %q", got)
	}

	if s, ok := AsString("x"); !ok || s != "x" {
		t.Fatalf("unexpected AsString for string: ok=%v s=%q", ok, s)
	}
	if s, ok := AsString(bson.Symbol("y")); !ok || s != "y" {
		t.Fatalf("unexpected AsString for symbol: ok=%v s=%q", ok, s)
	}
	if _, ok := AsString(1); ok {
		t.Fatalf("expected non-string to fail")
	}
}

func TestFilterHasOperatorAndStripOperatorKeys(t *testing.T) {
	filter := bson.M{
		"age":  bson.M{"$gt": 10, "$lt": 20},
		"name": "sam",
	}
	if !FilterHasOperator(filter, "$gt") {
		t.Fatalf("expected $gt to be detected")
	}
	if FilterHasOperator(filter, "$in") {
		t.Fatalf("did not expect $in to be detected")
	}

	stripped := StripOperatorKeys(filter, "$gt")
	if _, ok := stripped["age"]; ok {
		t.Fatalf("expected age to be stripped due to $gt")
	}
	if got := stripped["name"]; got != "sam" {
		t.Fatalf("unexpected name: %#v", got)
	}
}

func TestIdentifierMapping_RoundTrip(t *testing.T) {
	tests := []string{
		"_id",
		"age",
		"_private",
		"data",
		"id",
		"f_x",
		"weird-dash",
		"with space",
	}
	for _, field := range tests {
		col := SQLColumnNameForField(field)
		if col == "" {
			t.Fatalf("empty column for field %q", field)
		}
		back := MongoFieldNameForColumn(col)
		if field == "_id" {
			if back != "_id" {
				t.Fatalf("expected _id roundtrip, got %q", back)
			}
			continue
		}
		if field == "age" {
			if col != "age" || back != "age" {
				t.Fatalf("expected safe identifier passthrough, got col=%q back=%q", col, back)
			}
			continue
		}
		// For non-safe identifiers we expect a stable decode back to the original field.
		if strings.HasPrefix(col, "f_") && back != field {
			t.Fatalf("expected decode back to %q, got col=%q back=%q", field, col, back)
		}
	}
}

func TestOrderTopLevelDocForReply(t *testing.T) {
	in := bson.M{
		"b":   2,
		"_id": "x",
		"a":   1,
	}
	out := OrderTopLevelDocForReply(in, nil)
	if len(out) != 3 {
		t.Fatalf("unexpected length: %d", len(out))
	}
	if out[0].Name != "_id" || out[0].Value != "x" {
		t.Fatalf("expected _id first, got %#v", out[0])
	}
	if out[1].Name != "a" || out[2].Name != "b" {
		t.Fatalf("expected alphabetical keys, got: %#v", out)
	}
}

func TestOrderTopLevelDocForReply_LeavesNestedValuesUnchanged(t *testing.T) {
	in := bson.M{
		"_id": "x",
		"z": bson.M{
			"b": 2,
			"a": 1,
		},
		"arr": []interface{}{
			bson.M{"y": 2, "x": 1},
		},
	}
	out := OrderTopLevelDocForReply(in, nil)
	if nested, ok := out[2].Value.(bson.M); !ok || nested["a"] != 1 || nested["b"] != 2 {
		t.Fatalf("expected nested doc left as bson.M, got %#v", out[2].Value)
	}
	arr, ok := out[1].Value.([]interface{})
	if !ok || len(arr) != 1 {
		t.Fatalf("expected array unchanged, got %#v", out[1].Value)
	}
	nestedArrDoc, ok := arr[0].(bson.M)
	if !ok || nestedArrDoc["x"] != 1 || nestedArrDoc["y"] != 2 {
		t.Fatalf("expected nested doc inside array unchanged, got %#v", arr[0])
	}
}

func TestOrderTopLevelDocForReply_PrefersProvidedOrder(t *testing.T) {
	in := bson.M{"manager_id": 2, "_id": "u4", "active": false, "name": "Dan", "user_id": 4}
	out := OrderTopLevelDocForReply(in, []string{"_id", "user_id", "name", "manager_id"})
	if got := []string{out[0].Name, out[1].Name, out[2].Name, out[3].Name, out[4].Name}; fmt.Sprint(got) != fmt.Sprint([]string{"_id", "user_id", "name", "manager_id", "active"}) {
		t.Fatalf("unexpected order: %#v", out)
	}
}

func TestCoerceBsonM(t *testing.T) {
	if m, ok := CoerceBsonM(bson.M{"a": 1}); !ok || m["a"] != 1 {
		t.Fatalf("unexpected bson.M coercion: ok=%v m=%v", ok, m)
	}
	if m, ok := CoerceBsonM(map[string]interface{}{"a": 1}); !ok || m["a"] != 1 {
		t.Fatalf("unexpected map coercion: ok=%v m=%v", ok, m)
	}
	if m, ok := CoerceBsonM(bson.D{{Name: "a", Value: 1}}); !ok || m["a"] != 1 {
		t.Fatalf("unexpected bson.D coercion: ok=%v m=%v", ok, m)
	}

	b, err := bson.Marshal(bson.M{"a": 1})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if m, ok := CoerceBsonM(b); !ok || m["a"] != 1 {
		t.Fatalf("unexpected bson bytes coercion: ok=%v m=%v", ok, m)
	}
	if m, ok := CoerceBsonM(`{"a":1}`); !ok || m["a"] != float64(1) {
		// json numbers decode as float64
		t.Fatalf("unexpected json coercion: ok=%v m=%v", ok, m)
	}

	if _, ok := CoerceBsonM([]byte("not bson or json")); ok {
		t.Fatalf("expected invalid bytes to fail")
	}
}

func TestMarshalObject(t *testing.T) {
	// Use a normalized value for _id to avoid relying on mgo/bson's Extended JSON wrappers.
	got, err := MarshalObject(bson.M{"_id": bson.NewObjectId().Hex(), "n": int64(1)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "\"n\"") {
		t.Fatalf("expected JSON output to contain field, got: %q", got)
	}

	_, err = MarshalObject(func() {})
	if err == nil {
		t.Fatalf("expected marshal error")
	}
}

func TestDocHasOperatorKeysAndCoerceInterfaceSlice(t *testing.T) {
	if !DocHasOperatorKeys(bson.M{"$gt": 1}) {
		t.Fatalf("expected operator keys to be detected")
	}
	if DocHasOperatorKeys(bson.M{"a": 1}) {
		t.Fatalf("did not expect operator keys")
	}

	if s, ok := CoerceInterfaceSlice([]string{"a", "b"}); !ok || len(s) != 2 || s[0] != "a" {
		t.Fatalf("unexpected []string coercion: ok=%v s=%v", ok, s)
	}
	if _, ok := CoerceInterfaceSlice([]byte{1, 2, 3}); ok {
		t.Fatalf("expected []byte to not be treated as slice")
	}
	if _, ok := CoerceInterfaceSlice(errors.New("x")); ok {
		t.Fatalf("expected non-slice to fail")
	}
}
