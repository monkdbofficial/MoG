package relational

import "testing"

import "gopkg.in/mgo.v2/bson"

// TestRelationalAccessor_TopLevel runs the corresponding test case.
func TestRelationalAccessor_TopLevel(t *testing.T) {
	acc, ok := relationalAccessor("age")
	if !ok || acc != "age" {
		t.Fatalf("unexpected accessor: ok=%v acc=%q", ok, acc)
	}
}

// TestRelationalAccessor_Nested runs the corresponding test case.
func TestRelationalAccessor_Nested(t *testing.T) {
	acc, ok := relationalAccessor("addr.city")
	if !ok || acc != "addr['city']" {
		t.Fatalf("unexpected accessor: ok=%v acc=%q", ok, acc)
	}
}

// TestBuildRelationalWhere_NestedEquality runs the corresponding test case.
func TestBuildRelationalWhere_NestedEquality(t *testing.T) {
	w, ok, err := BuildWhere(bson.M{"addr.city": "Hyd"})
	if err != nil || !ok || w == nil {
		t.Fatalf("unexpected: ok=%v err=%v w=%#v", ok, err, w)
	}
	if w.SQL != "addr['city'] = $1" {
		t.Fatalf("unexpected sql: %q", w.SQL)
	}
	if len(w.Args) != 1 || w.Args[0] != "Hyd" {
		t.Fatalf("unexpected args: %#v", w.Args)
	}
}

// TestBuildRelationalWhere_NumericCast runs the corresponding test case.
func TestBuildRelationalWhere_NumericCast(t *testing.T) {
	w, ok, err := BuildWhere(bson.M{"age": bson.M{"$gt": 25}})
	if err != nil || !ok || w == nil {
		t.Fatalf("unexpected: ok=%v err=%v w=%#v", ok, err, w)
	}
	if w.SQL != "CAST(age AS DOUBLE PRECISION) > $1" {
		t.Fatalf("unexpected sql: %q", w.SQL)
	}
}

// TestBuildRelationalWhere_IDEquality_EncodesDocID runs the corresponding test case.
func TestBuildRelationalWhere_IDEquality_EncodesDocID(t *testing.T) {
	oid := bson.NewObjectId()
	w, ok, err := BuildWhere(bson.M{"_id": oid})
	if err != nil || !ok || w == nil {
		t.Fatalf("unexpected: ok=%v err=%v w=%#v", ok, err, w)
	}
	if w.SQL != "id = $1" {
		t.Fatalf("unexpected sql: %q", w.SQL)
	}
	want := "\"" + oid.Hex() + "\""
	if len(w.Args) != 1 || w.Args[0] != want {
		t.Fatalf("unexpected args: %#v want=%q", w.Args, want)
	}
}

// TestBuildRelationalWhere_IDIn_EncodesDocID runs the corresponding test case.
func TestBuildRelationalWhere_IDIn_EncodesDocID(t *testing.T) {
	oid1 := bson.NewObjectId()
	oid2 := bson.NewObjectId()
	w, ok, err := BuildWhere(bson.M{"_id": bson.M{"$in": []interface{}{oid1, oid2}}})
	if err != nil || !ok || w == nil {
		t.Fatalf("unexpected: ok=%v err=%v w=%#v", ok, err, w)
	}
	if w.SQL != "id IN ($1, $2)" {
		t.Fatalf("unexpected sql: %q", w.SQL)
	}
	if len(w.Args) != 2 {
		t.Fatalf("unexpected args: %#v", w.Args)
	}
	if w.Args[0] != "\""+oid1.Hex()+"\"" || w.Args[1] != "\""+oid2.Hex()+"\"" {
		t.Fatalf("unexpected args: %#v", w.Args)
	}
}

// TestBuildFilterPushdown_SplitsSupportedAndResidualFilters runs the corresponding test case.
func TestBuildFilterPushdown_SplitsSupportedAndResidualFilters(t *testing.T) {
	pushdown, err := BuildFilterPushdown(bson.M{
		"age":     bson.M{"$gt": 25},
		"complex": bson.M{"nested": "x"},
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if pushdown.Where == nil || pushdown.Where.SQL != "CAST(age AS DOUBLE PRECISION) > $1" {
		t.Fatalf("unexpected where: %#v", pushdown.Where)
	}
	if got := pushdown.PushedFilter["age"]; got == nil {
		t.Fatalf("expected age in pushed filter, got %#v", pushdown.PushedFilter)
	}
	if got := pushdown.ResidualFilter["complex"]; got == nil {
		t.Fatalf("expected complex in residual filter, got %#v", pushdown.ResidualFilter)
	}
}

// TestBuildFilterPushdown_LeavesInOperatorResidual runs the corresponding test case.
func TestBuildFilterPushdown_LeavesInOperatorResidual(t *testing.T) {
	pushdown, err := BuildFilterPushdown(bson.M{
		"status": bson.M{"$in": []string{"new", "active"}},
		"age":    bson.M{"$gte": 21},
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if pushdown.Where == nil || pushdown.Where.SQL != "CAST(age AS DOUBLE PRECISION) >= $1" {
		t.Fatalf("unexpected where: %#v", pushdown.Where)
	}
	if _, ok := pushdown.PushedFilter["status"]; ok {
		t.Fatalf("expected $in field to stay out of pushdown: %#v", pushdown.PushedFilter)
	}
	if _, ok := pushdown.ResidualFilter["status"]; !ok {
		t.Fatalf("expected $in field in residual filter: %#v", pushdown.ResidualFilter)
	}
}
