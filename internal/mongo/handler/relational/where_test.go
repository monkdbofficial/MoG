package relational

import "testing"

import "gopkg.in/mgo.v2/bson"

func TestRelationalAccessor_TopLevel(t *testing.T) {
	acc, ok := relationalAccessor("age")
	if !ok || acc != "age" {
		t.Fatalf("unexpected accessor: ok=%v acc=%q", ok, acc)
	}
}

func TestRelationalAccessor_Nested(t *testing.T) {
	acc, ok := relationalAccessor("addr.city")
	if !ok || acc != "addr['city']" {
		t.Fatalf("unexpected accessor: ok=%v acc=%q", ok, acc)
	}
}

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

func TestBuildRelationalWhere_NumericCast(t *testing.T) {
	w, ok, err := BuildWhere(bson.M{"age": bson.M{"$gt": 25}})
	if err != nil || !ok || w == nil {
		t.Fatalf("unexpected: ok=%v err=%v w=%#v", ok, err, w)
	}
	if w.SQL != "CAST(age AS DOUBLE PRECISION) > $1" {
		t.Fatalf("unexpected sql: %q", w.SQL)
	}
}

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
