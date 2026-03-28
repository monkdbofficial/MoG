package mongo

import (
	"sync"
	"testing"

	"gopkg.in/mgo.v2/bson"
)

func TestApplyUpdate_Set_DotPathCreates(t *testing.T) {
	doc := bson.M{}
	update := bson.M{"$set": bson.M{"a.b.c.d": 10}}

	out, _ := ApplyUpdate(doc, update)
	if doc["a"] != nil {
		t.Fatalf("expected input doc not mutated")
	}
	if getPathValue(out, "a.b.c.d") != 10 {
		t.Fatalf("expected a.b.c.d=10, got %#v", getPathValue(out, "a.b.c.d"))
	}
}

func TestApplyUpdate_Set_NullOverwritten(t *testing.T) {
	doc := bson.M{"address": nil}
	update := bson.M{"$set": bson.M{"address.city": "blr"}}

	out, _ := ApplyUpdate(doc, update)
	if getPathValue(out, "address.city") != "blr" {
		t.Fatalf("expected address.city=blr, got %#v", getPathValue(out, "address.city"))
	}
}

func TestApplyUpdate_Inc_MissingTreatZero(t *testing.T) {
	doc := bson.M{}
	update := bson.M{"$inc": bson.M{"counter.value": 5}}

	out, _ := ApplyUpdate(doc, update)
	if getPathValue(out, "counter.value") != float64(5) {
		t.Fatalf("expected counter.value=5, got %#v", getPathValue(out, "counter.value"))
	}
}

func TestApplyUpdate_Push_NestedCreatesArray(t *testing.T) {
	doc := bson.M{}
	update := bson.M{"$push": bson.M{"arr.items": bson.M{"x": 1}}}

	out, _ := ApplyUpdate(doc, update)
	got := getPathValue(out, "arr.items")
	arr, ok := got.([]interface{})
	if !ok || len(arr) != 1 {
		t.Fatalf("expected arr.items to be array len 1, got %#v", got)
	}
	m, ok := arr[0].(bson.M)
	if !ok || m["x"] != 1 {
		t.Fatalf("unexpected pushed value: %#v", arr[0])
	}
}

func TestApplyUpdate_Unset_NestedSafe(t *testing.T) {
	doc := bson.M{"a": bson.M{"b": 1, "c": 2}}
	update := bson.M{"$unset": bson.M{"a.b": ""}}

	out, _ := ApplyUpdate(doc, update)
	if getPathValue(out, "a.b") != nil {
		t.Fatalf("expected a.b removed, got %#v", getPathValue(out, "a.b"))
	}
	if getPathValue(out, "a.c") != 2 {
		t.Fatalf("expected a.c preserved, got %#v", getPathValue(out, "a.c"))
	}
}

func TestApplyUpdate_Concurrent_NoMutation(t *testing.T) {
	base := bson.M{"counter": bson.M{"value": 0}}
	update := bson.M{"$inc": bson.M{"counter.value": 1}}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = ApplyUpdate(base, update)
		}()
	}
	wg.Wait()

	if getPathValue(base, "counter.value") != 0 {
		t.Fatalf("expected base doc unchanged, got %#v", base)
	}
}

func TestBuildUpsertBaseDoc_IgnoresOperatorFilters(t *testing.T) {
	filter := bson.M{
		"a.b": 1,
		"age": bson.M{"$gte": 10},
	}
	doc := buildUpsertBaseDoc(filter)
	if getPathValue(doc, "a.b") != 1 {
		t.Fatalf("expected a.b=1, got %#v", getPathValue(doc, "a.b"))
	}
	if _, ok := doc["age"]; ok {
		t.Fatalf("expected operator filter field ignored, got %#v", doc["age"])
	}
}
