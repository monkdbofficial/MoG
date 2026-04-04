package update

import (
	"sync"
	"testing"

	mpipeline "mog/internal/mongo/pipeline"

	"gopkg.in/mgo.v2/bson"
)

// TestApplyUpdate_Set_DotPathCreates runs the corresponding test case.
func TestApplyUpdate_Set_DotPathCreates(t *testing.T) {
	doc := bson.M{}
	update := bson.M{"$set": bson.M{"a.b.c.d": 10}}

	out, _ := ApplyUpdate(doc, update)
	if doc["a"] != nil {
		t.Fatalf("expected input doc not mutated")
	}
	if mpipeline.GetPathValue(out, "a.b.c.d") != 10 {
		t.Fatalf("expected a.b.c.d=10, got %#v", mpipeline.GetPathValue(out, "a.b.c.d"))
	}
}

// TestApplyUpdate_Set_NullOverwritten runs the corresponding test case.
func TestApplyUpdate_Set_NullOverwritten(t *testing.T) {
	doc := bson.M{"address": nil}
	update := bson.M{"$set": bson.M{"address.city": "blr"}}

	out, _ := ApplyUpdate(doc, update)
	if mpipeline.GetPathValue(out, "address.city") != "blr" {
		t.Fatalf("expected address.city=blr, got %#v", mpipeline.GetPathValue(out, "address.city"))
	}
}

// TestApplyUpdate_Inc_MissingTreatZero runs the corresponding test case.
func TestApplyUpdate_Inc_MissingTreatZero(t *testing.T) {
	doc := bson.M{}
	update := bson.M{"$inc": bson.M{"counter.value": 5}}

	out, _ := ApplyUpdate(doc, update)
	if mpipeline.GetPathValue(out, "counter.value") != 5 {
		t.Fatalf("expected counter.value=5, got %#v", mpipeline.GetPathValue(out, "counter.value"))
	}
}

// TestApplyUpdate_Inc_PreservesInt32 runs the corresponding test case.
func TestApplyUpdate_Inc_PreservesInt32(t *testing.T) {
	doc := bson.M{"age": int32(25)}
	update := bson.M{"$inc": bson.M{"age": int32(5)}}

	out, _ := ApplyUpdate(doc, update)
	got := mpipeline.GetPathValue(out, "age")
	if got != int32(30) {
		t.Fatalf("expected age=int32(30), got %#v (%T)", got, got)
	}
}

// TestApplyUpdate_Push_NestedCreatesArray runs the corresponding test case.
func TestApplyUpdate_Push_NestedCreatesArray(t *testing.T) {
	doc := bson.M{}
	update := bson.M{"$push": bson.M{"arr.items": bson.M{"x": 1}}}

	out, _ := ApplyUpdate(doc, update)
	got := mpipeline.GetPathValue(out, "arr.items")
	arr, ok := got.([]interface{})
	if !ok || len(arr) != 1 {
		t.Fatalf("expected arr.items to be array len 1, got %#v", got)
	}
	m, ok := arr[0].(bson.M)
	if !ok || m["x"] != 1 {
		t.Fatalf("unexpected pushed value: %#v", arr[0])
	}
}

// TestApplyUpdate_Unset_NestedSafe runs the corresponding test case.
func TestApplyUpdate_Unset_NestedSafe(t *testing.T) {
	doc := bson.M{"a": bson.M{"b": 1, "c": 2}}
	update := bson.M{"$unset": bson.M{"a.b": ""}}

	out, _ := ApplyUpdate(doc, update)
	if mpipeline.GetPathValue(out, "a.b") != nil {
		t.Fatalf("expected a.b removed, got %#v", mpipeline.GetPathValue(out, "a.b"))
	}
	if mpipeline.GetPathValue(out, "a.c") != 2 {
		t.Fatalf("expected a.c preserved, got %#v", mpipeline.GetPathValue(out, "a.c"))
	}
}

// TestApplyUpdate_Concurrent_NoMutation runs the corresponding test case.
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

	if mpipeline.GetPathValue(base, "counter.value") != 0 {
		t.Fatalf("expected base doc unchanged, got %#v", base)
	}
}

// TestBuildUpsertBaseDoc_IgnoresOperatorFilters runs the corresponding test case.
func TestBuildUpsertBaseDoc_IgnoresOperatorFilters(t *testing.T) {
	filter := bson.M{
		"a.b": 1,
		"age": bson.M{"$gte": 10},
	}
	doc := BuildUpsertBaseDoc(filter)
	if mpipeline.GetPathValue(doc, "a.b") != 1 {
		t.Fatalf("expected a.b=1, got %#v", mpipeline.GetPathValue(doc, "a.b"))
	}
	if _, ok := doc["age"]; ok {
		t.Fatalf("expected operator filter field ignored, got %#v", doc["age"])
	}
}
