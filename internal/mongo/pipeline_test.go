package mongo

import (
	"fmt"
	"testing"

	"gopkg.in/mgo.v2/bson"
)

func TestApplyPipeline_GroupAvgSortLimit(t *testing.T) {
	docs := []bson.M{
		{"age": 25, "score": 10.0},
		{"age": 25, "score": 30.0},
		{"age": 30, "score": 50.0},
	}
	pipeline := []bson.M{
		{"$match": bson.M{"age": bson.M{"$gt": 20}}},
		{"$group": bson.M{"_id": "$age", "avgScore": bson.M{"$avg": "$score"}, "count": bson.M{"$sum": 1}}},
		{"$sort": bson.M{"avgScore": -1}},
		{"$limit": 1},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(out))
	}
	if out[0]["_id"] != 30 {
		t.Fatalf("expected _id=30, got %#v", out[0]["_id"])
	}
	if out[0]["avgScore"] != 50.0 {
		t.Fatalf("expected avgScore=50, got %#v", out[0]["avgScore"])
	}
}

func TestApplyPipeline_ProjectMultiply(t *testing.T) {
	docs := []bson.M{
		{"name": "a", "score": 2.0},
	}
	pipeline := []bson.M{
		{"$project": bson.M{"name": 1, "computed": bson.M{"$multiply": []interface{}{"$score", 3}}}},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if out[0]["name"] != "a" {
		t.Fatalf("expected name preserved, got %#v", out[0])
	}
	if out[0]["computed"] != 6.0 {
		t.Fatalf("expected computed=6, got %#v", out[0]["computed"])
	}
}

func TestApplyPipeline_ProjectSize(t *testing.T) {
	docs := []bson.M{
		{"_id": 1, "users": []interface{}{1, 2, 3}},
		{"_id": 2, "users": []int{1, 2}},
		{"_id": 3},                // missing => 0
		{"_id": 4, "users": nil},  // null => 0
		{"_id": 5, "users": "no"}, // non-array => 0 (safe mode)
	}
	pipeline := []bson.M{
		{"$project": bson.M{"_id": 1, "user_count": bson.M{"$size": "$users"}}},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if out[0]["user_count"] != int64(3) {
		t.Fatalf("expected user_count=3, got %#v", out[0]["user_count"])
	}
	if out[1]["user_count"] != int64(2) {
		t.Fatalf("expected user_count=2, got %#v", out[1]["user_count"])
	}
	if out[2]["user_count"] != int64(0) {
		t.Fatalf("expected user_count=0 for missing, got %#v", out[2]["user_count"])
	}
	if out[3]["user_count"] != int64(0) {
		t.Fatalf("expected user_count=0 for null, got %#v", out[3]["user_count"])
	}
	if out[4]["user_count"] != int64(0) {
		t.Fatalf("expected user_count=0 for non-array, got %#v", out[4]["user_count"])
	}
}

func TestApplyPipeline_UnsetStage_StringAndArray(t *testing.T) {
	docs := []bson.M{
		{"_id": 1, "a": 1, "nested": bson.M{"b": 2, "c": 3}},
	}

	out, err := applyPipeline(docs, []bson.M{
		{"$unset": "a"},
		{"$unset": []interface{}{"_id", "nested.b"}},
	})
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(out))
	}
	if _, ok := out[0]["a"]; ok {
		t.Fatalf("expected a unset, got %#v", out[0])
	}
	if _, ok := out[0]["_id"]; ok {
		t.Fatalf("expected _id unset, got %#v", out[0])
	}
	nested, _ := out[0]["nested"].(bson.M)
	if _, ok := nested["b"]; ok {
		t.Fatalf("expected nested.b unset, got %#v", out[0])
	}
	if nested["c"] != 3 {
		t.Fatalf("expected nested.c preserved, got %#v", out[0])
	}

	// Original doc is not mutated.
	if _, ok := docs[0]["a"]; !ok {
		t.Fatalf("expected original doc preserved, got %#v", docs[0])
	}
	origNested, _ := docs[0]["nested"].(bson.M)
	if _, ok := origNested["b"]; !ok {
		t.Fatalf("expected original nested.b preserved, got %#v", docs[0])
	}
}

func TestApplyPipeline_GroupConstantIDCount(t *testing.T) {
	docs := []bson.M{
		{"age": 1},
		{"age": 2},
		{"age": 3},
	}
	pipeline := []bson.M{
		{"$group": bson.M{"_id": nil, "n": bson.M{"$sum": 1}}},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(out))
	}
	if out[0]["n"] != int64(3) {
		t.Fatalf("expected n=3, got %#v", out[0]["n"])
	}
}

func TestApplyPipeline_CountStage(t *testing.T) {
	docs := []bson.M{
		{"a": 1},
		{"a": 2},
	}
	out, err := applyPipeline(docs, []bson.M{{"$count": "n"}})
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if len(out) != 1 || out[0]["n"] != int64(2) {
		t.Fatalf("unexpected out: %#v", out)
	}
}

func TestApplyPipeline_Lookup_NestedAndMissing(t *testing.T) {
	base := []bson.M{
		{"_id": 1, "address": bson.M{"city": "NY"}},
		{"_id": 2}, // missing localField => null
	}

	foreign := []bson.M{
		{"name": "nyc", "city": "NY"},
		{"name": "none"}, // missing foreignField => null
	}

	pipeline := []bson.M{
		{"$lookup": bson.M{
			"from":         "cities",
			"localField":   "address.city",
			"foreignField": "city",
			"as":           "cities",
		}},
	}

	resolver := func(from string) ([]bson.M, error) {
		if from != "cities" {
			return nil, nil
		}
		return foreign, nil
	}

	out, err := applyPipelineWithLookup(base, pipeline, resolver)
	if err != nil {
		t.Fatalf("applyPipelineWithLookup err: %v", err)
	}
	if _, ok := base[0]["cities"]; ok {
		t.Fatalf("expected base docs not mutated")
	}
	if got := out[0]["cities"].([]bson.M); len(got) != 1 || got[0]["name"] != "nyc" {
		t.Fatalf("unexpected lookup match: %#v", out[0]["cities"])
	}
	if got := out[1]["cities"].([]bson.M); len(got) != 1 || got[0]["name"] != "none" {
		t.Fatalf("expected null match: %#v", out[1]["cities"])
	}
}

func TestApplyPipeline_Lookup_ArrayLocalField(t *testing.T) {
	base := []bson.M{
		{"_id": 1, "tags": []interface{}{"a", "b"}},
	}
	foreign := []bson.M{
		{"tag": "a", "v": 1},
		{"tag": "c", "v": 2},
	}
	pipeline := []bson.M{
		{"$lookup": bson.M{
			"from":         "tagdocs",
			"localField":   "tags",
			"foreignField": "tag",
			"as":           "matches",
		}},
	}
	out, err := applyPipelineWithLookup(base, pipeline, func(from string) ([]bson.M, error) { return foreign, nil })
	if err != nil {
		t.Fatalf("applyPipelineWithLookup err: %v", err)
	}
	got := out[0]["matches"].([]bson.M)
	if len(got) != 1 || got[0]["tag"] != "a" {
		t.Fatalf("unexpected matches: %#v", got)
	}
}

func TestApplyPipeline_Unwind_Simple(t *testing.T) {
	docs := []bson.M{
		{"_id": 1, "orders": []interface{}{bson.M{"id": 10}, bson.M{"id": 11}}},
	}
	out, err := applyPipeline(docs, []bson.M{{"$unwind": "$orders"}})
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(out))
	}
	if docs[0]["orders"].([]interface{})[0].(bson.M)["id"] != 10 {
		t.Fatalf("expected original docs not mutated")
	}
	if out[0]["orders"].(bson.M)["id"] != 10 || out[1]["orders"].(bson.M)["id"] != 11 {
		t.Fatalf("unexpected unwind out: %#v", out)
	}
}

func TestApplyPipeline_Unwind_PreserveNullAndEmpty(t *testing.T) {
	docs := []bson.M{
		{"_id": 1},                            // missing
		{"_id": 2, "orders": []interface{}{}}, // empty array
		{"_id": 3, "orders": nil},             // null
	}
	out, err := applyPipeline(docs, []bson.M{{"$unwind": bson.M{"path": "$orders", "preserveNullAndEmptyArrays": true}}})
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("expected 3 docs, got %d", len(out))
	}
	for _, d := range out {
		if d["orders"] != nil {
			t.Fatalf("expected orders=null, got %#v", d["orders"])
		}
	}
}

func TestApplyPipeline_Unwind_NestedPath(t *testing.T) {
	docs := []bson.M{
		{"_id": 1, "orders": bson.M{"items": []interface{}{"a", "b"}}},
	}
	out, err := applyPipeline(docs, []bson.M{{"$unwind": "$orders.items"}})
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(out))
	}
	if out[0]["orders"].(bson.M)["items"] != "a" || out[1]["orders"].(bson.M)["items"] != "b" {
		t.Fatalf("unexpected unwind result: %#v", out)
	}
	// Ensure original not mutated.
	origItems := docs[0]["orders"].(bson.M)["items"].([]interface{})
	if len(origItems) != 2 || origItems[0] != "a" || origItems[1] != "b" {
		t.Fatalf("expected original nested array unchanged, got %#v", origItems)
	}
}

func TestApplyPipeline_Unwind_TypedSlices(t *testing.T) {
	t.Run("slice of bson.M", func(t *testing.T) {
		docs := []bson.M{
			{"_id": 1, "orders": []bson.M{{"id": 10}, {"id": 11}}},
		}
		out, err := applyPipeline(docs, []bson.M{{"$unwind": "$orders"}})
		if err != nil {
			t.Fatalf("applyPipeline err: %v", err)
		}
		if len(out) != 2 {
			t.Fatalf("expected 2 docs, got %d", len(out))
		}
		if out[0]["orders"].(bson.M)["id"] != 10 || out[1]["orders"].(bson.M)["id"] != 11 {
			t.Fatalf("unexpected unwind out: %#v", out)
		}

		// Ensure deep copy: mutating output must not affect input.
		out[0]["orders"].(bson.M)["id"] = 99
		if docs[0]["orders"].([]bson.M)[0]["id"] != 10 {
			t.Fatalf("expected original docs not mutated, got %#v", docs[0]["orders"])
		}
	})

	t.Run("slice of ints", func(t *testing.T) {
		docs := []bson.M{
			{"_id": 1, "nums": []int{1, 2}},
		}
		out, err := applyPipeline(docs, []bson.M{{"$unwind": "$nums"}})
		if err != nil {
			t.Fatalf("applyPipeline err: %v", err)
		}
		if len(out) != 2 {
			t.Fatalf("expected 2 docs, got %d", len(out))
		}
		if out[0]["nums"] != 1 || out[1]["nums"] != 2 {
			t.Fatalf("unexpected unwind out: %#v", out)
		}
		orig := docs[0]["nums"].([]int)
		if len(orig) != 2 || orig[0] != 1 || orig[1] != 2 {
			t.Fatalf("expected original nums unchanged, got %#v", orig)
		}
	})

	t.Run("non-array treated as single element", func(t *testing.T) {
		docs := []bson.M{
			{"_id": 1, "orders": "x"},
		}
		out, err := applyPipeline(docs, []bson.M{{"$unwind": "$orders"}})
		if err != nil {
			t.Fatalf("applyPipeline err: %v", err)
		}
		if len(out) != 1 || out[0]["orders"] != "x" {
			t.Fatalf("unexpected out: %#v", out)
		}
	})
}

func TestApplyPipeline_AddFields_FieldRef_NestedAndMissing(t *testing.T) {
	docs := []bson.M{
		{"_id": 1, "name": "a", "address": bson.M{"city": "NY"}},
		{"_id": 2, "address": bson.M{"city": nil}},
		{"_id": 3}, // missing address
	}
	pipeline := []bson.M{
		{"$addFields": bson.M{
			"cityCopy":     "$address.city",
			"missingCopy":  "$does.not.exist",
			"address.city": "LA",
		}},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if docs[0]["address"].(bson.M)["city"] != "NY" {
		t.Fatalf("expected original docs not mutated, got %#v", docs[0]["address"])
	}
	if out[0]["cityCopy"] != "NY" {
		t.Fatalf("expected cityCopy=NY, got %#v", out[0]["cityCopy"])
	}
	if out[0]["missingCopy"] != nil {
		t.Fatalf("expected missingCopy=null, got %#v", out[0]["missingCopy"])
	}
	if out[0]["address"].(bson.M)["city"] != "LA" {
		t.Fatalf("expected nested overwrite address.city=LA, got %#v", out[0]["address"])
	}
	if out[1]["cityCopy"] != nil {
		t.Fatalf("expected cityCopy=null for null input, got %#v", out[1]["cityCopy"])
	}
	if out[2]["cityCopy"] != nil {
		t.Fatalf("expected cityCopy=null for missing input, got %#v", out[2]["cityCopy"])
	}
	if out[2]["address"].(bson.M)["city"] != "LA" {
		t.Fatalf("expected missing nested path created with city=LA, got %#v", out[2]["address"])
	}
}

func TestApplyPipeline_AddFields_Size(t *testing.T) {
	docs := []bson.M{
		{"_id": 1, "arr": []interface{}{1, 2, 3}},
		{"_id": 2, "arr": []interface{}{}},
		{"_id": 3, "arr": "not-array"},
		{"_id": 4},             // missing
		{"_id": 5, "arr": nil}, // null
		{"_id": 6, "nested": bson.M{"arr": []interface{}{"a"}}},
	}
	pipeline := []bson.M{
		{"$addFields": bson.M{
			"arrLen":    bson.M{"$size": "$arr"},
			"nestedLen": bson.M{"$size": "$nested.arr"},
		}},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}

	if out[0]["arrLen"] != int64(3) {
		t.Fatalf("expected arrLen=3, got %#v", out[0]["arrLen"])
	}
	if out[1]["arrLen"] != int64(0) {
		t.Fatalf("expected arrLen=0, got %#v", out[1]["arrLen"])
	}
	if out[2]["arrLen"] != nil {
		t.Fatalf("expected arrLen=null for non-array, got %#v", out[2]["arrLen"])
	}
	if out[3]["arrLen"] != nil {
		t.Fatalf("expected arrLen=null for missing, got %#v", out[3]["arrLen"])
	}
	if out[4]["arrLen"] != nil {
		t.Fatalf("expected arrLen=null for null, got %#v", out[4]["arrLen"])
	}
	if out[5]["nestedLen"] != int64(1) {
		t.Fatalf("expected nestedLen=1, got %#v", out[5]["nestedLen"])
	}
}

func TestApplyPipeline_AddFields_CondIsArraySize(t *testing.T) {
	docs := []bson.M{
		{"_id": 1, "orders": bson.M{"items": []interface{}{"a", "b"}}},
		{"_id": 2, "orders": bson.M{"items": []interface{}{}}},
		{"_id": 3, "orders": bson.M{"items": "not-array"}},
		{"_id": 4, "orders": bson.M{}}, // missing items
		{"_id": 5},                     // missing orders
	}
	pipeline := []bson.M{
		{"$addFields": bson.M{
			"itemCount": bson.M{"$cond": []interface{}{
				bson.M{"$isArray": "$orders.items"},
				bson.M{"$size": "$orders.items"},
				int64(0),
			}},
		}},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if out[0]["itemCount"] != int64(2) {
		t.Fatalf("expected itemCount=2, got %#v", out[0]["itemCount"])
	}
	if out[1]["itemCount"] != int64(0) {
		t.Fatalf("expected itemCount=0, got %#v", out[1]["itemCount"])
	}
	if out[2]["itemCount"] != int64(0) {
		t.Fatalf("expected itemCount=0 for non-array, got %#v", out[2]["itemCount"])
	}
	if out[3]["itemCount"] != int64(0) {
		t.Fatalf("expected itemCount=0 for missing, got %#v", out[3]["itemCount"])
	}
	if out[4]["itemCount"] != int64(0) {
		t.Fatalf("expected itemCount=0 for missing orders, got %#v", out[4]["itemCount"])
	}
}

func TestApplyPipeline_SetStage_AliasOfAddFields(t *testing.T) {
	docs := []bson.M{
		{"city": "NY"},
		{"city": "SF"},
		{"city": "NY"},
	}
	pipeline := []bson.M{
		{"$group": bson.M{"_id": "$city", "n": bson.M{"$sum": 1}}},
		{"$set": bson.M{"city": "$_id"}},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(out))
	}
	if out[0]["_id"] != "NY" || out[0]["city"] != "NY" || out[0]["n"] != int64(2) {
		t.Fatalf("unexpected first group: %#v", out[0])
	}
	if out[1]["_id"] != "SF" || out[1]["city"] != "SF" || out[1]["n"] != int64(1) {
		t.Fatalf("unexpected second group: %#v", out[1])
	}
}

func TestApplyPipeline_Match_NestedComparison(t *testing.T) {
	docs := []bson.M{
		{"_id": 1, "orders": bson.M{"amount": 300}},
		{"_id": 2, "orders": bson.M{"amount": 150}},
		{"_id": 3}, // missing
		{"_id": 4, "orders": bson.M{"amount": nil}},
	}
	pipeline := []bson.M{
		{"$match": bson.M{"orders.amount": bson.M{"$gte": 200}}},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if len(out) != 1 || out[0]["_id"] != 1 {
		t.Fatalf("unexpected out: %#v", out)
	}
}

func TestApplyPipeline_AddFieldsThenMatch_Comparison(t *testing.T) {
	docs := []bson.M{
		{"_id": 1, "orders": bson.M{"amount": 300}},
		{"_id": 2, "orders": bson.M{"amount": 150}},
		{"_id": 3, "orders": bson.M{"amount": 200}},
		{"_id": 4}, // missing orders => order_amount becomes null => should not match
	}
	pipeline := []bson.M{
		{"$addFields": bson.M{"order_amount": "$orders.amount"}},
		{"$match": bson.M{"order_amount": bson.M{"$gte": 200}}},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if len(out) != 2 || out[0]["_id"] != 1 || out[1]["_id"] != 3 {
		t.Fatalf("unexpected out: %#v", out)
	}
}

func TestApplyPipeline_Group_AddToSet(t *testing.T) {
	docs := []bson.M{
		{"user_id": 1},
		{"user_id": 1},
		{"user_id": 2},
	}
	pipeline := []bson.M{
		{"$group": bson.M{"_id": nil, "users": bson.M{"$addToSet": "$user_id"}}},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(out))
	}
	users, ok := out[0]["users"].([]interface{})
	if !ok {
		t.Fatalf("expected users array, got %#v", out[0]["users"])
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 unique users, got %#v", users)
	}
	seen := map[string]bool{}
	for _, u := range users {
		seen[fmt.Sprint(u)] = true
	}
	if !seen["1"] || !seen["2"] {
		t.Fatalf("unexpected users: %#v", users)
	}
}

func TestApplyPipeline_Sample(t *testing.T) {
	docs := []bson.M{
		{"_id": 1},
		{"_id": 2},
		{"_id": 3},
	}

	out, err := applyPipeline(docs, []bson.M{{"$sample": bson.M{"size": 1}}})
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(out))
	}
	id := out[0]["_id"]
	if id != 1 && id != 2 && id != 3 {
		t.Fatalf("unexpected sampled doc: %#v", out[0])
	}

	out, err = applyPipeline(docs, []bson.M{{"$sample": bson.M{"size": 0}}})
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expected 0 docs, got %d", len(out))
	}

	out, err = applyPipeline(docs, []bson.M{{"$sample": bson.M{"size": 10}}})
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("expected 3 docs, got %d", len(out))
	}
}
