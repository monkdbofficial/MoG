package mongo

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

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

func TestApplyPipeline_AddFields_DatePartsAndArithmetic(t *testing.T) {
	t0 := time.Date(2024, time.March, 2, 3, 4, 5, 6*1_000_000, time.UTC)
	docs := []bson.M{
		{"_id": 1, "created_at": t0},
	}
	pipeline := []bson.M{
		{"$addFields": bson.M{
			"y":    bson.M{"$year": "$created_at"},
			"m":    bson.M{"$month": "$created_at"},
			"next": bson.M{"$add": []interface{}{"$created_at", int64(24 * 60 * 60 * 1000)}},
			"diff": bson.M{"$subtract": []interface{}{bson.M{"$add": []interface{}{"$created_at", int64(1000)}}, "$created_at"}},
		}},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if out[0]["y"] != int64(2024) {
		t.Fatalf("expected y=2024, got %#v", out[0]["y"])
	}
	if out[0]["m"] != int64(3) {
		t.Fatalf("expected m=3, got %#v", out[0]["m"])
	}
	next, ok := out[0]["next"].(time.Time)
	if !ok {
		t.Fatalf("expected next to be time.Time, got %#v", out[0]["next"])
	}
	if !next.Equal(t0.Add(24 * time.Hour)) {
		t.Fatalf("expected next=%v, got %v", t0.Add(24*time.Hour), next)
	}
	if out[0]["diff"] != int64(1000) {
		t.Fatalf("expected diff=1000, got %#v", out[0]["diff"])
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

func TestApplyPipeline_ProjectYear(t *testing.T) {
	docs := []bson.M{
		{"_id": 1, "created_at": time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)},
		{"_id": 2, "created_at": "2025-01-02T03:04:05Z"},
	}
	pipeline := []bson.M{
		{"$project": bson.M{"_id": 1, "y": bson.M{"$year": "$created_at"}, "m": bson.M{"$month": "$created_at"}}},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if out[0]["y"] != int64(2026) {
		t.Fatalf("expected y=2026, got %#v", out[0]["y"])
	}
	if out[0]["m"] != int64(3) {
		t.Fatalf("expected m=3, got %#v", out[0]["m"])
	}
	if out[1]["y"] != int64(2025) {
		t.Fatalf("expected y=2025, got %#v", out[1]["y"])
	}
	if out[1]["m"] != int64(1) {
		t.Fatalf("expected m=1, got %#v", out[1]["m"])
	}
}

func TestApplyPipeline_Facet(t *testing.T) {
	docs := []bson.M{
		{"_id": 1, "age": 30, "salary": 10.0, "created_at": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"_id": 2, "age": 30, "salary": 20.0, "created_at": time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"_id": 3, "age": 40, "salary": 50.0, "created_at": time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)},
	}

	pipeline := []bson.M{{
		"$facet": bson.M{
			"age_stats": []interface{}{
				bson.M{"$group": bson.M{"_id": "$age", "count": bson.M{"$sum": 1}}},
				bson.M{"$sort": bson.M{"count": -1}},
				bson.M{"$limit": 10},
			},
			"recent": []interface{}{
				bson.M{"$sort": bson.M{"created_at": -1}},
				bson.M{"$limit": 10},
			},
			"salary_stats": []interface{}{
				bson.M{"$group": bson.M{"_id": nil, "avg_salary": bson.M{"$avg": "$salary"}, "max_salary": bson.M{"$max": "$salary"}}},
			},
		},
	}}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(out))
	}
	ageStats, ok := out[0]["age_stats"].([]bson.M)
	if !ok || len(ageStats) == 0 {
		t.Fatalf("expected age_stats array, got %#v", out[0]["age_stats"])
	}
	recent, ok := out[0]["recent"].([]bson.M)
	if !ok || len(recent) == 0 {
		t.Fatalf("expected recent array, got %#v", out[0]["recent"])
	}
	if recent[0]["_id"] != 3 {
		t.Fatalf("expected most recent _id=3, got %#v", recent[0]["_id"])
	}
	salaryStats, ok := out[0]["salary_stats"].([]bson.M)
	if !ok || len(salaryStats) != 1 {
		t.Fatalf("expected salary_stats array len=1, got %#v", out[0]["salary_stats"])
	}
	if salaryStats[0]["max_salary"] != 50.0 {
		t.Fatalf("expected max_salary=50, got %#v", salaryStats[0]["max_salary"])
	}
}

func TestApplyPipeline_SetWindowFields_AvgAndRank(t *testing.T) {
	docs := []bson.M{
		{"_id": 1, "age": 30, "salary": 20.0},
		{"_id": 2, "age": 30, "salary": 10.0},
		{"_id": 3, "age": 30, "salary": 20.0},
		{"_id": 4, "age": 40, "salary": 5.0},
	}

	pipeline := []bson.M{{
		"$setWindowFields": bson.M{
			"partitionBy": "$age",
			"sortBy":      bson.M{"salary": 1},
			"output": bson.M{
				"avgSalary": bson.M{"$avg": "$salary", "window": bson.M{"documents": []interface{}{"unbounded", "current"}}},
				"rank":      bson.M{"$rank": bson.M{}},
			},
		},
	}}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}

	byID := map[interface{}]bson.M{}
	for _, d := range out {
		byID[d["_id"]] = d
	}

	// Partition age=30 sorted salaries: 10 (rank 1, avg 10), 20 (rank 2, avg 15), 20 (rank 2, avg 16.666...)
	if byID[2]["rank"] != int64(1) {
		t.Fatalf("expected _id=2 rank=1, got %#v", byID[2]["rank"])
	}
	if byID[1]["rank"] != int64(2) || byID[3]["rank"] != int64(2) {
		t.Fatalf("expected _id=1 and _id=3 rank=2, got %#v %#v", byID[1]["rank"], byID[3]["rank"])
	}

	avg2, _ := byID[2]["avgSalary"].(float64)
	avg1, _ := byID[1]["avgSalary"].(float64)
	avg3, _ := byID[3]["avgSalary"].(float64)
	if math.Abs(avg2-10.0) > 1e-9 {
		t.Fatalf("expected _id=2 avgSalary=10, got %#v", byID[2]["avgSalary"])
	}
	if math.Abs(avg1-15.0) > 1e-9 {
		t.Fatalf("expected _id=1 avgSalary=15, got %#v", byID[1]["avgSalary"])
	}
	if math.Abs(avg3-(50.0/3.0)) > 1e-9 {
		t.Fatalf("expected _id=3 avgSalary=16.666..., got %#v", byID[3]["avgSalary"])
	}
}

func TestApplyPipeline_GraphLookup_ManagerChain(t *testing.T) {
	base := []bson.M{
		{"_id": 10, "manager_id": 3},
	}
	foreign := []bson.M{
		{"user_id": 1, "manager_id": nil},
		{"user_id": 2, "manager_id": 1},
		{"user_id": 3, "manager_id": 2},
		{"user_id": 4, "manager_id": 2},
	}
	resolver := func(from string) ([]bson.M, error) {
		if from != "users" {
			return nil, fmt.Errorf("unexpected from: %s", from)
		}
		return foreign, nil
	}

	pipeline := []bson.M{{
		"$graphLookup": bson.M{
			"from":             "users",
			"startWith":        "$manager_id",
			"connectFromField": "manager_id",
			"connectToField":   "user_id",
			"maxDepth":         3,
			"as":               "hierarchy",
		},
	}}

	out, err := applyPipelineWithLookup(base, pipeline, resolver)
	if err != nil {
		t.Fatalf("applyPipelineWithLookup err: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(out))
	}
	h, ok := out[0]["hierarchy"].([]bson.M)
	if !ok {
		t.Fatalf("expected hierarchy array, got %#v", out[0]["hierarchy"])
	}
	seen := map[interface{}]bool{}
	for _, d := range h {
		seen[d["user_id"]] = true
	}
	if !seen[3] || !seen[2] || !seen[1] {
		t.Fatalf("expected hierarchy to include user_id 3->2->1, got %#v", h)
	}
}

func TestApplyPipeline_Project_ArithmeticStringBoolArrayDate(t *testing.T) {
	docs := []bson.M{
		{
			"_id":        1,
			"a":          10,
			"b":          3,
			"name":       "  Alice  ",
			"tags":       []interface{}{"x", "y", "z"},
			"created_at": "2026-03-28T10:11:12Z",
		},
	}

	pipeline := []bson.M{
		{"$project": bson.M{
			"_id": 1,
			"abs": bson.M{"$abs": -5},
			"add": bson.M{"$add": []interface{}{"$a", "$b", 1}},
			"div": bson.M{"$divide": []interface{}{"$a", "$b"}},
			"mod": bson.M{"$mod": []interface{}{"$a", "$b"}},
			"pow": bson.M{"$pow": []interface{}{2, 3}},
			"rnd": bson.M{"$round": []interface{}{3.14159, 2}},
			"trc": bson.M{"$trunc": []interface{}{3.14159, 2}},

			"concat": bson.M{"$concat": []interface{}{"Hi ", "Bob"}},
			"lower":  bson.M{"$toLower": "AbC"},
			"upper":  bson.M{"$toUpper": "aBc"},
			"trim":   bson.M{"$trim": bson.M{"input": "$name"}},
			"split":  bson.M{"$split": []interface{}{"a,b,c", ","}},
			"repl":   bson.M{"$replaceAll": bson.M{"input": "a-a-a", "find": "-", "replacement": "_"}},
			"lenb":   bson.M{"$strLenBytes": "hey"},

			"eq":  bson.M{"$eq": []interface{}{"$a", 10}},
			"gt":  bson.M{"$gt": []interface{}{"$a", 9}},
			"cmp": bson.M{"$cmp": []interface{}{"$a", "$b"}},

			"ifNullA": bson.M{"$ifNull": []interface{}{"$missing", 123}},
			"sw": bson.M{"$switch": bson.M{
				"branches": []interface{}{
					bson.M{"case": bson.M{"$gt": []interface{}{"$a", 100}}, "then": "big"},
					bson.M{"case": bson.M{"$gt": []interface{}{"$a", 5}}, "then": "mid"},
				},
				"default": "small",
			}},
			"and": bson.M{"$and": []interface{}{true, bson.M{"$eq": []interface{}{"$b", 3}}}},
			"or":  bson.M{"$or": []interface{}{false, bson.M{"$eq": []interface{}{"$b", 4}}, true}},
			"not": bson.M{"$not": []interface{}{false}},

			"in":    bson.M{"$in": []interface{}{"y", "$tags"}},
			"elem":  bson.M{"$arrayElemAt": []interface{}{"$tags", 1}},
			"slice": bson.M{"$slice": []interface{}{"$tags", 1, 2}},
			"rev":   bson.M{"$reverseArray": "$tags"},
			"rng":   bson.M{"$range": []interface{}{1, 5, 2}},

			"dt":   bson.M{"$toDate": "$created_at"},
			"year": bson.M{"$year": bson.M{"$toDate": "$created_at"}},
			"mon":  bson.M{"$month": bson.M{"$toDate": "$created_at"}},
			"dom":  bson.M{"$dayOfMonth": bson.M{"$toDate": "$created_at"}},
			"dow":  bson.M{"$dayOfWeek": bson.M{"$toDate": "$created_at"}},
			"doy":  bson.M{"$dayOfYear": bson.M{"$toDate": "$created_at"}},
			"hr":   bson.M{"$hour": bson.M{"$toDate": "$created_at"}},
			"min":  bson.M{"$minute": bson.M{"$toDate": "$created_at"}},
			"sec":  bson.M{"$second": bson.M{"$toDate": "$created_at"}},
			"ms":   bson.M{"$millisecond": bson.M{"$toDate": "$created_at"}},
			"wk":   bson.M{"$week": bson.M{"$toDate": "$created_at"}},
		}},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	got := out[0]

	if got["abs"] != 5.0 {
		t.Fatalf("abs: %#v", got["abs"])
	}
	if got["add"] != 14.0 {
		t.Fatalf("add: %#v", got["add"])
	}
	if got["div"] != (10.0 / 3.0) {
		t.Fatalf("div: %#v", got["div"])
	}
	if got["mod"] != 1.0 {
		t.Fatalf("mod: %#v", got["mod"])
	}
	if got["pow"] != 8.0 {
		t.Fatalf("pow: %#v", got["pow"])
	}
	if got["rnd"] != 3.14 {
		t.Fatalf("rnd: %#v", got["rnd"])
	}
	if got["trc"] != 3.14 {
		t.Fatalf("trc: %#v", got["trc"])
	}
	if got["concat"] != "Hi Bob" {
		t.Fatalf("concat: %#v", got["concat"])
	}
	if got["lower"] != "abc" || got["upper"] != "ABC" {
		t.Fatalf("case: lower=%#v upper=%#v", got["lower"], got["upper"])
	}
	if got["trim"] != "Alice" {
		t.Fatalf("trim: %#v", got["trim"])
	}
	if got["repl"] != "a_a_a" {
		t.Fatalf("repl: %#v", got["repl"])
	}
	if got["lenb"] != int64(3) {
		t.Fatalf("lenb: %#v", got["lenb"])
	}
	if got["eq"] != true || got["gt"] != true {
		t.Fatalf("compare: eq=%#v gt=%#v", got["eq"], got["gt"])
	}
	if got["ifNullA"] != 123 {
		t.Fatalf("ifNullA: %#v", got["ifNullA"])
	}
	if got["sw"] != "mid" {
		t.Fatalf("switch: %#v", got["sw"])
	}
	if got["and"] != true || got["or"] != true || got["not"] != true {
		t.Fatalf("bool: and=%#v or=%#v not=%#v", got["and"], got["or"], got["not"])
	}
	if got["in"] != true {
		t.Fatalf("in: %#v", got["in"])
	}
	if got["elem"] != "y" {
		t.Fatalf("elem: %#v", got["elem"])
	}
	if sl, ok := got["slice"].([]interface{}); !ok || len(sl) != 2 || sl[0] != "y" || sl[1] != "z" {
		t.Fatalf("slice: %#v", got["slice"])
	}
	if rg, ok := got["rng"].([]interface{}); !ok || len(rg) != 2 || rg[0] != int64(1) || rg[1] != int64(3) {
		t.Fatalf("range: %#v", got["rng"])
	}
	if _, ok := got["dt"].(time.Time); !ok {
		t.Fatalf("toDate: %#v", got["dt"])
	}
	if got["year"] != int64(2026) || got["mon"] != int64(3) || got["dom"] != int64(28) {
		t.Fatalf("date parts: year=%#v mon=%#v dom=%#v", got["year"], got["mon"], got["dom"])
	}
}

func TestApplyPipeline_Project_Phase2_MapFilterReduceSortZipRegexSubstrDate(t *testing.T) {
	docs := []bson.M{
		{
			"_id":  1,
			"nums": []interface{}{int64(1), int64(2), int64(3)},
			"arr":  []interface{}{int64(3), int64(1), int64(2)},
			"s":    "Äbc",
			"ds":   "2026-03-28T10:11:12Z",
		},
	}

	pipeline := []bson.M{
		{"$project": bson.M{
			"_id": 1,
			"mapInc": bson.M{"$map": bson.M{
				"input": "$nums",
				"as":    "n",
				"in":    bson.M{"$add": []interface{}{"$$n", 1}},
			}},
			"filterGt1": bson.M{"$filter": bson.M{
				"input": "$nums",
				"as":    "n",
				"cond":  bson.M{"$gt": []interface{}{"$$n", 1}},
			}},
			"reduceSum": bson.M{"$reduce": bson.M{
				"input":        "$nums",
				"initialValue": 0,
				"in":           bson.M{"$add": []interface{}{"$$value", "$$this"}},
			}},
			"sorted": bson.M{"$sortArray": bson.M{"input": "$arr", "sortBy": 1}},
			"zipped": bson.M{"$zip": bson.M{"inputs": []interface{}{"$nums", "$arr"}}},
			"rx":     bson.M{"$regexMatch": bson.M{"input": "hello", "regex": "^he"}},
			"subcp":  bson.M{"$substrCP": []interface{}{"$s", 0, 2}},
			"lencp":  bson.M{"$strLenCP": "$s"},
			"idxb":   bson.M{"$indexOfBytes": []interface{}{"abcabc", "ca"}},
			"idxcp":  bson.M{"$indexOfCP": []interface{}{"$s", "b"}},
			"dt":     bson.M{"$dateFromString": bson.M{"dateString": "$ds"}},
			"dstr":   bson.M{"$dateToString": bson.M{"date": bson.M{"$dateFromString": bson.M{"dateString": "$ds"}}, "format": "%Y-%m-%d"}},
			"dadd": bson.M{"$dateToString": bson.M{
				"date":   bson.M{"$dateAdd": bson.M{"startDate": bson.M{"$dateFromString": bson.M{"dateString": "$ds"}}, "unit": "day", "amount": 1}},
				"format": "%Y-%m-%d",
			}},
			"dtrHr": bson.M{"$hour": bson.M{"$dateTrunc": bson.M{"date": bson.M{"$dateFromString": bson.M{"dateString": "$ds"}}, "unit": "hour"}}},
		}},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	got := out[0]

	if inc, ok := got["mapInc"].([]interface{}); !ok || len(inc) != 3 || inc[0] != 2.0 || inc[1] != 3.0 || inc[2] != 4.0 {
		t.Fatalf("mapInc: %#v", got["mapInc"])
	}
	if f, ok := got["filterGt1"].([]interface{}); !ok || len(f) != 2 || fmt.Sprint(f[0]) != "2" || fmt.Sprint(f[1]) != "3" {
		t.Fatalf("filterGt1: %#v", got["filterGt1"])
	}
	if got["reduceSum"] != 6.0 {
		t.Fatalf("reduceSum: %#v", got["reduceSum"])
	}
	if s, ok := got["sorted"].([]interface{}); !ok || len(s) != 3 || fmt.Sprint(s[0]) != "1" || fmt.Sprint(s[1]) != "2" || fmt.Sprint(s[2]) != "3" {
		t.Fatalf("sorted: %#v", got["sorted"])
	}
	if got["rx"] != true {
		t.Fatalf("regexMatch: %#v", got["rx"])
	}
	if got["subcp"] != "Äb" || got["lencp"] != int64(3) {
		t.Fatalf("substr/len: subcp=%#v lencp=%#v", got["subcp"], got["lencp"])
	}
	if got["idxb"] != int64(2) || got["idxcp"] != int64(1) {
		t.Fatalf("indexOf: idxb=%#v idxcp=%#v", got["idxb"], got["idxcp"])
	}
	if got["dstr"] != "2026-03-28" || got["dadd"] != "2026-03-29" {
		t.Fatalf("date strings: dstr=%#v dadd=%#v", got["dstr"], got["dadd"])
	}
	if got["dtrHr"] != int64(10) {
		t.Fatalf("dateTrunc hour: %#v", got["dtrHr"])
	}
	if _, ok := got["dt"].(time.Time); !ok {
		t.Fatalf("dateFromString: %#v", got["dt"])
	}
}

func TestApplyPipeline_SetWindowFields_DenseRank_DocNum_Shift(t *testing.T) {
	docs := []bson.M{
		{"_id": 1, "age": 30, "salary": 10.0},
		{"_id": 2, "age": 30, "salary": 20.0},
		{"_id": 3, "age": 30, "salary": 20.0},
	}
	pipeline := []bson.M{{
		"$setWindowFields": bson.M{
			"partitionBy": "$age",
			"sortBy":      bson.M{"salary": 1},
			"output": bson.M{
				"dense": bson.M{"$denseRank": bson.M{}},
				"num":   bson.M{"$documentNumber": bson.M{}},
				"prev":  bson.M{"$shift": bson.M{"output": "$salary", "by": -1, "default": nil}},
			},
		},
	}}
	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	byID := map[interface{}]bson.M{}
	for _, d := range out {
		byID[d["_id"]] = d
	}
	// Sorted salaries: 10,20,20 => dense ranks 1,2,2; doc numbers 1,2,3; prev of first is nil.
	if byID[1]["dense"] != int64(1) || byID[2]["dense"] != int64(2) || byID[3]["dense"] != int64(2) {
		t.Fatalf("dense: %#v %#v %#v", byID[1]["dense"], byID[2]["dense"], byID[3]["dense"])
	}
	if byID[1]["num"] != int64(1) || byID[2]["num"] != int64(2) || byID[3]["num"] != int64(3) {
		t.Fatalf("num: %#v %#v %#v", byID[1]["num"], byID[2]["num"], byID[3]["num"])
	}
	if byID[1]["prev"] != nil || byID[2]["prev"] != 10.0 || byID[3]["prev"] != 20.0 {
		t.Fatalf("prev: %#v %#v %#v", byID[1]["prev"], byID[2]["prev"], byID[3]["prev"])
	}
}

func TestApplyPipeline_Project_Phase3_ObjectConvertRegexFindDatePartsIsoIndexOfArray(t *testing.T) {
	docs := []bson.M{
		{
			"_id":     1,
			"obj":     bson.M{"a": 1, "b": 2},
			"arr":     []interface{}{"x", "y", "z"},
			"msg":     "Hello hello",
			"created": "2026-03-28T10:11:12Z",
		},
	}

	pipeline := []bson.M{
		{"$project": bson.M{
			"_id":    1,
			"getA":   bson.M{"$getField": bson.M{"field": "a", "input": "$obj"}},
			"setC":   bson.M{"$setField": bson.M{"field": "c", "input": "$obj", "value": 3}},
			"unsetB": bson.M{"$unsetField": bson.M{"field": "b", "input": "$obj"}},
			"merge":  bson.M{"$mergeObjects": []interface{}{"$obj", bson.M{"b": 10, "d": 4}}},
			"o2a":    bson.M{"$objectToArray": "$obj"},
			"a2o": bson.M{"$arrayToObject": []interface{}{
				bson.M{"k": "x", "v": 1},
				[]interface{}{"y", 2},
			}},
			"idxArr":     bson.M{"$indexOfArray": []interface{}{"$arr", "y"}},
			"strcasecmp": bson.M{"$strcasecmp": []interface{}{"AbC", "aBc"}},

			"rxFind":    bson.M{"$regexFind": bson.M{"input": "$msg", "regex": "hello", "options": "i"}},
			"rxFindAll": bson.M{"$regexFindAll": bson.M{"input": "$msg", "regex": "hello", "options": "i"}},

			"toInt":    bson.M{"$toInt": "123"},
			"toDouble": bson.M{"$toDouble": "1.25"},
			"toBool":   bson.M{"$toBool": 0},
			"toStr":    bson.M{"$toString": bson.M{"$toDate": "$created"}},
			"typ":      bson.M{"$type": "$obj"},
			"conv": bson.M{"$convert": bson.M{
				"input":   "5",
				"to":      "int",
				"onError": -1,
			}},

			"parts": bson.M{"$dateToParts": bson.M{"date": bson.M{"$toDate": "$created"}}},
			"fromParts": bson.M{"$dateFromParts": bson.M{
				"year":  2026,
				"month": 3,
				"day":   28,
				"hour":  10,
			}},
			"diffH": bson.M{"$dateDiff": bson.M{"startDate": bson.M{"$toDate": "$created"}, "endDate": bson.M{"$dateAdd": bson.M{"startDate": bson.M{"$toDate": "$created"}, "unit": "hour", "amount": 3}}, "unit": "hour"}},
			"isoW":  bson.M{"$isoWeek": bson.M{"$toDate": "$created"}},
		}},
	}

	out, err := applyPipeline(docs, pipeline)
	if err != nil {
		t.Fatalf("applyPipeline err: %v", err)
	}
	got := out[0]

	if got["getA"] != 1 {
		t.Fatalf("getA: %#v", got["getA"])
	}
	if got["idxArr"] != int64(1) {
		t.Fatalf("idxArr: %#v", got["idxArr"])
	}
	if got["strcasecmp"] != int64(0) {
		t.Fatalf("strcasecmp: %#v", got["strcasecmp"])
	}
	if got["toInt"] != int64(123) || got["toDouble"] != 1.25 || got["toBool"] != false || got["conv"] != int64(5) {
		t.Fatalf("convert: toInt=%#v toDouble=%#v toBool=%#v conv=%#v", got["toInt"], got["toDouble"], got["toBool"], got["conv"])
	}
	if got["typ"] != "object" {
		t.Fatalf("type: %#v", got["typ"])
	}
	if got["diffH"] != int64(3) {
		t.Fatalf("dateDiff: %#v", got["diffH"])
	}
	if _, ok := got["isoW"].(int64); !ok {
		t.Fatalf("isoWeek: %#v", got["isoW"])
	}

	rxFind, ok := got["rxFind"].(bson.M)
	if !ok || rxFind["match"] == nil {
		t.Fatalf("regexFind: %#v", got["rxFind"])
	}
	rxAll, ok := got["rxFindAll"].([]interface{})
	if !ok || len(rxAll) != 2 {
		t.Fatalf("regexFindAll: %#v", got["rxFindAll"])
	}

	parts, ok := got["parts"].(bson.M)
	if !ok || parts["year"] != int64(2026) || parts["month"] != int64(3) {
		t.Fatalf("dateToParts: %#v", got["parts"])
	}
	if _, ok := got["fromParts"].(time.Time); !ok {
		t.Fatalf("dateFromParts: %#v", got["fromParts"])
	}
	if s, ok := got["toStr"].(string); !ok || !strings.HasPrefix(s, "2026-03-28T") {
		t.Fatalf("toString(date): %#v", got["toStr"])
	}

	// setField/unsetField/mergeObjects are structural; spot-check keys.
	setC, _ := got["setC"].(bson.M)
	if setC["c"] != 3 {
		t.Fatalf("setC: %#v", got["setC"])
	}
	unsetB, _ := got["unsetB"].(bson.M)
	if _, ok := unsetB["b"]; ok {
		t.Fatalf("unsetB: %#v", got["unsetB"])
	}
	merge, _ := got["merge"].(bson.M)
	if merge["b"] != 10 || merge["d"] != 4 {
		t.Fatalf("merge: %#v", got["merge"])
	}
	a2o, _ := got["a2o"].(bson.M)
	if a2o["x"] != 1 || a2o["y"] != 2 {
		t.Fatalf("arrayToObject: %#v", got["a2o"])
	}
	o2a, ok := got["o2a"].([]interface{})
	if !ok || len(o2a) == 0 {
		t.Fatalf("objectToArray: %#v", got["o2a"])
	}
}
