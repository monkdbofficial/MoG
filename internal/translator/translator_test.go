package translator

import (
	"strings"
	"testing"

	"gopkg.in/mgo.v2/bson"
)

// TestTranslateUpdate_Inc runs the corresponding test case.
func TestTranslateUpdate_Inc(t *testing.T) {
	tr := New()
	sql, args, err := tr.TranslateUpdate("db__col", bson.M{"age": bson.M{"$lt": 30}}, bson.M{"$inc": bson.M{"score": 5}})
	if err != nil {
		t.Fatalf("TranslateUpdate err: %v", err)
	}
	if !strings.Contains(sql, "SET data['score']") || !strings.Contains(sql, "COALESCE") {
		t.Fatalf("unexpected sql: %s", sql)
	}
	if !strings.Contains(sql, "CAST(data['age'] AS DOUBLE PRECISION) < $1") {
		t.Fatalf("unexpected sql: %s", sql)
	}
	if len(args) == 0 {
		t.Fatalf("expected args")
	}
}

// TestTranslateUpdate_Set_BsonD runs the corresponding test case.
func TestTranslateUpdate_Set_BsonD(t *testing.T) {
	tr := New()
	sql, args, err := tr.TranslateUpdate(
		"db__col",
		bson.M{"name": "Alice"},
		bson.M{"$set": bson.D{{Name: "score", Value: 85}}},
	)
	if err != nil {
		t.Fatalf("TranslateUpdate err: %v", err)
	}
	if !strings.Contains(sql, "SET data['score'] =") || !strings.Contains(sql, "WHERE data['name'] = $1") {
		t.Fatalf("unexpected sql: %s", sql)
	}
	if len(args) == 0 {
		t.Fatalf("expected args")
	}
}

// TestTranslateFind_NestedField runs the corresponding test case.
func TestTranslateFind_NestedField(t *testing.T) {
	tr := New()
	sql, _, err := tr.TranslateFind("db__col", bson.M{"orders.amount": bson.M{"$gte": 200}})
	if err != nil {
		t.Fatalf("TranslateFind err: %v", err)
	}
	if !strings.Contains(sql, "data['orders']['amount']") {
		t.Fatalf("unexpected sql: %s", sql)
	}
}

// TestTranslateFindWithOptions_SortLimitSkip_In runs the corresponding test case.
func TestTranslateFindWithOptions_SortLimitSkip_In(t *testing.T) {
	tr := New()
	sql, args, err := tr.TranslateFindWithOptions(
		"db__col",
		bson.M{"status": bson.M{"$in": []interface{}{"paid", "shipped"}}},
		bson.M{"total": -1},
		10,
		5,
	)
	if err != nil {
		t.Fatalf("TranslateFindWithOptions err: %v", err)
	}
	if len(args) != 2 || args[0] != "paid" || args[1] != "shipped" {
		t.Fatalf("unexpected args: %#v", args)
	}
	if !strings.Contains(sql, "SELECT data FROM doc.db__col") {
		t.Fatalf("unexpected sql: %s", sql)
	}
	if !strings.Contains(sql, "data['status'] IN ($1, $2)") {
		t.Fatalf("unexpected sql: %s", sql)
	}
	if !strings.Contains(sql, "ORDER BY data['total'] DESC") {
		t.Fatalf("unexpected sql: %s", sql)
	}
	if !strings.Contains(sql, "LIMIT 5") || !strings.Contains(sql, "OFFSET 10") {
		t.Fatalf("unexpected sql: %s", sql)
	}
}

// TestTranslateCount_NoFilter runs the corresponding test case.
func TestTranslateCount_NoFilter(t *testing.T) {
	tr := New()
	sql, args, err := tr.TranslateCount("db__col", bson.M{})
	if err != nil {
		t.Fatalf("TranslateCount err: %v", err)
	}
	if !strings.Contains(sql, "COUNT(*)") {
		t.Fatalf("unexpected sql: %s", sql)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args, got %d", len(args))
	}
}

// TestTranslateDelete_NoFilter runs the corresponding test case.
func TestTranslateDelete_NoFilter(t *testing.T) {
	tr := New()
	sql, args, err := tr.TranslateDelete("db__col", bson.M{})
	if err != nil {
		t.Fatalf("TranslateDelete err: %v", err)
	}
	if strings.Contains(sql, "WHERE") {
		t.Fatalf("expected no WHERE for empty filter, got: %s", sql)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args, got %d", len(args))
	}
}

// TestTranslateAggregatePlan_GroupSortLimit runs the corresponding test case.
func TestTranslateAggregatePlan_GroupSortLimit(t *testing.T) {
	tr := New()
	plan, err := tr.TranslateAggregatePlan("db__col", []bson.M{
		{"$match": bson.M{"status": "paid"}},
		{"$group": bson.M{"_id": "$user_id", "n": bson.M{"$sum": 1}}},
		{"$sort": bson.M{"n": -1}},
		{"$limit": 10},
	})
	if err != nil {
		t.Fatalf("TranslateAggregatePlan err: %v", err)
	}
	if plan.SQL == "" {
		t.Fatalf("expected SQL")
	}
	if len(plan.Args) != 1 || plan.Args[0] != "paid" {
		t.Fatalf("unexpected args: %#v", plan.Args)
	}
	if len(plan.Fields) != 2 || plan.Fields[0] != "_id" || plan.Fields[1] != "n" {
		t.Fatalf("unexpected fields: %#v", plan.Fields)
	}
	// SQL shape checks (avoid full-string match due to map iteration ordering).
	if !strings.Contains(plan.SQL, "FROM doc.db__col") {
		t.Fatalf("unexpected sql: %s", plan.SQL)
	}
	if !strings.Contains(plan.SQL, "WHERE data['status'] = $1") {
		t.Fatalf("unexpected sql: %s", plan.SQL)
	}
	if !strings.Contains(plan.SQL, "GROUP BY data['user_id']") {
		t.Fatalf("unexpected sql: %s", plan.SQL)
	}
	if !strings.Contains(plan.SQL, "ORDER BY n DESC") {
		t.Fatalf("unexpected sql: %s", plan.SQL)
	}
	if !strings.Contains(plan.SQL, "LIMIT 10") {
		t.Fatalf("unexpected sql: %s", plan.SQL)
	}
}

// TestTranslateAggregatePlan_NoGroup_SortLimit runs the corresponding test case.
func TestTranslateAggregatePlan_NoGroup_SortLimit(t *testing.T) {
	tr := New()
	plan, err := tr.TranslateAggregatePlan("db__col", []bson.M{
		{"$match": bson.M{"status": "paid"}},
		{"$sort": bson.M{"total": -1}},
		{"$limit": int64(5)},
	})
	if err != nil {
		t.Fatalf("TranslateAggregatePlan err: %v", err)
	}
	if len(plan.Fields) != 1 || plan.Fields[0] != "data" {
		t.Fatalf("unexpected fields: %#v", plan.Fields)
	}
	if !strings.Contains(plan.SQL, "SELECT data FROM doc.db__col") {
		t.Fatalf("unexpected sql: %s", plan.SQL)
	}
	if !strings.Contains(plan.SQL, "WHERE data['status'] = $1") {
		t.Fatalf("unexpected sql: %s", plan.SQL)
	}
	if !strings.Contains(plan.SQL, "ORDER BY data['total'] DESC") {
		t.Fatalf("unexpected sql: %s", plan.SQL)
	}
	if !strings.Contains(plan.SQL, "LIMIT 5") {
		t.Fatalf("unexpected sql: %s", plan.SQL)
	}
}

// TestTranslateAggregatePrefixPlan_PushdownThenStop runs the corresponding test case.
func TestTranslateAggregatePrefixPlan_PushdownThenStop(t *testing.T) {
	tr := New()
	p, err := tr.TranslateAggregatePrefixPlan("db__col", []bson.M{
		{"$match": bson.M{"status": "paid"}},
		{"$group": bson.M{"_id": "$user_id", "n": bson.M{"$sum": 1}}},
		{"$sort": bson.M{"n": -1}},
		{"$limit": 10},
		{"$project": bson.M{"n": 1, "_id": 0}}, // not supported for SQL pushdown
	})
	if err != nil {
		t.Fatalf("TranslateAggregatePrefixPlan err: %v", err)
	}
	if p == nil || p.Plan == nil {
		t.Fatalf("expected plan")
	}
	if p.ConsumedStages != 4 {
		t.Fatalf("expected consumed=4, got %d", p.ConsumedStages)
	}
	if !strings.Contains(p.Plan.SQL, "GROUP BY data['user_id']") {
		t.Fatalf("unexpected sql: %s", p.Plan.SQL)
	}
	if !strings.Contains(p.Plan.SQL, "ORDER BY n DESC") {
		t.Fatalf("unexpected sql: %s", p.Plan.SQL)
	}
	if !strings.Contains(p.Plan.SQL, "LIMIT 10") {
		t.Fatalf("unexpected sql: %s", p.Plan.SQL)
	}
}

// TestTranslateAggregatePrefixPlan_Count runs the corresponding test case.
func TestTranslateAggregatePrefixPlan_Count(t *testing.T) {
	tr := New()
	p, err := tr.TranslateAggregatePrefixPlan("db__col", []bson.M{
		{"$match": bson.M{"status": "paid"}},
		{"$count": "n"},
		{"$project": bson.M{"n": 1}}, // should not be pushed down after count
	})
	if err != nil {
		t.Fatalf("TranslateAggregatePrefixPlan err: %v", err)
	}
	if p.ConsumedStages != 2 {
		t.Fatalf("expected consumed=2, got %d", p.ConsumedStages)
	}
	if len(p.Plan.Fields) != 1 || p.Plan.Fields[0] != "n" {
		t.Fatalf("unexpected fields: %#v", p.Plan.Fields)
	}
	if !strings.Contains(p.Plan.SQL, "SELECT COUNT(*) AS n") {
		t.Fatalf("unexpected sql: %s", p.Plan.SQL)
	}
}

// TestTranslateAggregatePlan_GroupNullID_NoGroupBy runs the corresponding test case.
func TestTranslateAggregatePlan_GroupNullID_NoGroupBy(t *testing.T) {
	tr := New()
	plan, err := tr.TranslateAggregatePlan("db__col", []bson.M{
		{"$match": bson.M{"status": "paid"}},
		{"$group": bson.M{"_id": nil, "total": bson.M{"$sum": "$score"}, "count": bson.M{"$sum": 1}}},
	})
	if err != nil {
		t.Fatalf("TranslateAggregatePlan err: %v", err)
	}
	if strings.Contains(plan.SQL, "GROUP BY NULL") {
		t.Fatalf("unexpected sql: %s", plan.SQL)
	}
	if !strings.Contains(plan.SQL, "HAVING COUNT(*) > 0") {
		t.Fatalf("unexpected sql: %s", plan.SQL)
	}
}

// TestTranslateAggregatePrefixPlan_GroupNullID_NoGroupBy runs the corresponding test case.
func TestTranslateAggregatePrefixPlan_GroupNullID_NoGroupBy(t *testing.T) {
	tr := New()
	p, err := tr.TranslateAggregatePrefixPlan("db__col", []bson.M{
		{"$match": bson.M{"status": "paid"}},
		{"$group": bson.M{"_id": nil, "total": bson.M{"$sum": "$score"}, "count": bson.M{"$sum": 1}}},
		{"$project": bson.M{"_id": 0, "total": 1, "count": 1}},
	})
	if err != nil {
		t.Fatalf("TranslateAggregatePrefixPlan err: %v", err)
	}
	if p.ConsumedStages != 2 {
		t.Fatalf("expected consumed=2, got %d", p.ConsumedStages)
	}
	if strings.Contains(p.Plan.SQL, "GROUP BY NULL") {
		t.Fatalf("unexpected sql: %s", p.Plan.SQL)
	}
	if !strings.Contains(p.Plan.SQL, "HAVING COUNT(*) > 0") {
		t.Fatalf("unexpected sql: %s", p.Plan.SQL)
	}
}
