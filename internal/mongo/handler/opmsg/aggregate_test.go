package opmsg

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"gopkg.in/mgo.v2/bson"
	"strings"
	"testing"
	"time"

	"mog/internal/mongo/handler/relational"
)

// TestParseVectorLimit runs the corresponding test case.
func TestParseVectorLimit(t *testing.T) {
	if got, err := parseVectorLimit(int64(5)); err != nil || got != 5 {
		t.Fatalf("expected limit 5, got %d err %v", got, err)
	}
	if _, err := parseVectorLimit("bad"); err == nil {
		t.Fatalf("expected error for invalid limit")
	}
}

// TestBuildVectorSearchSQL runs the corresponding test case.
func TestBuildVectorSearchSQL(t *testing.T) {
	got := buildVectorSearchSQL("testcol", []string{"id", vectorScoreAliasPrefix + "score"}, "embedding", "[0.1,0.2]", 4, 3, "cosine", nil)
	want := `SELECT id, VECTOR_SIMILARITY(embedding, [0.1,0.2]::float_vector(4), 'cosine') AS "score"
FROM doc.testcol
WHERE KNN_MATCH(embedding, [0.1,0.2]::float_vector(4), 3, 'cosine')
ORDER BY "score" DESC
LIMIT 3`
	if got != want {
		t.Fatalf("unexpected vector search SQL:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

// TestBuildVectorSearchSQL_WithPrefilter runs the corresponding test case.
func TestBuildVectorSearchSQL_WithPrefilter(t *testing.T) {
	got := buildVectorSearchSQL("testcol", []string{"id", vectorScoreAliasPrefix + "score"}, "embedding", "[0.1,0.2]", 4, 3, "cosine", &relational.Where{SQL: "CAST(age AS DOUBLE PRECISION) >= $1"})
	want := `SELECT id, VECTOR_SIMILARITY(embedding, [0.1,0.2]::float_vector(4), 'cosine') AS "score"
FROM doc.testcol
WHERE KNN_MATCH(embedding, [0.1,0.2]::float_vector(4), 3, 'cosine') AND CAST(age AS DOUBLE PRECISION) >= $1
ORDER BY "score" DESC
LIMIT 3`
	if got != want {
		t.Fatalf("unexpected vector search SQL with prefilter:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

// TestBuildAggregatePrefilterPlan_PartialMatchPushdownKeepsResidualMatch runs the corresponding test case.
func TestBuildAggregatePrefilterPlan_PartialMatchPushdownKeepsResidualMatch(t *testing.T) {
	plan, ok, err := buildAggregatePrefilterPlan("test__users", []bson.M{
		{"$match": bson.M{"age": bson.M{"$gt": 25}, "complex": bson.M{"nested": "x"}}},
		{"$sort": bson.M{"age": -1}},
		{"$limit": 5},
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ok {
		t.Fatalf("expected prefilter plan")
	}
	wantSQL := "SELECT * FROM doc.test__users WHERE CAST(age AS DOUBLE PRECISION) > $1 ORDER BY age DESC"
	if plan.Query != wantSQL {
		t.Fatalf("unexpected SQL:\ngot:  %s\nwant: %s", plan.Query, wantSQL)
	}
	if len(plan.Args) != 1 || plan.Args[0] != 25 {
		t.Fatalf("unexpected args: %#v", plan.Args)
	}
	if len(plan.RemainingPipeline) != 2 {
		t.Fatalf("expected residual match and limit stages, got %#v", plan.RemainingPipeline)
	}
	if _, ok := plan.RemainingPipeline[0]["$match"]; !ok {
		t.Fatalf("expected residual match stage to remain: %#v", plan.RemainingPipeline)
	}
	if _, ok := plan.RemainingPipeline[1]["$limit"]; !ok {
		t.Fatalf("expected limit stage to remain when residual filter exists: %#v", plan.RemainingPipeline)
	}
}

// TestBuildAggregatePrefilterPlan_FullMatchPushdownConsumesMatchAndLimit runs the corresponding test case.
func TestBuildAggregatePrefilterPlan_FullMatchPushdownConsumesMatchAndLimit(t *testing.T) {
	plan, ok, err := buildAggregatePrefilterPlan("test__users", []bson.M{
		{"$match": bson.M{"age": bson.M{"$gte": 21}}},
		{"$limit": 3},
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ok {
		t.Fatalf("expected prefilter plan")
	}
	wantSQL := "SELECT * FROM doc.test__users WHERE CAST(age AS DOUBLE PRECISION) >= $1 LIMIT 3"
	if plan.Query != wantSQL {
		t.Fatalf("unexpected SQL:\ngot:  %s\nwant: %s", plan.Query, wantSQL)
	}
	if len(plan.RemainingPipeline) != 0 {
		t.Fatalf("expected pushed stages to be consumed, got %#v", plan.RemainingPipeline)
	}
}

// TestBuildAndExecuteVectorSearchPlan_PushesLeadingMatchIntoSQL runs the corresponding test case.
func TestBuildAndExecuteVectorSearchPlan_PushesLeadingMatchIntoSQL(t *testing.T) {
	var gotQuery string
	var gotArgs []any
	var execSQL []string
	deps := Deps{
		DB: func() DBExecutor {
			return aggregateExecRecorder{execs: &execSQL}
		},
		LoadSQLDocsWithIDsQry: func(ctx context.Context, query string, args ...any) ([]SQLDoc, error) {
			gotQuery = query
			gotArgs = append([]any(nil), args...)
			return []SQLDoc{
				{DocID: `"doc_2"`, Doc: bson.M{"_id": "doc_2", vectorSearchScoreField: 0.99}},
				{DocID: `"doc_1"`, Doc: bson.M{"_id": "doc_1", vectorSearchScoreField: 0.88}},
			}, nil
		},
	}

	plan, err := buildAndExecuteVectorSearchPlan(context.Background(), deps, "test__docs", []bson.M{
		{"$match": bson.M{"age": bson.M{"$gte": 21}}},
		{"$vectorSearch": bson.M{
			"path":        "embedding",
			"queryVector": []float64{0.1, 0.2, 0.3},
			"limit":       2,
		}},
		{"$project": bson.M{"_id": 1, "title": 1, "score": bson.M{"$meta": "vectorSearchScore"}}},
	}, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if plan == nil {
		t.Fatalf("expected vector plan")
	}
	if len(gotArgs) != 1 || gotArgs[0] != 21 {
		t.Fatalf("unexpected args: %#v", gotArgs)
	}
	if len(execSQL) != 1 || execSQL[0] != "REFRESH TABLE doc.test__docs" {
		t.Fatalf("expected refresh before vector query, got %#v", execSQL)
	}
	if !strings.Contains(gotQuery, "KNN_MATCH(embedding, [0.1, 0.2, 0.3]::float_vector(3), 2)") {
		t.Fatalf("expected KNN query, got %q", gotQuery)
	}
	if !strings.Contains(gotQuery, "AND CAST(age AS DOUBLE PRECISION) >= $1") {
		t.Fatalf("expected match prefilter in SQL, got %q", gotQuery)
	}
	if !strings.Contains(gotQuery, `VECTOR_SIMILARITY(embedding, [0.1, 0.2, 0.3]::float_vector(3)) AS "score"`) {
		t.Fatalf("expected projected similarity alias, got %q", gotQuery)
	}
	if len(plan.Docs) != 2 || plan.Docs[0]["_id"] != "doc_2" || plan.Docs[0][vectorSearchScoreField] != 0.99 {
		t.Fatalf("unexpected docs: %#v", plan.Docs)
	}
	if len(plan.RemainingPipeline) != 0 {
		t.Fatalf("expected project stage to be consumed, got %#v", plan.RemainingPipeline)
	}
}

// TestBuildAndExecuteVectorSearchPlan_RejectsNonMatchBeforeVector runs the corresponding test case.
func TestBuildAndExecuteVectorSearchPlan_RejectsNonMatchBeforeVector(t *testing.T) {
	_, err := buildAndExecuteVectorSearchPlan(context.Background(), Deps{}, "test__docs", []bson.M{
		{"$sort": bson.M{"age": 1}},
		{"$vectorSearch": bson.M{
			"path":        "embedding",
			"queryVector": []float64{0.1, 0.2, 0.3},
			"limit":       2,
		}},
	}, 5*time.Second)
	if err == nil {
		t.Fatalf("expected error")
	}
}

// TestAggregateMaxTime_DefaultsAndParses runs the corresponding test case.
func TestAggregateMaxTime_DefaultsAndParses(t *testing.T) {
	if got := aggregateMaxTime(bson.M{}); got != 10*time.Second {
		t.Fatalf("unexpected default max time: %v", got)
	}
	if got := aggregateMaxTime(bson.M{"maxTimeMS": int32(2500)}); got != 2500*time.Millisecond {
		t.Fatalf("unexpected parsed max time: %v", got)
	}
}

// TestApplyGraphLookupPushdown_Basic runs the corresponding test case.
func TestApplyGraphLookupPushdown_Basic(t *testing.T) {
	db := &aggregateFakeDB{
		rows: &aggregateFakeRows{
			vals: [][]any{
				{"2", int64(0)},
				{"1", int64(1)},
			},
		},
	}
	deps := Deps{
		DB: func() DBExecutor { return db },
		PhysicalCollectionName: func(dbName, collection string) (string, error) {
			return dbName + "__" + collection, nil
		},
		LoadSQLDocsWithIDsQry: func(ctx context.Context, query string, args ...any) ([]SQLDoc, error) {
			return []SQLDoc{
				{DocID: `"u1"`, Doc: bson.M{"_id": "u1", "user_id": 1}},
				{DocID: `"u2"`, Doc: bson.M{"_id": "u2", "user_id": 2}},
			}, nil
		},
	}

	out, pushed, err := applyGraphLookupPushdown(context.Background(), deps, "testdb", []bson.M{
		{"_id": 10, "user_id": 3, "name": "Cara", "manager_id": 2},
	}, bson.M{
		"from":             "users",
		"startWith":        "$manager_id",
		"connectFromField": "manager_id",
		"connectToField":   "user_id",
		"maxDepth":         3,
		"depthField":       "depth",
		"restrictSearchWithMatch": bson.M{
			"active": true,
		},
		"as": "hierarchy",
	}, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !pushed {
		t.Fatalf("expected graph pushdown")
	}
	if len(db.execSQL) == 0 || !strings.Contains(strings.Join(db.execSQL, "\n"), "CREATE GRAPH testdb__users__graph__") {
		t.Fatalf("expected graph resource setup, got %#v", db.execSQL)
	}
	if !strings.Contains(db.querySQL, "FROM traverse('testdb__users__graph__") {
		t.Fatalf("expected MonkDB traverse SQL, got %q", db.querySQL)
	}
	hierarchy, ok := out[0]["hierarchy"].([]bson.M)
	if !ok || len(hierarchy) != 2 {
		t.Fatalf("unexpected hierarchy: %#v", out[0]["hierarchy"])
	}
	if out[0]["user_id"] != 3 || out[0]["name"] != "Cara" {
		t.Fatalf("unexpected root doc: %#v", out[0])
	}
	if hierarchy[0]["user_id"] != 2 || hierarchy[0]["depth"] != int64(0) {
		t.Fatalf("unexpected first hop: %#v", hierarchy[0])
	}
	if hierarchy[1]["user_id"] != 1 || hierarchy[1]["depth"] != int64(1) {
		t.Fatalf("unexpected second hop: %#v", hierarchy[1])
	}
}

type aggregateExecRecorder struct {
	execs *[]string
}

// Query is a helper used by the adapter.
func (r aggregateExecRecorder) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return nil, nil
}

// QueryRow is a helper used by the adapter.
func (r aggregateExecRecorder) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return nil
}

// Exec is a helper used by the adapter.
func (r aggregateExecRecorder) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	*r.execs = append(*r.execs, sql)
	return pgconn.CommandTag{}, nil
}

type aggregateFakeDB struct {
	rows      pgx.Rows
	querySQL  string
	queryArgs []interface{}
	execSQL   []string
	queryRow  pgx.Row
}

// Query is a helper used by the adapter.
func (d *aggregateFakeDB) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	d.querySQL = sql
	d.queryArgs = append([]interface{}(nil), args...)
	return d.rows, nil
}

// QueryRow is a helper used by the adapter.
func (d *aggregateFakeDB) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	if d.queryRow != nil {
		return d.queryRow
	}
	return aggregateFakeRow{vals: []any{int64(0)}}
}

// Exec is a helper used by the adapter.
func (d *aggregateFakeDB) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	d.execSQL = append(d.execSQL, sql)
	return pgconn.CommandTag{}, nil
}

type aggregateFakeRows struct {
	vals [][]any
	i    int
}

// Close is a helper used by the adapter.
func (r *aggregateFakeRows) Close() {}

// Err is a helper used by the adapter.
func (r *aggregateFakeRows) Err() error { return nil }

// CommandTag is a helper used by the adapter.
func (r *aggregateFakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

// FieldDescriptions is a helper used by the adapter.
func (r *aggregateFakeRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

// Next is a helper used by the adapter.
func (r *aggregateFakeRows) Next() bool {
	if r.i >= len(r.vals) {
		return false
	}
	r.i++
	return true
}

// Scan is a helper used by the adapter.
func (r *aggregateFakeRows) Scan(dest ...any) error {
	row := r.vals[r.i-1]
	for idx := range dest {
		switch d := dest[idx].(type) {
		case *string:
			*d = row[idx].(string)
		case *int64:
			*d = row[idx].(int64)
		case *float64:
			*d = row[idx].(float64)
		}
	}
	return nil
}

// Values is a helper used by the adapter.
func (r *aggregateFakeRows) Values() ([]any, error) {
	if r.i == 0 || r.i > len(r.vals) {
		return nil, nil
	}
	return r.vals[r.i-1], nil
}

// RawValues is a helper used by the adapter.
func (r *aggregateFakeRows) RawValues() [][]byte { return nil }

// Conn is a helper used by the adapter.
func (r *aggregateFakeRows) Conn() *pgx.Conn { return nil }

type aggregateFakeRow struct {
	vals []any
	err  error
}

// Scan is a helper used by the adapter.
func (r aggregateFakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for idx := range dest {
		switch d := dest[idx].(type) {
		case *int64:
			*d = r.vals[idx].(int64)
		case *string:
			*d = r.vals[idx].(string)
		}
	}
	return nil
}
