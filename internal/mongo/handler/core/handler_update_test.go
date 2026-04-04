package mongo

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"gopkg.in/mgo.v2/bson"
)

type recordingQueryExec struct {
	querySQL  []string
	queryArgs [][]interface{}
	rows      pgx.Rows
}

// Query is a helper used by the adapter.
func (e *recordingQueryExec) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	e.querySQL = append(e.querySQL, sql)
	copied := append([]interface{}(nil), args...)
	e.queryArgs = append(e.queryArgs, copied)
	return e.rows, nil
}

// QueryRow is a helper used by the adapter.
func (e *recordingQueryExec) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return nil
}

// Exec is a helper used by the adapter.
func (e *recordingQueryExec) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec not implemented")
}

// TestLoadCandidateSQLDocsWithIDs_UsesCoarseWhereClause runs the corresponding test case.
func TestLoadCandidateSQLDocsWithIDs_UsesCoarseWhereClause(t *testing.T) {
	h := &Handler{}
	exec := &recordingQueryExec{
		rows: &fakeRows{
			fields: []pgconn.FieldDescription{
				{Name: "id"},
				{Name: "data"},
			},
			vals: [][]any{
				{
					"\"aaaaaaaaaaaaaaaaaaaaaaaa\"",
					bson.M{"_id": "aaaaaaaaaaaaaaaaaaaaaaaa", "age": int32(30), "complex": bson.M{"nested": "x"}},
				},
			},
		},
	}

	filter := bson.M{
		"age":     bson.M{"$gt": 25},
		"complex": bson.M{"nested": "x"},
	}
	pdocs, residual, err := h.loadCandidateSQLDocsWithIDs(context.Background(), exec, "test__users", filter, false)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if _, ok := residual["complex"]; !ok {
		t.Fatalf("expected residual filter to retain complex predicate, got %#v", residual)
	}
	if len(pdocs) != 1 {
		t.Fatalf("expected one candidate doc, got %d", len(pdocs))
	}
	if len(exec.querySQL) != 1 {
		t.Fatalf("expected one query, got %#v", exec.querySQL)
	}
	if exec.querySQL[0] != "SELECT id, complex FROM doc.test__users WHERE CAST(age AS DOUBLE PRECISION) > $1" {
		t.Fatalf("unexpected query: %q", exec.querySQL[0])
	}
	if len(exec.queryArgs[0]) != 1 || exec.queryArgs[0][0] != 25 {
		t.Fatalf("unexpected query args: %#v", exec.queryArgs[0])
	}
}
