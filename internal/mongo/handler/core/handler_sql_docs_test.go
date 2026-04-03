package mongo

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"gopkg.in/mgo.v2/bson"
)

type fakeRows struct {
	fields []pgconn.FieldDescription
	vals   [][]any
	i      int
	closed bool
}

// Close is a helper used by the adapter.
func (r *fakeRows) Close() { r.closed = true }

// Err is a helper used by the adapter.
func (r *fakeRows) Err() error {
	return nil
}

// CommandTag is a helper used by the adapter.
func (r *fakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

// FieldDescriptions is a helper used by the adapter.
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription {
	return r.fields
}

// Next is a helper used by the adapter.
func (r *fakeRows) Next() bool {
	if r.closed {
		return false
	}
	if r.i >= len(r.vals) {
		r.closed = true
		return false
	}
	r.i++
	return true
}

// Scan is a helper used by the adapter.
func (r *fakeRows) Scan(dest ...any) error { return nil }

// Values is a helper used by the adapter.
func (r *fakeRows) Values() ([]any, error) {
	if r.i == 0 || r.i > len(r.vals) {
		return nil, nil
	}
	return r.vals[r.i-1], nil
}

// RawValues is a helper used by the adapter.
func (r *fakeRows) RawValues() [][]byte { return nil }

// Conn is a helper used by the adapter.
func (r *fakeRows) Conn() *pgx.Conn { return nil }

type fakeExec struct {
	rows pgx.Rows
}

// Query is a helper used by the adapter.
func (e fakeExec) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return e.rows, nil
}

// QueryRow is a helper used by the adapter.
func (e fakeExec) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return nil
}

// Exec is a helper used by the adapter.
func (e fakeExec) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

// TestLoadSQLDocsWithIDsQuery_UsesDataColumnEvenWhenStoreRawDisabled runs the corresponding test case.
func TestLoadSQLDocsWithIDsQuery_UsesDataColumnEvenWhenStoreRawDisabled(t *testing.T) {
	h := &Handler{storeRawMongoJSON: false}

	idFD := pgconn.FieldDescription{Name: "id"}
	dataFD := pgconn.FieldDescription{Name: "data"}

	row := &fakeRows{
		fields: []pgconn.FieldDescription{idFD, dataFD},
		vals: [][]any{
			{
				"\"aaaaaaaaaaaaaaaaaaaaaaaa\"",
				bson.M{"mongo_user_id": int32(1), "age": int32(25)},
			},
		},
	}

	out, err := h.loadSQLDocsWithIDsQuery(context.Background(), fakeExec{rows: row}, "SELECT * FROM doc.test__users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(out))
	}
	got := out[0].doc
	if got["mongo_user_id"] != int32(1) {
		t.Fatalf("expected mongo_user_id=1, got %#v", got["mongo_user_id"])
	}
	if got["age"] != int32(25) {
		t.Fatalf("expected age=25, got %#v", got["age"])
	}
}
