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

func (r *fakeRows) Close() { r.closed = true }
func (r *fakeRows) Err() error {
	return nil
}
func (r *fakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription {
	return r.fields
}
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
func (r *fakeRows) Scan(dest ...any) error { return nil }
func (r *fakeRows) Values() ([]any, error) {
	if r.i == 0 || r.i > len(r.vals) {
		return nil, nil
	}
	return r.vals[r.i-1], nil
}
func (r *fakeRows) RawValues() [][]byte { return nil }
func (r *fakeRows) Conn() *pgx.Conn     { return nil }

type fakeExec struct {
	rows pgx.Rows
}

func (e fakeExec) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return e.rows, nil
}
func (e fakeExec) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return nil
}
func (e fakeExec) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

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
