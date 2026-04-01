package opmsg

import (
	"context"
	"testing"

	"gopkg.in/mgo.v2/bson"
)

func TestLoadDeleteCandidateDocs_UsesCoarseWhereClause(t *testing.T) {
	var fullLoadCalled bool
	var querySQL string
	var queryArgs []any

	deps := Deps{
		LoadSQLDocsWithIDs: func(ctx context.Context, physical string) ([]SQLDoc, error) {
			fullLoadCalled = true
			return nil, nil
		},
		LoadSQLDocsWithIDsQry: func(ctx context.Context, query string, args ...any) ([]SQLDoc, error) {
			querySQL = query
			queryArgs = append([]any(nil), args...)
			return []SQLDoc{
				{DocID: "\"1\"", Doc: bson.M{"_id": "1", "age": 30, "complex": bson.M{"nested": "x"}}},
			}, nil
		},
	}

	filter := bson.M{
		"age":     bson.M{"$gt": 25},
		"complex": bson.M{"nested": "x"},
	}
	pdocs, err := loadDeleteCandidateDocs(context.Background(), deps, "test__users", filter, false)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if fullLoadCalled {
		t.Fatalf("expected coarse pushdown path, but full load was used")
	}
	if len(pdocs) != 1 {
		t.Fatalf("expected one candidate doc, got %d", len(pdocs))
	}
	if querySQL != "SELECT * FROM doc.test__users WHERE CAST(age AS DOUBLE PRECISION) > $1" {
		t.Fatalf("unexpected query: %q", querySQL)
	}
	if len(queryArgs) != 1 || queryArgs[0] != 25 {
		t.Fatalf("unexpected query args: %#v", queryArgs)
	}
}
