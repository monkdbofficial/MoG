package opmsg

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"gopkg.in/mgo.v2/bson"
)

type DBExecutor interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
}

type SQLDoc struct {
	Doc        bson.M
	DocID      string
	FieldOrder []string
}

type Deps struct {
	LogWriteInfo bool

	RemoteAddr func(ctx context.Context) string

	NewMsg      func(requestID int32, doc bson.M) ([]byte, error)
	NewMsgError func(requestID int32, code int32, codeName, errmsg string) ([]byte, error)

	CommandDB              func(cmd bson.M) string
	PhysicalCollectionName func(dbName, collection string) (string, error)
	CatalogUpsert          func(ctx context.Context, dbName, collection string) error

	DB                    func() DBExecutor
	LoadSQLDocs           func(ctx context.Context, physical string) ([]bson.M, error)
	LoadSQLDocsWithIDs    func(ctx context.Context, physical string) ([]SQLDoc, error)
	LoadSQLDocsWithIDsQry func(ctx context.Context, query string, args ...any) ([]SQLDoc, error)

	EnsureCollectionTable func(ctx context.Context, physical string) error
	ClearSchemaCache      func(physical string)

	InsertMany func(ctx context.Context, physical string, rawDocs []interface{}) (seen int, inserted int64, err error)

	CurrentTx func() pgx.Tx
	BeginTx   func(ctx context.Context) (pgx.Tx, error)

	ApplyPureSQLUpdate func(ctx context.Context, exec DBExecutor, physical string, filter bson.M, update bson.M, multi bool, upsert bool) (matched int, modified int, upsertedID interface{}, err error)

	MarkTouched    func(physical string)
	RefreshTouched func(ctx context.Context)

	NormalizeDocForReply     func(ctx context.Context, doc bson.M) error
	OrderTopLevelDocForReply func(m bson.M, fieldOrder []string) bson.D

	IsUndefinedRelation func(err error) bool
	IsUndefinedSchema   func(err error) bool
}
