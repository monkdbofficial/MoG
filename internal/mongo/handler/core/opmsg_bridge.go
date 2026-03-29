package mongo

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/handler/opmsg"
	"mog/internal/mongo/handler/shared"
)

type opmsgExecAdapter struct {
	exec opmsg.DBExecutor
}

func (a opmsgExecAdapter) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return a.exec.Query(ctx, sql, args...)
}

func (a opmsgExecAdapter) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return a.exec.QueryRow(ctx, sql, args...)
}

func (a opmsgExecAdapter) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	return a.exec.Exec(ctx, sql, args...)
}

func (h *Handler) opmsgDeps() opmsg.Deps {
	return opmsg.Deps{
		StableFieldOrder: h.stableFieldOrder,
		LogWriteInfo:     h.logWriteInfo,
		RemoteAddr:       RemoteAddr,

		NewMsg:      h.newMsg,
		NewMsgError: h.newMsgError,

		CommandDB:              shared.CommandDB,
		PhysicalCollectionName: physicalCollectionName,
		CatalogUpsert:          h.catalogUpsert,

		DB: func() opmsg.DBExecutor {
			if h.tx != nil {
				return h.tx
			}
			return h.pool
		},

		LoadSQLDocs: func(ctx context.Context, physical string) ([]bson.M, error) {
			return h.loadSQLDocs(ctx, physical)
		},
		LoadSQLDocsWithIDs: func(ctx context.Context, physical string) ([]opmsg.SQLDoc, error) {
			exec := DBExecutor(h.pool)
			if h.tx != nil {
				exec = h.tx
			}
			pdocs, err := h.loadSQLDocsWithIDs(ctx, exec, physical)
			if err != nil {
				return nil, err
			}
			out := make([]opmsg.SQLDoc, 0, len(pdocs))
			for _, pd := range pdocs {
				out = append(out, opmsg.SQLDoc{Doc: pd.doc, DocID: pd.docID})
			}
			return out, nil
		},
		LoadSQLDocsWithIDsQry: func(ctx context.Context, query string, args ...any) ([]opmsg.SQLDoc, error) {
			exec := DBExecutor(h.pool)
			if h.tx != nil {
				exec = h.tx
			}
			pdocs, err := h.loadSQLDocsWithIDsQuery(ctx, exec, query, args...)
			if err != nil {
				return nil, err
			}
			out := make([]opmsg.SQLDoc, 0, len(pdocs))
			for _, pd := range pdocs {
				out = append(out, opmsg.SQLDoc{Doc: pd.doc, DocID: pd.docID})
			}
			return out, nil
		},

		EnsureCollectionTable: h.ensureCollectionTable,
		ClearSchemaCache: func(physical string) {
			h.schemaCache().clear(physical)
		},

		InsertMany: h.insertMany,

		CurrentTx: func() pgx.Tx { return h.tx },
		BeginTx:   func(ctx context.Context) (pgx.Tx, error) { return h.pool.Begin(ctx) },

		ApplyPureSQLUpdate: func(ctx context.Context, exec opmsg.DBExecutor, physical string, filter bson.M, update bson.M, multi bool, upsert bool) (matched int, modified int, upsertedID interface{}, err error) {
			return h.applyPureSQLUpdate(ctx, opmsgExecAdapter{exec: exec}, physical, filter, update, multi, upsert)
		},

		MarkTouched:    h.markTouched,
		RefreshTouched: h.refreshTouched,

		NormalizeDocForReply:     normalizeDocForReply,
		OrderTopLevelDocForReply: shared.OrderTopLevelDocForReply,

		IsUndefinedRelation: isUndefinedRelation,
		IsUndefinedSchema:   isUndefinedSchema,
	}
}
