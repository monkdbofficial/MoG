package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"gopkg.in/mgo.v2/bson"
)

// SQL schema and document loading helpers.
func (h *Handler) ensureDocSchema(ctx context.Context) error {
	if h.pool == nil {
		return nil
	}
	_, err := h.pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS doc")
	return err
}

func (h *Handler) ensureDocSchemaExec(ctx context.Context, exec DBExecutor) error {
	if exec == nil {
		return nil
	}
	_, err := exec.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS doc")
	return err
}

type pureSQLDoc struct {
	docID string
	doc   bson.M
}

func decodeDocID(docID string) interface{} {
	if docID == "" {
		return nil
	}
	var v interface{}
	if err := json.Unmarshal([]byte(docID), &v); err == nil {
		return v
	}
	// Legacy ids were stored as raw strings (not JSON string literals).
	return docID
}

func encodeDocID(v interface{}) (string, error) {
	if v == nil {
		return "", fmt.Errorf("_id is nil")
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (h *Handler) loadSQLDocsWithIDs(ctx context.Context, exec DBExecutor, physical string) ([]pureSQLDoc, error) {
	if exec == nil {
		return nil, fmt.Errorf("db executor is nil")
	}
	if physical == "" {
		return []pureSQLDoc{}, nil
	}
	return h.loadSQLDocsWithIDsQuery(ctx, exec, "SELECT * FROM doc."+physical)
}

func (h *Handler) loadSQLDocsWithIDsQuery(ctx context.Context, exec DBExecutor, query string, args ...interface{}) ([]pureSQLDoc, error) {
	if exec == nil {
		return nil, fmt.Errorf("db executor is nil")
	}
	if strings.TrimSpace(query) == "" {
		return []pureSQLDoc{}, nil
	}

	rows, err := exec.Query(ctx, query, args...)
	if err != nil {
		if isUndefinedRelation(err) || isUndefinedSchema(err) {
			return []pureSQLDoc{}, nil
		}
		return nil, err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	numFields := len(fields)
	out := make([]pureSQLDoc, 0, 16) // Pre-allocate small capacity
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		relFields := bson.M{}
		var rawDoc bson.M
		docID := ""
		for i := 0; i < numFields; i++ {
			if i >= len(vals) {
				continue
			}
			fd := fields[i]
			col := string(fd.Name)
			if col == "" {
				continue
			}
			val := vals[i]
			if col == "id" {
				if val != nil {
					docID = fmt.Sprint(val)
				}
				continue
			}
			if col == "data" {
				// When enabled, `data` stores the raw Mongo document (including arrays).
				// Keep it internal: only used for reconstruction and uniqueness checks.
				if h.storeRawMongoJSON && val != nil {
					if m, ok := coerceBsonM(val); ok {
						rawDoc = m
					}
				}
				continue
			}
			if val == nil {
				continue
			}
			field := mongoFieldNameForColumn(col)
			if field == "" || field == "_id" {
				continue
			}
			relFields[field] = normalizeRowValue(val)
		}
		if docID == "" && rawDoc != nil {
			if id, ok := rawDoc["_id"]; ok {
				docID, _ = encodeDocID(id)
			}
		}

		if docID == "" {
			continue
		}
		doc := rawDoc
		if doc == nil {
			doc = bson.M{}
		}
		doc["_id"] = decodeDocID(docID)
		for k, v := range relFields {
			if k == "" || k == "_id" {
				continue
			}
			// Some backends represent NULL object columns as `{}`. If we have a raw doc and it
			// contains a non-empty object for this field, don't overwrite it with an empty object.
			if rawDoc != nil {
				switch vv := v.(type) {
				case bson.M:
					if len(vv) == 0 {
						if existing, ok := rawDoc[k].(bson.M); ok && len(existing) > 0 {
							continue
						}
						if existing, ok := rawDoc[k].(map[string]interface{}); ok && len(existing) > 0 {
							continue
						}
					}
				case map[string]interface{}:
					if len(vv) == 0 {
						if existing, ok := rawDoc[k].(bson.M); ok && len(existing) > 0 {
							continue
						}
						if existing, ok := rawDoc[k].(map[string]interface{}); ok && len(existing) > 0 {
							continue
						}
					}
				}
			}
			doc[k] = v
		}
		out = append(out, pureSQLDoc{docID: docID, doc: doc})
	}
	return out, nil
}

func (h *Handler) loadSQLDocs(ctx context.Context, physical string) ([]bson.M, error) {
	// For performance and safety under high throughput, never load more than 1000 docs
	// if pushdown failed. In-memory filtering/sorting is extremely slow.
	pdocs, err := h.loadSQLDocsWithIDsQuery(ctx, h.db(), "SELECT * FROM doc."+physical+" LIMIT 1000")
	if err != nil {
		return nil, err
	}
	out := make([]bson.M, 0, len(pdocs))
	for _, pd := range pdocs {
		out = append(out, pd.doc)
	}
	return out, nil
}

func (h *Handler) ensureCollectionTable(ctx context.Context, collection string) error {
	if h.pool == nil {
		return fmt.Errorf("database pool is not configured")
	}
	return h.ensureCollectionTableExec(ctx, h.pool, collection)
}

func (h *Handler) ensureCollectionTableExec(ctx context.Context, exec DBExecutor, collection string) error {
	if exec == nil {
		return fmt.Errorf("db executor is nil")
	}
	if !isSafeIdentifier(collection) {
		return fmt.Errorf("invalid collection name: %s", collection)
	}
	// Schema is cached per-process and shared across connections/handlers.
	// If `MOG_STORE_RAW_MONGO_JSON` is toggled at runtime, ensure the optional
	// `data` column still gets added even when the base table is already cached
	// as initialized.
	if h.schemaCache().isInitialized(collection) {
		// Cache can get stale if the backend schema was modified by a different process
		// (or if `data` existed from an older run). Refresh the cache for this table once
		// to detect `data` so write paths can keep it in sync.
		if !h.schemaCache().hasColumn(collection, "data") {
			if cols, err := h.listColumnsExec(ctx, exec, collection); err == nil {
				for _, c := range cols {
					if c != "" {
						h.schemaCache().setColumn(collection, c, "UNKNOWN")
					}
				}
			}
		}
		if h.storeRawMongoJSON && !h.schemaCache().hasColumn(collection, "data") {
			lock := ddlLockForTable(collection)
			lock.Lock()
			defer lock.Unlock()
			if _, err := exec.Exec(ctx, "ALTER TABLE doc."+collection+" ADD COLUMN data OBJECT(DYNAMIC)"); err != nil && !isDuplicateColumnName(err) {
				return err
			}
			h.schemaCache().setColumn(collection, "data", "OBJECT(DYNAMIC)")
		}
		return nil
	}

	// Ensure the backing schema exists (fresh MonkDB instances may not have it yet).
	_ = h.ensureDocSchemaExec(ctx, exec)

	// Always create/upgrade to relational storage. Optional raw-doc storage is controlled by MOG_STORE_RAW_MONGO_JSON.
	if _, err := exec.Exec(ctx, "CREATE TABLE IF NOT EXISTS doc."+collection+" (id TEXT PRIMARY KEY)"); err != nil {
		return err
	}
	// Best-effort upgrade path for existing tables.
	if _, err := exec.Exec(ctx, "ALTER TABLE doc."+collection+" ADD COLUMN id TEXT"); err != nil && !isDuplicateColumnName(err) {
		return err
	}
	// If a legacy `data` column exists, backfill ids.
	if _, err := exec.Exec(ctx, "UPDATE doc."+collection+" SET id = data['_id'] WHERE id IS NULL AND data['_id'] IS NOT NULL"); err != nil && !isUndefinedColumn(err) {
		// Best-effort: ignore if the table doesn't have a legacy `data` column.
	}
	if h.storeRawMongoJSON {
		if _, err := exec.Exec(ctx, "ALTER TABLE doc."+collection+" ADD COLUMN data OBJECT(DYNAMIC)"); err != nil && !isDuplicateColumnName(err) {
			return err
		}
	}

	// Cache schema for this process to avoid repeated DDL.
	h.schemaCache().markInitialized(collection)
	h.schemaCache().setColumn(collection, "id", "TEXT")
	if h.storeRawMongoJSON {
		h.schemaCache().setColumn(collection, "data", "OBJECT(DYNAMIC)")
	}
	if cols, err := h.listColumnsExec(ctx, exec, collection); err == nil {
		for _, c := range cols {
			if c != "" {
				h.schemaCache().setColumn(collection, c, "UNKNOWN")
			}
		}
	}
	return nil
}

func (h *Handler) ensureColumn(ctx context.Context, physical string, col string, sqlType string) error {
	if h.pool == nil {
		return fmt.Errorf("database pool is not configured")
	}
	return h.ensureColumnExec(ctx, h.pool, physical, col, sqlType)
}

func (h *Handler) ensureColumnExec(ctx context.Context, exec DBExecutor, physical string, col string, sqlType string) error {
	if exec == nil {
		return fmt.Errorf("db executor is nil")
	}
	if physical == "" || col == "" || sqlType == "" {
		return nil
	}
	if h.schemaCache().hasColumn(physical, col) {
		return nil
	}
	if strings.HasPrefix(col, "_") || !isSafeIdentifier(col) {
		return fmt.Errorf("field %q is not supported as a SQL column name", col)
	}
	if _, err := exec.Exec(ctx, fmt.Sprintf("ALTER TABLE doc.%s ADD COLUMN %s %s", physical, col, sqlType)); err != nil {
		if isDuplicateColumnName(err) {
			h.schemaCache().setColumn(physical, col, sqlType)
			return nil
		}
		return err
	}
	h.schemaCache().setColumn(physical, col, sqlType)
	return nil
}

func (h *Handler) listColumns(ctx context.Context, physical string) ([]string, error) {
	if h.pool == nil || physical == "" {
		return nil, nil
	}
	var rows pgx.Rows
	var err error
	for _, q := range []string{
		"SELECT column_name FROM information_schema.columns WHERE table_schema = 'doc' AND table_name = $1",
		"SELECT column_name FROM information_schema.columns WHERE table_schema_name = 'doc' AND table_name = $1",
		"SELECT column_name FROM information_schema.columns WHERE schema_name = 'doc' AND table_name = $1",
	} {
		rows, err = h.pool.Query(ctx, q, physical)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err == nil && c != "" {
			cols = append(cols, c)
		}
	}
	return cols, nil
}
