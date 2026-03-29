package mongo

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/handler/shared"
)

// Catalog and backend-introspection helpers.
func (h *Handler) dropCollectionTable(ctx context.Context, collection string) error {
	if h.pool == nil {
		return fmt.Errorf("database pool is not configured")
	}
	if !shared.IsSafeIdentifier(collection) {
		return fmt.Errorf("invalid collection name: %s", collection)
	}
	_ = h.ensureDocSchema(ctx)
	// If the table is dropped, clear the in-process schema cache so subsequent writes
	// don't incorrectly assume the relation still exists.
	h.schemaCache().clear(collection)
	_, err := h.pool.Exec(ctx, "DROP TABLE IF EXISTS doc."+collection)
	return err
}

func (h *Handler) dropDatabase(ctx context.Context, dbName string) error {
	if h.pool == nil {
		return nil
	}
	if dbName == "" {
		return nil
	}
	if !shared.IsSafeIdentifier(dbName) {
		return fmt.Errorf("invalid database name: %s", dbName)
	}

	// Clear any in-memory unique index definitions for this logical db.
	h.clearUniqueIndexesForDB(dbName)
	// Clear any in-process schema cache entries for this logical db so subsequent writes
	// can recreate missing relations.
	h.schemaCache().clearDB(dbName)

	colls, err := h.catalogListCollections(ctx, dbName)
	if err != nil {
		return nil
	}

	for _, logical := range colls {
		physical, err := physicalCollectionName(dbName, logical)
		if err != nil {
			continue
		}
		_ = h.dropCollectionTable(ctx, physical)
	}
	return nil
}

var (
	physicalNameCache = sync.Map{} // (dbName, collection) -> physical
)

func physicalCollectionName(dbName, collection string) (string, error) {
	if dbName == "" && collection == "" {
		return "", nil
	}
	key := dbName + ":" + collection
	if v, ok := physicalNameCache.Load(key); ok {
		return v.(string), nil
	}

	// When a driver supplies "$db", use it to namespace physical tables so multiple Mongo "databases"
	// don't collide on the same backend schema.
	var physical string
	if dbName == "" {
		if !shared.IsSafeIdentifier(collection) {
			return "", fmt.Errorf("invalid collection name: %s", collection)
		}
		physical = collection
	} else {
		if !shared.IsSafeIdentifier(dbName) {
			return "", fmt.Errorf("invalid database name: %s", dbName)
		}
		if !shared.IsSafeIdentifier(collection) {
			return "", fmt.Errorf("invalid collection name: %s", collection)
		}
		physical = dbName + "__" + collection
	}
	physicalNameCache.Store(key, physical)
	return physical, nil
}

func (h *Handler) ensureCatalogTable(ctx context.Context) error {
	if h.pool == nil {
		return nil
	}
	// Avoid repeated DDL/backfills on hot paths by caching init per-process.
	// NOTE: if the catalog table is dropped externally, call sites should clear this cache
	// (we do this on undefined-relation errors) so ensureCatalogTable runs again.
	if h.schemaCache().isInitialized(catalogCollection) {
		return nil
	}

	lock := ddlLockForTable(catalogCollection)
	lock.Lock()
	defer lock.Unlock()
	if h.schemaCache().isInitialized(catalogCollection) {
		return nil
	}

	_ = h.ensureDocSchema(ctx)

	// v2 schema (no PRIMARY KEY): some MonkDB/Crate-like backends reject PRIMARY KEY syntax.
	// We rely on best-effort dedupe in catalogUpsert instead of strict constraints.
	if _, err := h.pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS doc."+catalogCollection+" (id TEXT, db TEXT, coll TEXT, data OBJECT(DYNAMIC))"); err != nil {
		return err
	}

	// Best-effort upgrade path for old schemas (which only had `data`).
	for _, stmt := range []string{
		"ALTER TABLE doc." + catalogCollection + " ADD COLUMN id TEXT",
		"ALTER TABLE doc." + catalogCollection + " ADD COLUMN db TEXT",
		"ALTER TABLE doc." + catalogCollection + " ADD COLUMN coll TEXT",
		"ALTER TABLE doc." + catalogCollection + " ADD COLUMN data OBJECT(DYNAMIC)",
	} {
		if _, err := h.pool.Exec(ctx, stmt); err != nil && !isDuplicateColumnName(err) {
			// Ignore missing relation/schema; some backends return different errors mid-upgrade.
			if !isUndefinedRelation(err) && !isUndefinedSchema(err) {
				return err
			}
		}
	}

	// Backfill `db`, `coll`, `id` from legacy `data` when possible.
	if _, err := h.pool.Exec(ctx, "UPDATE doc."+catalogCollection+" SET db = data['db'] WHERE db IS NULL AND data['db'] IS NOT NULL"); err != nil && !isUndefinedColumn(err) {
	}
	if _, err := h.pool.Exec(ctx, "UPDATE doc."+catalogCollection+" SET coll = data['coll'] WHERE coll IS NULL AND data['coll'] IS NOT NULL"); err != nil && !isUndefinedColumn(err) {
	}
	if _, err := h.pool.Exec(ctx, "UPDATE doc."+catalogCollection+" SET id = db || '__' || coll WHERE id IS NULL AND db IS NOT NULL AND coll IS NOT NULL"); err != nil && !isUndefinedColumn(err) {
	}

	// Cache schema for this process to avoid repeated DDL.
	h.schemaCache().markInitialized(catalogCollection)
	h.schemaCache().setColumn(catalogCollection, "id", "TEXT")
	h.schemaCache().setColumn(catalogCollection, "db", "TEXT")
	h.schemaCache().setColumn(catalogCollection, "coll", "TEXT")
	h.schemaCache().setColumn(catalogCollection, "data", "OBJECT(DYNAMIC)")
	return nil
}

func (h *Handler) catalogUpsert(ctx context.Context, dbName, collection string) error {
	if h.pool == nil {
		return nil
	}
	if dbName == "" || collection == "" {
		return nil
	}
	if !shared.IsSafeIdentifier(dbName) {
		return fmt.Errorf("invalid database name: %s", dbName)
	}
	if !shared.IsSafeIdentifier(collection) {
		return fmt.Errorf("invalid collection name: %s", collection)
	}

	if err := h.ensureCatalogTable(ctx); err != nil {
		// Catalog is derived; if it can't be created right now, still keep a process-local record
		// so listCollections/listDatabases work for this running instance.
		globalCatalogCache.add(dbName, collection)
		return nil
	}

	// Process-local best-effort short-circuit to avoid repeated SQL round-trips on hot paths.
	if globalCatalogCache.has(dbName, collection) {
		return nil
	}

	docJSON, err := shared.MarshalObject(bson.M{
		"db":   dbName,
		"coll": collection,
	})
	if err != nil {
		return err
	}

	id := dbName + "__" + collection
	tryOnce := func() error {
		// Best-effort dedupe without requiring a UNIQUE index (older catalogs may have duplicates already).
		if _, err := h.pool.Exec(ctx, "DELETE FROM doc."+catalogCollection+" WHERE id = $1", id); err != nil {
			if isUndefinedColumn(err) {
				_, _ = h.pool.Exec(ctx, "DELETE FROM doc."+catalogCollection+" WHERE data['db'] = $1 AND data['coll'] = $2", dbName, collection)
			} else if isUndefinedRelation(err) || isUndefinedSchema(err) {
				return err
			}
		}

		if _, err := h.pool.Exec(ctx, "INSERT INTO doc."+catalogCollection+" (id, db, coll, data) VALUES ($1, $2, $3, CAST($4 AS OBJECT(DYNAMIC)))", id, dbName, collection, docJSON); err != nil {
			return err
		}
		return nil
	}

	if err := tryOnce(); err != nil {
		if isUndefinedRelation(err) || isUndefinedSchema(err) {
			// Catalog was dropped or schema cache is stale; clear and retry once.
			h.schemaCache().clear(catalogCollection)
			if err2 := h.ensureCatalogTable(ctx); err2 == nil {
				if err3 := tryOnce(); err3 == nil {
					globalCatalogCache.add(dbName, collection)
					return nil
				} else {
					err = err3
				}
			}
		}
		// Back-compat: older catalogs might not have the v2 columns yet.
		if isUndefinedColumn(err) {
			if _, err2 := h.pool.Exec(ctx, "INSERT INTO doc."+catalogCollection+" (data) VALUES (CAST($1 AS OBJECT))", docJSON); err2 != nil {
				// Catalog is best-effort derived metadata. Never fail writes because catalog persistence failed.
				// Still record the collection in-process so listCollections/listDatabases keep working.
				globalCatalogCache.add(dbName, collection)
				return nil
			}
			globalCatalogCache.add(dbName, collection)
			return nil
		}
		// Uniqueness is enforced at the storage layer; treat conflicts as success.
		if isUniqueViolation(err) {
			globalCatalogCache.add(dbName, collection)
			return nil
		}
		// Catalog is best-effort derived metadata. Never fail writes because catalog persistence failed.
		// Still record the collection in-process so listCollections/listDatabases keep working.
		globalCatalogCache.add(dbName, collection)
		return nil
	}
	globalCatalogCache.add(dbName, collection)
	return nil
}

func (h *Handler) catalogRemoveCollection(ctx context.Context, dbName, collection string) error {
	if h.pool == nil {
		return nil
	}
	if dbName == "" || collection == "" {
		return nil
	}
	if err := h.ensureCatalogTable(ctx); err != nil {
		return err
	}
	if _, err := h.pool.Exec(ctx, "DELETE FROM doc."+catalogCollection+" WHERE db = $1 AND coll = $2", dbName, collection); err != nil {
		if isUndefinedColumn(err) {
			_, err = h.pool.Exec(ctx, "DELETE FROM doc."+catalogCollection+" WHERE data['db'] = $1 AND data['coll'] = $2", dbName, collection)
		}
		if err != nil {
			return err
		}
	}
	globalCatalogCache.remove(dbName, collection)
	return nil
}

func (h *Handler) catalogRemoveDatabase(ctx context.Context, dbName string) error {
	if h.pool == nil {
		return nil
	}
	if dbName == "" {
		return nil
	}
	if err := h.ensureCatalogTable(ctx); err != nil {
		return err
	}
	if _, err := h.pool.Exec(ctx, "DELETE FROM doc."+catalogCollection+" WHERE db = $1", dbName); err != nil {
		if isUndefinedColumn(err) {
			_, err = h.pool.Exec(ctx, "DELETE FROM doc."+catalogCollection+" WHERE data['db'] = $1", dbName)
		}
		if err != nil {
			return err
		}
	}
	globalCatalogCache.clearDB(dbName)
	return nil
}

func (h *Handler) catalogListDatabases(ctx context.Context) ([]string, error) {
	if h.pool == nil {
		return []string{"admin"}, nil
	}
	if err := h.ensureCatalogTable(ctx); err != nil {
		// Fall back to listing from backend tables if the catalog table can't be created (e.g. dialect mismatch).
		if tables, _ := h.listDocTables(ctx); len(tables) > 0 {
			seen := map[string]bool{"admin": true}
			out := []string{"admin"}
			for _, name := range tables {
				parts := strings.SplitN(name, "__", 2)
				if len(parts) != 2 {
					continue
				}
				db := parts[0]
				if db == "" || seen[db] {
					continue
				}
				seen[db] = true
				out = append(out, db)
			}
			return out, nil
		}
		// Final fallback: include process-local cache so clients can see DBs created during this run.
		seen := map[string]bool{"admin": true}
		out := []string{"admin"}
		for _, db := range globalCatalogCache.listDBs() {
			if db == "" || seen[db] {
				continue
			}
			seen[db] = true
			out = append(out, db)
		}
		return out, nil
	}

	for attempt := 0; attempt < 2; attempt++ {
		// Prefer the v2 schema columns to avoid decoding `data`.
		rows, err := h.pool.Query(ctx, "SELECT db FROM doc."+catalogCollection)
		if err != nil {
			if isUndefinedRelation(err) || isUndefinedSchema(err) {
				// Catalog was dropped; clear cache and retry by rebuilding the table.
				h.schemaCache().clear(catalogCollection)
				_ = h.ensureCatalogTable(ctx)
				continue
			}
			if !isUndefinedColumn(err) {
				return []string{}, err
			}
			rows, err = h.pool.Query(ctx, "SELECT data FROM doc."+catalogCollection)
			if err != nil {
				return []string{}, err
			}
		}

		seen := map[string]bool{"admin": true}
		out := []string{"admin"}
		for rows.Next() {
			var db string
			var v interface{}
			if err := rows.Scan(&v); err != nil {
				// Best-effort: ignore scan errors and keep going.
				continue
			}
			switch t := v.(type) {
			case string:
				db = t
			case []byte:
				db = string(t)
			default:
				data, okDoc := shared.CoerceBsonM(v)
				if !okDoc {
					continue
				}
				db, _ = data["db"].(string)
			}
			if db == "" || seen[db] {
				continue
			}
			seen[db] = true
			out = append(out, db)
		}
		rows.Close()

		// Merge in DBs from actual backend tables to avoid catalog inconsistencies.
		if tables, _ := h.listDocTables(ctx); len(tables) > 0 {
			for _, name := range tables {
				parts := strings.SplitN(name, "__", 2)
				if len(parts) != 2 {
					continue
				}
				db := parts[0]
				if db == "" || seen[db] {
					continue
				}
				seen[db] = true
				out = append(out, db)
			}
		}

		if len(out) > 1 || attempt == 1 {
			// Merge in process-local catalog cache (helps when SQL catalog is missing/broken).
			for _, db := range globalCatalogCache.listDBs() {
				if db != "" && !seen[db] {
					seen[db] = true
					out = append(out, db)
				}
			}
			return out, nil
		}
		// Catalog may be empty after upgrade; try best-effort backfill from existing backend tables.
		_ = h.catalogBackfillFromTables(ctx)
	}
	return []string{"admin"}, nil
}

func (h *Handler) relationalWhereAndArgs(ctx context.Context, physical string, filter bson.M) (string, []interface{}, error) {
	// Relational/promotion mode removed. All pure-SQL filtering is evaluated in-memory over KV-reconstructed documents.
	return "", nil, fmt.Errorf("relational mode is not supported")
}

func relationalTagsCondition(v interface{}, argCount *int, physical string) (string, []interface{}, error) {
	return "", nil, fmt.Errorf("tags table is not supported")
}

func (h *Handler) catalogListCollections(ctx context.Context, dbName string) ([]string, error) {
	if h.pool == nil {
		return []string{}, nil
	}
	if dbName == "" {
		return []string{}, nil
	}
	if !shared.IsSafeIdentifier(dbName) {
		return []string{}, fmt.Errorf("invalid database name: %s", dbName)
	}
	if err := h.ensureCatalogTable(ctx); err != nil {
		// Fall back to listing from backend tables if the catalog table can't be created (e.g. dialect mismatch).
		if tables, _ := h.listDocTables(ctx); len(tables) > 0 {
			seen := map[string]bool{}
			out := []string{}
			prefix := dbName + "__"
			for _, name := range tables {
				if !strings.HasPrefix(name, prefix) {
					continue
				}
				coll := strings.TrimPrefix(name, prefix)
				if coll == "" || coll == catalogCollection || seen[coll] {
					continue
				}
				seen[coll] = true
				out = append(out, coll)
			}
			if len(out) > 0 {
				return out, nil
			}
		}
		// Final fallback: process-local catalog cache (collections seen during this server run).
		return globalCatalogCache.listCollections(dbName), nil
	}

	for attempt := 0; attempt < 2; attempt++ {
		existing := map[string]bool{}
		if tables, _ := h.listDocTables(ctx); len(tables) > 0 {
			prefix := dbName + "__"
			for _, name := range tables {
				if !strings.HasPrefix(name, prefix) {
					continue
				}
				coll := strings.TrimPrefix(name, prefix)
				if coll == "" || coll == catalogCollection {
					continue
				}
				existing[coll] = true
			}
		}

		// Prefer the v2 schema columns to avoid decoding `data`.
		rows, err := h.pool.Query(ctx, "SELECT coll FROM doc."+catalogCollection+" WHERE db = $1", dbName)
		if err != nil {
			if isUndefinedRelation(err) || isUndefinedSchema(err) {
				// Catalog was dropped; clear cache and retry by rebuilding the table.
				h.schemaCache().clear(catalogCollection)
				_ = h.ensureCatalogTable(ctx)
				continue
			}
			if !isUndefinedColumn(err) {
				return []string{}, err
			}
			rows, err = h.pool.Query(ctx, "SELECT data FROM doc."+catalogCollection+" WHERE data['db'] = $1", dbName)
			if err != nil {
				return []string{}, err
			}
		}

		seen := make(map[string]bool)
		var out []string
		for rows.Next() {
			var coll string
			var v interface{}
			if err := rows.Scan(&v); err != nil {
				// Best-effort: ignore scan errors and keep going.
				continue
			}
			switch t := v.(type) {
			case string:
				coll = t
			case []byte:
				coll = string(t)
			default:
				data, okDoc := shared.CoerceBsonM(v)
				if !okDoc {
					continue
				}
				coll, _ = data["coll"].(string)
			}
			if coll == "" || coll == catalogCollection {
				continue
			}
			if len(existing) > 0 && !existing[coll] {
				continue
			}
			if coll == "" || seen[coll] {
				continue
			}
			seen[coll] = true
			out = append(out, coll)
		}
		rows.Close()

		// If the v2 columns exist but are NULL/unbackfilled, the query above can return 0 rows
		// without error. Fall back to the legacy `data` scan in that case.
		if len(out) == 0 {
			legacyRows, lerr := h.pool.Query(ctx, "SELECT data FROM doc."+catalogCollection+" WHERE data['db'] = $1", dbName)
			if lerr == nil {
				for legacyRows.Next() {
					var v interface{}
					if err := legacyRows.Scan(&v); err != nil {
						continue
					}
					data, okDoc := shared.CoerceBsonM(v)
					if !okDoc {
						continue
					}
					coll, _ := data["coll"].(string)
					if coll == "" || coll == catalogCollection || seen[coll] {
						continue
					}
					if len(existing) > 0 && !existing[coll] {
						continue
					}
					seen[coll] = true
					out = append(out, coll)
				}
				legacyRows.Close()
			}
		}

		// Merge in collections from actual backend tables to avoid catalog inconsistencies.
		if tables, _ := h.listDocTables(ctx); len(tables) > 0 {
			prefix := dbName + "__"
			for _, name := range tables {
				if !strings.HasPrefix(name, prefix) {
					continue
				}
				coll := strings.TrimPrefix(name, prefix)
				if coll == "" || coll == catalogCollection || seen[coll] {
					continue
				}
				seen[coll] = true
				out = append(out, coll)
			}
		}

		if len(out) > 0 || attempt == 1 {
			// Merge in process-local catalog cache (helps when SQL catalog is missing/broken).
			for _, coll := range globalCatalogCache.listCollections(dbName) {
				if coll != "" && coll != catalogCollection && !seen[coll] {
					seen[coll] = true
					out = append(out, coll)
				}
			}
			return out, nil
		}
		_ = h.catalogBackfillFromTables(ctx)
	}
	return []string{}, nil
}

func (h *Handler) catalogRebuildFromTables(ctx context.Context) error {
	if h.pool == nil {
		return nil
	}
	_ = h.ensureDocSchema(ctx)
	// Catalog is derived; a rebuild is safe and also allows us to re-create the table
	// with a proper primary key even if it previously existed without one.
	if _, err := h.pool.Exec(ctx, "DROP TABLE IF EXISTS doc."+catalogCollection); err != nil {
		// ignore
	}
	if _, err := h.pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS doc."+catalogCollection+" (id TEXT, db TEXT, coll TEXT, data OBJECT(DYNAMIC))"); err != nil {
		return err
	}
	globalCatalogCache.clearAll()
	return h.catalogBackfillFromTables(ctx)
}

func (h *Handler) applyRelationalUpdate(ctx context.Context, exec DBExecutor, physical string, filter bson.M, update bson.M, multi bool) (matched int, modified int, err error) {
	return 0, 0, fmt.Errorf("relational mode is not supported")
}

func (h *Handler) catalogBackfillFromTables(ctx context.Context) error {
	if h.pool == nil {
		return nil
	}
	if err := h.ensureCatalogTable(ctx); err != nil {
		return err
	}

	tables, err := h.listDocTables(ctx)
	if err != nil {
		return nil
	}
	for _, name := range tables {
		parts := strings.SplitN(name, "__", 2)
		if len(parts) != 2 {
			continue
		}
		dbName := parts[0]
		coll := parts[1]
		if coll == "" || coll == catalogCollection {
			continue
		}
		_ = h.catalogUpsert(ctx, dbName, coll)
	}
	return nil
}

func (h *Handler) listDocTables(ctx context.Context) ([]string, error) {
	if h.pool == nil {
		return nil, nil
	}

	queries := []string{
		// CrateDB/MonkDB-like system catalog (often more reliable than information_schema).
		"SELECT name FROM sys.tables WHERE schema_name = 'doc'",
		"SELECT table_name FROM sys.tables WHERE schema_name = 'doc'",
		"SELECT table_name FROM sys.tables WHERE table_schema = 'doc'",
		"SELECT name FROM sys.tables WHERE table_schema = 'doc'",
		// PostgreSQL-ish catalogs (some backends emulate these).
		"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'doc'",
		"SELECT relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'doc' AND c.relkind = 'r'",
		// information_schema variants.
		"SELECT table_name FROM information_schema.tables WHERE table_schema = 'doc'",
		"SELECT table_name FROM information_schema.tables WHERE schema_name = 'doc'",
		"SELECT table_name FROM information_schema.tables WHERE table_schema_name = 'doc'",
	}

	var lastErr error
	sawSuccess := false
	for _, q := range queries {
		rows, err := h.pool.Query(ctx, q)
		if err != nil {
			lastErr = err
			continue
		}
		sawSuccess = true
		var out []string
		for rows.Next() {
			var name string
			if scanErr := rows.Scan(&name); scanErr != nil {
				// Best-effort: ignore scan errors and keep going.
				continue
			}
			if name != "" {
				out = append(out, name)
			}
		}
		rows.Close()
		if len(out) > 0 {
			return out, nil
		}
		// Query succeeded but returned 0 rows; try other catalog sources.
	}

	// If at least one query succeeded (but returned empty), return empty without error.
	if sawSuccess {
		return nil, nil
	}
	// Otherwise, return the last error so callers can decide how to behave.
	return nil, lastErr
}
