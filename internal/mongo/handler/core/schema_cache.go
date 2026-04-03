package mongo

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
)

type tableSchemaCache struct {
	mu     sync.RWMutex
	tables map[string]*cachedTableSchema
}

type cachedTableSchema struct {
	initialized bool
	columns     map[string]string // col -> sqlType (best-effort)
}

var globalSchemaCache = &tableSchemaCache{tables: map[string]*cachedTableSchema{}}
var globalDDLLocks sync.Map // physical -> *sync.Mutex

func ddlLockForTable(physical string) *sync.Mutex {
	if physical == "" {
		return &sync.Mutex{}
	}
	if v, ok := globalDDLLocks.Load(physical); ok {
		if m, ok := v.(*sync.Mutex); ok {
			return m
		}
	}
	m := &sync.Mutex{}
	actual, _ := globalDDLLocks.LoadOrStore(physical, m)
	if mm, ok := actual.(*sync.Mutex); ok {
		return mm
	}
	return m
}

func (c *tableSchemaCache) get(physical string) *cachedTableSchema {
	if c == nil || physical == "" {
		return nil
	}
	c.mu.RLock()
	t := c.tables[physical]
	c.mu.RUnlock()
	return t
}

func (c *tableSchemaCache) ensure(physical string) *cachedTableSchema {
	if c == nil || physical == "" {
		return nil
	}
	c.mu.Lock()
	t := c.tables[physical]
	if t == nil {
		t = &cachedTableSchema{columns: map[string]string{}}
		c.tables[physical] = t
	}
	c.mu.Unlock()
	return t
}

func (c *tableSchemaCache) markInitialized(physical string) {
	if c == nil || physical == "" {
		return
	}
	t := c.ensure(physical)
	c.mu.Lock()
	t.initialized = true
	c.mu.Unlock()
}

func (c *tableSchemaCache) isInitialized(physical string) bool {
	if c == nil || physical == "" {
		return false
	}
	c.mu.RLock()
	t := c.tables[physical]
	ok := t != nil && t.initialized
	c.mu.RUnlock()
	return ok
}

func (c *tableSchemaCache) hasColumn(physical, col string) bool {
	if c == nil || physical == "" || col == "" {
		return false
	}
	c.mu.RLock()
	t := c.tables[physical]
	if t == nil {
		c.mu.RUnlock()
		return false
	}
	_, ok := t.columns[col]
	c.mu.RUnlock()
	return ok
}

func (c *tableSchemaCache) setColumn(physical, col, sqlType string) {
	if c == nil || physical == "" || col == "" {
		return
	}
	if sqlType == "" {
		sqlType = "UNKNOWN"
	}
	t := c.ensure(physical)
	c.mu.Lock()
	if t.columns == nil {
		t.columns = map[string]string{}
	}
	t.columns[col] = sqlType
	c.mu.Unlock()
}

func (c *tableSchemaCache) clear(physical string) {
	if c == nil || physical == "" {
		return
	}
	c.mu.Lock()
	delete(c.tables, physical)
	c.mu.Unlock()
}

func (c *tableSchemaCache) clearDB(dbName string) {
	if c == nil || dbName == "" {
		return
	}
	prefix := dbName + "__"
	c.mu.Lock()
	for k := range c.tables {
		if strings.HasPrefix(k, prefix) {
			delete(c.tables, k)
		}
	}
	c.mu.Unlock()
}

func (h *Handler) schemaCache() *tableSchemaCache {
	return globalSchemaCache
}

func (h *Handler) listColumnsExec(ctx context.Context, exec DBExecutor, physical string) ([]string, error) {
	if exec == nil || physical == "" {
		return nil, nil
	}
	// Hot path: if we already initialized and cached the table schema in this
	// process, avoid hitting information_schema on every write.
	if c := h.schemaCache(); c != nil && c.isInitialized(physical) {
		if t := c.get(physical); t != nil && len(t.columns) > 0 {
			cols := make([]string, 0, len(t.columns))
			c.mu.RLock()
			for col := range t.columns {
				if col == "" {
					continue
				}
				// Skip backend-exposed "object sub-columns" like `meta['path']`.
				// We only want top-level physical columns here.
				if strings.Contains(col, "['") {
					continue
				}
				cols = append(cols, strings.ToLower(col))
			}
			c.mu.RUnlock()
			sort.Strings(cols)
			return cols, nil
		}
	}

	// Try a few variants for MonkDB compatibility.
	var rows pgx.Rows
	var err error
	for _, q := range []string{
		"SELECT column_name FROM information_schema.columns WHERE table_schema = 'doc' AND table_name = $1",
		"SELECT column_name FROM information_schema.columns WHERE table_schema_name = 'doc' AND table_name = $1",
		"SELECT column_name FROM information_schema.columns WHERE schema_name = 'doc' AND table_name = $1",
	} {
		rows, err = exec.Query(ctx, q, physical)
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
			// CrateDB exposes object sub-columns in information_schema with names like
			// `meta['path']` or `data['user_id']`. These are not physical columns.
			if strings.Contains(c, "['") {
				continue
			}
			cols = append(cols, strings.ToLower(c))
		}
	}
	return cols, nil
}
