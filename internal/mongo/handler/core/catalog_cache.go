package mongo

import (
	"sort"
	"strings"
	"sync"

	"mog/internal/mongo/handler/shared"
)

// catalogCache avoids repeated catalogUpsert round-trips on hot paths.
// It is process-local and best-effort; correctness still relies on the SQL catalog table.
type catalogCache struct {
	mu   sync.RWMutex
	seen map[string]struct{}
}

var globalCatalogCache = &catalogCache{seen: map[string]struct{}{}}

func (c *catalogCache) key(dbName, coll string) string {
	if dbName == "" || coll == "" {
		return ""
	}
	return dbName + "__" + coll
}

func (c *catalogCache) has(dbName, coll string) bool {
	if c == nil {
		return false
	}
	k := c.key(dbName, coll)
	if k == "" {
		return false
	}
	c.mu.RLock()
	_, ok := c.seen[k]
	c.mu.RUnlock()
	return ok
}

func (c *catalogCache) add(dbName, coll string) {
	if c == nil {
		return
	}
	k := c.key(dbName, coll)
	if k == "" {
		return
	}
	c.mu.Lock()
	if c.seen == nil {
		c.seen = map[string]struct{}{}
	}
	c.seen[k] = struct{}{}
	c.mu.Unlock()
}

func (c *catalogCache) remove(dbName, coll string) {
	if c == nil {
		return
	}
	k := c.key(dbName, coll)
	if k == "" {
		return
	}
	c.mu.Lock()
	delete(c.seen, k)
	c.mu.Unlock()
}

func (c *catalogCache) clearDB(dbName string) {
	if c == nil || dbName == "" {
		return
	}
	prefix := dbName + "__"
	c.mu.Lock()
	for k := range c.seen {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(c.seen, k)
		}
	}
	c.mu.Unlock()
}

func (c *catalogCache) clearAll() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.seen = map[string]struct{}{}
	c.mu.Unlock()
}

func (c *catalogCache) listDBs() []string {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	dbs := map[string]struct{}{}
	for k := range c.seen {
		parts := strings.SplitN(k, "__", 2)
		if len(parts) != 2 {
			continue
		}
		if parts[0] != "" {
			dbs[parts[0]] = struct{}{}
		}
	}
	out := make([]string, 0, len(dbs))
	for db := range dbs {
		out = append(out, db)
	}
	sort.Strings(out)
	return out
}

func (c *catalogCache) listCollections(dbName string) []string {
	if c == nil || dbName == "" {
		return nil
	}
	prefix := dbName + "__"
	c.mu.RLock()
	defer c.mu.RUnlock()
	seen := map[string]struct{}{}
	for k := range c.seen {
		if len(k) < len(prefix) || k[:len(prefix)] != prefix {
			continue
		}
		coll := k[len(prefix):]
		if shared.IsInternalCollectionName(coll) {
			continue
		}
		if coll != "" {
			seen[coll] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for coll := range seen {
		out = append(out, coll)
	}
	sort.Strings(out)
	return out
}
