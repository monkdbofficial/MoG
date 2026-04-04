package mongo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/handler/shared"
	mpipeline "mog/internal/mongo/pipeline"
)

type uniqueIndexDef struct {
	name   string
	fields []string
}

type uniqueIndexRegistry struct {
	mu      sync.RWMutex
	uniques map[string][]uniqueIndexDef
}

var globalUniqueIndexes = &uniqueIndexRegistry{uniques: map[string][]uniqueIndexDef{}}

// list is a helper used by the adapter.
func (r *uniqueIndexRegistry) list(physical string) []uniqueIndexDef {
	if r == nil || physical == "" {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	src := r.uniques[physical]
	return append([]uniqueIndexDef(nil), src...)
}

// add is a helper used by the adapter.
func (r *uniqueIndexRegistry) add(physical string, defs []uniqueIndexDef) {
	if r == nil || physical == "" || len(defs) == 0 {
		return
	}
	r.mu.Lock()
	r.uniques[physical] = append(r.uniques[physical], defs...)
	r.mu.Unlock()
}

// clear is a helper used by the adapter.
func (r *uniqueIndexRegistry) clear(physical string) {
	if r == nil || physical == "" {
		return
	}
	r.mu.Lock()
	delete(r.uniques, physical)
	r.mu.Unlock()
}

// clearDB is a helper used by the adapter.
func (r *uniqueIndexRegistry) clearDB(dbName string) {
	if r == nil || dbName == "" {
		return
	}
	prefix := dbName + "__"
	r.mu.Lock()
	for k := range r.uniques {
		if strings.HasPrefix(k, prefix) {
			delete(r.uniques, k)
		}
	}
	r.mu.Unlock()
}

// listUniqueIndexes is a helper used by the adapter.
func (h *Handler) listUniqueIndexes(physical string) []uniqueIndexDef {
	return globalUniqueIndexes.list(physical)
}

// addUniqueIndexes is a helper used by the adapter.
func (h *Handler) addUniqueIndexes(physical string, defs []uniqueIndexDef) {
	globalUniqueIndexes.add(physical, defs)
}

// clearUniqueIndexes is a helper used by the adapter.
func (h *Handler) clearUniqueIndexes(physical string) {
	globalUniqueIndexes.clear(physical)
}

// clearUniqueIndexesForDB is a helper used by the adapter.
func (h *Handler) clearUniqueIndexesForDB(dbName string) {
	globalUniqueIndexes.clearDB(dbName)
}

// checkUniqueViolation is a helper used by the adapter.
func (h *Handler) checkUniqueViolation(ctx context.Context, physical string, docID string, doc bson.M, excludeDocID string) (dupKeyMsg string, indexName string, violated bool) {
	if physical == "" || doc == nil {
		return "", "", false
	}

	indexes := h.listUniqueIndexes(physical)
	if len(indexes) == 0 {
		return "", "", false
	}

	var (
		pdocs       []pureSQLDoc
		pdocsLoaded bool
	)

	for _, idx := range indexes {
		newKeyTuples, ok := uniqueIndexKeyTuples(doc, idx.fields)
		if !ok || len(newKeyTuples) == 0 {
			continue
		}

		// Fast path: if the unique key is a single scalar tuple and all fields map to simple columns,
		// check duplicates with a single SQL query instead of scanning all docs.
		if len(newKeyTuples) == 1 && uniqueIndexTupleIsScalar(newKeyTuples[0]) {
			if exists, err := h.uniqueKeyExistsSQL(ctx, h.db(), physical, idx.fields, newKeyTuples[0], excludeDocID, docID); err == nil {
				if exists {
					parts := make([]string, 0, len(idx.fields))
					for _, f := range idx.fields {
						parts = append(parts, fmt.Sprintf("%s: %v", f, mpipeline.GetPathValue(doc, f)))
					}
					return "{" + strings.Join(parts, ", ") + "}", idx.name, true
				}
				continue
			}
			// On SQL errors, fall back to in-memory behavior.
		}

		if !pdocsLoaded {
			var err error
			pdocs, err = h.loadSQLDocsWithIDs(ctx, h.db(), physical)
			if err != nil {
				return "", "", false
			}
			pdocsLoaded = true
		}

		for _, existing := range pdocs {
			if excludeDocID != "" && existing.docID == excludeDocID {
				continue
			}
			if docID != "" && existing.docID == docID {
				continue
			}

			existingKeyTuples, ok := uniqueIndexKeyTuples(existing.doc, idx.fields)
			if !ok || len(existingKeyTuples) == 0 {
				continue
			}
			if uniqueIndexKeyTuplesIntersect(newKeyTuples, existingKeyTuples) {
				parts := make([]string, 0, len(idx.fields))
				for _, f := range idx.fields {
					parts = append(parts, fmt.Sprintf("%s: %v", f, mpipeline.GetPathValue(doc, f)))
				}
				return "{" + strings.Join(parts, ", ") + "}", idx.name, true
			}
		}
	}

	return "", "", false
}

// uniqueIndexTupleIsScalar is a helper used by the adapter.
func uniqueIndexTupleIsScalar(tuple []interface{}) bool {
	for _, v := range tuple {
		if v == nil {
			return false
		}
		if _, ok := shared.CoerceInterfaceSlice(v); ok {
			return false
		}
		if _, ok := shared.CoerceBsonM(v); ok {
			return false
		}
	}
	return true
}

// uniqueKeyExistsSQL is a helper used by the adapter.
func (h *Handler) uniqueKeyExistsSQL(ctx context.Context, exec DBExecutor, physical string, fields []string, tuple []interface{}, excludeDocID string, docID string) (bool, error) {
	if exec == nil || physical == "" || len(fields) == 0 || len(fields) != len(tuple) {
		return false, fmt.Errorf("invalid unique key query args")
	}

	conds := make([]string, 0, len(fields)+1)
	args := make([]interface{}, 0, len(fields)+1)
	for i, f := range fields {
		// Only support top-level scalar fields in the fast path.
		if strings.Contains(f, ".") {
			return false, fmt.Errorf("dotted unique keys not supported")
		}
		col := shared.SQLColumnNameForField(f)
		if col == "" || col == "id" || col == "data" {
			return false, fmt.Errorf("unsupported unique index field %q", f)
		}
		args = append(args, tuple[i])
		conds = append(conds, fmt.Sprintf("%s = $%d", col, len(args)))
	}

	exclude := excludeDocID
	if exclude == "" {
		exclude = docID
	}
	if exclude != "" {
		args = append(args, exclude)
		conds = append(conds, fmt.Sprintf("id <> $%d", len(args)))
	}

	q := "SELECT 1 FROM doc." + physical + " WHERE " + strings.Join(conds, " AND ") + " LIMIT 1"
	var one int
	err := exec.QueryRow(ctx, q, args...).Scan(&one)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return false, err
}

// uniqueIndexKeyTuples is a helper used by the adapter.
func uniqueIndexKeyTuples(doc bson.M, fields []string) ([][]interface{}, bool) {
	if doc == nil || len(fields) == 0 {
		return nil, false
	}

	valueSets := make([][]interface{}, 0, len(fields))
	for _, f := range fields {
		v := mpipeline.GetPathValue(doc, f)
		if v == nil {
			return nil, false
		}
		values := explodeUniqueIndexValues(v)
		// Empty arrays generate no index entries; treat as not enforceable.
		if len(values) == 0 {
			return nil, false
		}
		valueSets = append(valueSets, values)
	}

	// Cartesian product of per-field values. Cap to avoid pathological blow-ups.
	const maxTuples = 4096
	total := 1
	for _, vs := range valueSets {
		if len(vs) == 0 {
			return nil, false
		}
		total *= len(vs)
		if total > maxTuples {
			return nil, false
		}
	}

	out := make([][]interface{}, 0, total)
	var build func(i int, cur []interface{})
	build = func(i int, cur []interface{}) {
		if i == len(valueSets) {
			t := make([]interface{}, len(cur))
			copy(t, cur)
			out = append(out, t)
			return
		}
		for _, v := range valueSets[i] {
			build(i+1, append(cur, v))
		}
	}
	build(0, nil)
	return out, true
}

// explodeUniqueIndexValues is a helper used by the adapter.
func explodeUniqueIndexValues(v interface{}) []interface{} {
	if v == nil {
		return nil
	}
	if arr, ok := shared.CoerceInterfaceSlice(v); ok {
		out := make([]interface{}, 0, len(arr))
		for _, el := range arr {
			if el == nil {
				continue
			}
			out = append(out, el)
		}
		return out
	}
	return []interface{}{v}
}

// uniqueIndexKeyTuplesIntersect is a helper used by the adapter.
func uniqueIndexKeyTuplesIntersect(a, b [][]interface{}) bool {
	for _, ka := range a {
		for _, kb := range b {
			if uniqueIndexKeyTupleEquals(ka, kb) {
				return true
			}
		}
	}
	return false
}

// uniqueIndexKeyTupleEquals is a helper used by the adapter.
func uniqueIndexKeyTupleEquals(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if !mpipeline.MatchEquals(a[i], b[i]) {
			return false
		}
	}
	return true
}
