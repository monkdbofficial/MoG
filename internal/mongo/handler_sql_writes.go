package mongo

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"gopkg.in/mgo.v2/bson"

	mpipeline "mog/internal/mongo/pipeline"
)

// SQL-backed write helpers.
func (h *Handler) updateRowFromDoc(ctx context.Context, exec DBExecutor, physical string, docID string, doc bson.M) error {
	if exec == nil || physical == "" || docID == "" {
		return nil
	}
	if err := h.ensureCollectionTableExec(ctx, exec, physical); err != nil {
		return err
	}

	// Ensure columns exist for any new fields.
	for k, v := range doc {
		if k == "" || k == "_id" {
			continue
		}
		col := sqlColumnNameForField(k)
		if col == "" || col == "id" || col == "data" {
			continue
		}
		sqlType := sqlTypeForValue(v)
		if sqlType == "" {
			continue
		}
		if err := h.ensureColumnExec(ctx, exec, physical, col, sqlType); err != nil {
			return err
		}
	}

	cols, err := h.listColumnsExec(ctx, exec, physical)
	if err != nil {
		return err
	}

	setParts := make([]string, 0, len(cols))
	args := make([]interface{}, 0, len(cols)+1)
	hasDataCol := false
	for _, c := range cols {
		if c == "" || c == "id" {
			continue
		}
		if c == "data" {
			hasDataCol = true
			continue
		}
		field := mongoFieldNameForColumn(c)
		v, exists := doc[field]
		if !exists || v == nil {
			setParts = append(setParts, fmt.Sprintf("%s = NULL", c))
			continue
		}

		sqlType := sqlTypeForValue(v)
		switch sqlType {
		case "OBJECT(DYNAMIC)":
			js, err := marshalObject(v)
			if err != nil {
				return err
			}
			args = append(args, js)
			setParts = append(setParts, fmt.Sprintf("%s = CAST($%d AS OBJECT(DYNAMIC))", c, len(args)))
		case "DOUBLE PRECISION":
			if f, ok := mpipeline.ToFloat64Match(v); ok {
				args = append(args, f)
			} else {
				args = append(args, v)
			}
			setParts = append(setParts, fmt.Sprintf("%s = $%d", c, len(args)))
		default:
			if strings.HasPrefix(sqlType, "FLOAT_VECTOR(") {
				lit, _, ok := floatVectorLiteral(v)
				if !ok {
					return fmt.Errorf("invalid FLOAT_VECTOR value for field %q", field)
				}
				setParts = append(setParts, fmt.Sprintf("%s = %s", c, lit))
				continue
			}
			// For TEXT columns, encode arrays/objects as JSON so they can be rehydrated on reads.
			if sqlType == "TEXT" {
				if _, ok := coerceInterfaceSlice(v); ok {
					js, err := marshalObject(v)
					if err != nil {
						return err
					}
					args = append(args, js)
					setParts = append(setParts, fmt.Sprintf("%s = $%d", c, len(args)))
					continue
				}
				if _, ok := coerceBsonM(v); ok {
					js, err := marshalObject(v)
					if err != nil {
						return err
					}
					args = append(args, js)
					setParts = append(setParts, fmt.Sprintf("%s = $%d", c, len(args)))
					continue
				}
			}
			args = append(args, v)
			setParts = append(setParts, fmt.Sprintf("%s = $%d", c, len(args)))
		}
	}

	// Nothing to update besides id.
	if len(setParts) == 0 {
		return nil
	}
	args = append(args, docID)
	sql := fmt.Sprintf("UPDATE doc.%s SET %s WHERE id = $%d", physical, strings.Join(setParts, ", "), len(args))
	if _, err := exec.Exec(ctx, sql, args...); err != nil {
		return err
	}

	// Raw `data` sync: do it as a separate UPDATE. Some backends/versions behave
	// inconsistently when updating many typed columns and an OBJECT/JSON column
	// in the same statement.
	wantRaw := h.storeRawMongoJSON || hasDataCol
	if wantRaw {
		docJSON, err := marshalObject(doc)
		if err != nil {
			return err
		}
		if _, err := exec.Exec(ctx, "UPDATE doc."+physical+" SET data = CAST($1 AS OBJECT(DYNAMIC)) WHERE id = $2", docJSON, docID); err != nil {
			if isUndefinedColumn(err) {
				return nil
			}
			// Best-effort fallback for backends that store raw docs as JSONB.
			if _, err2 := exec.Exec(ctx, "UPDATE doc."+physical+" SET data = CAST($1 AS JSONB) WHERE id = $2", docJSON, docID); err2 == nil {
				return nil
			}
			return err
		}
	}
	return nil
}

func (h *Handler) insertRowFromDoc(ctx context.Context, exec DBExecutor, physical string, docID string, doc bson.M) error {
	if exec == nil || physical == "" || docID == "" {
		return nil
	}
	if err := h.ensureCollectionTableExec(ctx, exec, physical); err != nil {
		return err
	}

	storeRaw := h.storeRawMongoJSON || h.schemaCache().hasColumn(physical, "data")

	cols := []string{"id"}
	exprs := []string{"$1"}
	args := []interface{}{docID}

	if storeRaw {
		cols = append(cols, "data")
		docJSON, err := marshalObject(doc)
		if err != nil {
			return err
		}
		args = append(args, docJSON)
		exprs = append(exprs, fmt.Sprintf("CAST($%d AS OBJECT(DYNAMIC))", len(args)))
	}

	var keys []string
	for k := range doc {
		if k == "" || k == "_id" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := doc[k]
		col := sqlColumnNameForField(k)
		if col == "" || col == "id" || col == "data" {
			continue
		}
		sqlType := sqlTypeForValue(v)
		if sqlType == "" {
			continue
		}
		if err := h.ensureColumnExec(ctx, exec, physical, col, sqlType); err != nil {
			return err
		}
		cols = append(cols, col)
		if v == nil {
			exprs = append(exprs, "NULL")
			continue
		}
		switch sqlType {
		case "OBJECT(DYNAMIC)":
			js, err := marshalObject(v)
			if err != nil {
				return err
			}
			args = append(args, js)
			exprs = append(exprs, fmt.Sprintf("CAST($%d AS OBJECT(DYNAMIC))", len(args)))
		case "DOUBLE PRECISION":
			if f, ok := mpipeline.ToFloat64Match(v); ok {
				args = append(args, f)
			} else {
				args = append(args, v)
			}
			exprs = append(exprs, fmt.Sprintf("$%d", len(args)))
		default:
			if strings.HasPrefix(sqlType, "FLOAT_VECTOR(") {
				lit, _, ok := floatVectorLiteral(v)
				if !ok {
					return fmt.Errorf("invalid FLOAT_VECTOR value for field %q", k)
				}
				exprs = append(exprs, lit)
				continue
			}
			// For TEXT columns, encode arrays/objects as JSON so they can be rehydrated on reads.
			if sqlType == "TEXT" {
				if _, ok := coerceInterfaceSlice(v); ok {
					js, err := marshalObject(v)
					if err != nil {
						return err
					}
					args = append(args, js)
					exprs = append(exprs, fmt.Sprintf("$%d", len(args)))
					continue
				}
				if _, ok := coerceBsonM(v); ok {
					js, err := marshalObject(v)
					if err != nil {
						return err
					}
					args = append(args, js)
					exprs = append(exprs, fmt.Sprintf("$%d", len(args)))
					continue
				}
			}
			args = append(args, v)
			exprs = append(exprs, fmt.Sprintf("$%d", len(args)))
		}
	}

	sql := fmt.Sprintf("INSERT INTO doc.%s (%s) VALUES (%s)", physical, strings.Join(cols, ", "), strings.Join(exprs, ", "))
	_, err := exec.Exec(ctx, sql, args...)
	if err == nil {
		return nil
	}
	if storeRaw && isUndefinedColumn(err) {
		// Backend doesn't have the `data` column; retry without it.
		cols = []string{"id"}
		exprs = []string{"$1"}
		args = []interface{}{docID}
		sql = fmt.Sprintf("INSERT INTO doc.%s (%s) VALUES (%s)", physical, strings.Join(cols, ", "), strings.Join(exprs, ", "))
		_, err2 := exec.Exec(ctx, sql, args...)
		return err2
	}
	return err
}
