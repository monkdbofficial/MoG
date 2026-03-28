package mongo

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/mgo.v2/bson"
)

type relationalWhere struct {
	SQL  string
	Args []interface{}
}

func buildRelationalWhere(filter bson.M) (*relationalWhere, bool, error) {
	if len(filter) == 0 {
		return &relationalWhere{SQL: "", Args: nil}, true, nil
	}

	argN := 1
	var parts []string
	var args []interface{}

	keys := make([]string, 0, len(filter))
	for k := range filter {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, field := range keys {
		val := filter[field]
		if field == "" {
			return nil, false, nil
		}

		accessor, ok := relationalAccessor(field)
		if !ok || accessor == "" {
			return nil, false, nil
		}

		// Operator object?
		if cond, ok := coerceBsonM(val); ok && docHasOperatorKeys(cond) {
			// Only a small safe subset.
			ops := make([]string, 0, len(cond))
			for op := range cond {
				ops = append(ops, op)
			}
			sort.Strings(ops)

			for _, op := range ops {
				opVal := cond[op]
				switch op {
				case "$gt", "$gte", "$lt", "$lte", "$ne":
					sqlOp := map[string]string{"$gt": ">", "$gte": ">=", "$lt": "<", "$lte": "<=", "$ne": "!="}[op]
					// For numeric comparisons, cast to DOUBLE PRECISION. Keep $ne uncasted.
					if op != "$ne" {
						parts = append(parts, fmt.Sprintf("CAST(%s AS DOUBLE PRECISION) %s $%d", accessor, sqlOp, argN))
					} else {
						parts = append(parts, fmt.Sprintf("%s %s $%d", accessor, sqlOp, argN))
					}
					args = append(args, opVal)
					argN++
				case "$in":
					list, ok := coerceInterfaceSlice(opVal)
					if !ok || len(list) == 0 {
						return nil, false, nil
					}
					placeholders := make([]string, 0, len(list))
					for _, it := range list {
						placeholders = append(placeholders, fmt.Sprintf("$%d", argN))
						args = append(args, it)
						argN++
					}
					parts = append(parts, fmt.Sprintf("%s IN (%s)", accessor, strings.Join(placeholders, ", ")))
				default:
					return nil, false, nil
				}
			}
			continue
		}

		// Equality
		parts = append(parts, fmt.Sprintf("%s = $%d", accessor, argN))
		args = append(args, val)
		argN++
	}

	if len(parts) == 0 {
		return &relationalWhere{SQL: "", Args: nil}, true, nil
	}
	return &relationalWhere{SQL: strings.Join(parts, " AND "), Args: args}, true, nil
}

func buildRelationalOrderBy(sortSpec bson.M) (string, bool, error) {
	if len(sortSpec) == 0 {
		return "", true, nil
	}
	keys := make([]string, 0, len(sortSpec))
	for k := range sortSpec {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, field := range keys {
		if field == "" {
			return "", false, nil
		}
		accessor, ok := relationalAccessor(field)
		if !ok || accessor == "" {
			return "", false, nil
		}
		dir := 1
		switch v := sortSpec[field].(type) {
		case int:
			dir = v
		case int32:
			dir = int(v)
		case int64:
			dir = int(v)
		case float32:
			dir = int(v)
		case float64:
			dir = int(v)
		default:
			return "", false, fmt.Errorf("$sort direction must be an integer")
		}
		if dir == 0 {
			dir = 1
		}
		order := "ASC"
		if dir < 0 {
			order = "DESC"
		}
		parts = append(parts, fmt.Sprintf("%s %s", accessor, order))
	}
	if len(parts) == 0 {
		return "", true, nil
	}
	return " ORDER BY " + strings.Join(parts, ", "), true, nil
}

func relationalAccessor(path string) (string, bool) {
	if path == "" {
		return "", false
	}
	parts := strings.Split(path, ".")
	if len(parts) == 0 {
		return "", false
	}
	for _, p := range parts {
		if p == "" || !isSafeIdentifier(p) {
			return "", false
		}
	}

	// Special-case _id (stored in `id` column as JSON string literal).
	if len(parts) == 1 && parts[0] == "_id" {
		return "id", true
	}

	root := parts[0]
	col := sqlColumnNameForField(root)
	if col == "" || col == "id" || col == "data" {
		return "", false
	}
	acc := col
	for _, p := range parts[1:] {
		acc += fmt.Sprintf("['%s']", p)
	}
	return acc, true
}
