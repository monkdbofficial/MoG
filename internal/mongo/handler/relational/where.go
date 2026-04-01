package relational

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/handler/shared"
)

type Where struct {
	SQL  string
	Args []interface{}
}

type FilterPushdown struct {
	Where          *Where
	PushedFilter   bson.M
	ResidualFilter bson.M
}

func normalizeIDForStorage(v interface{}) interface{} {
	// MoG stores ObjectIds as hex strings for SQL compatibility.
	if oid, ok := v.(bson.ObjectId); ok {
		return oid.Hex()
	}
	return v
}

func encodeDocIDArg(v interface{}) (string, error) {
	// Core stores `doc.id` as a JSON-encoded value (most commonly a JSON string literal).
	// Mirror that here so relational pushdown can match by _id.
	//
	// Examples:
	// - ObjectId("...") -> "\"<hex>\""
	// - "abc" -> "\"abc\""
	// - 123 -> "123"
	b, err := json.Marshal(normalizeIDForStorage(v))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func BuildWhere(filter bson.M) (*Where, bool, error) {
	if len(filter) == 0 {
		return &Where{SQL: "", Args: nil}, true, nil
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
		if cond, ok := shared.CoerceBsonM(val); ok && shared.DocHasOperatorKeys(cond) {
			// Only a small safe subset.
			ops := make([]string, 0, len(cond))
			for op := range cond {
				ops = append(ops, op)
			}
			sort.Strings(ops)

			for _, op := range ops {
				opVal := cond[op]
				// Special-case _id: the column stores JSON-encoded doc ids.
				if field == "_id" {
					switch op {
					case "$in":
						list, ok := shared.CoerceInterfaceSlice(opVal)
						if !ok || len(list) == 0 {
							return nil, false, nil
						}
						placeholders := make([]string, 0, len(list))
						for _, it := range list {
							enc, err := encodeDocIDArg(it)
							if err != nil {
								return nil, false, err
							}
							placeholders = append(placeholders, fmt.Sprintf("$%d", argN))
							args = append(args, enc)
							argN++
						}
						parts = append(parts, fmt.Sprintf("%s IN (%s)", accessor, strings.Join(placeholders, ", ")))
						continue
					case "$ne", "$gt", "$gte", "$lt", "$lte":
						enc, err := encodeDocIDArg(opVal)
						if err != nil {
							return nil, false, err
						}
						opVal = enc
					}
				}
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
					list, ok := shared.CoerceInterfaceSlice(opVal)
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

		// Special-case _id equality.
		if field == "_id" {
			enc, err := encodeDocIDArg(val)
			if err != nil {
				return nil, false, err
			}
			val = enc
		}

		// Equality
		parts = append(parts, fmt.Sprintf("%s = $%d", accessor, argN))
		args = append(args, val)
		argN++
	}

	if len(parts) == 0 {
		return &Where{SQL: "", Args: nil}, true, nil
	}
	return &Where{SQL: strings.Join(parts, " AND "), Args: args}, true, nil
}

func BuildFilterPushdown(filter bson.M) (FilterPushdown, error) {
	pushdown := FilterPushdown{
		Where:          &Where{SQL: "", Args: nil},
		PushedFilter:   bson.M{},
		ResidualFilter: bson.M{},
	}
	if len(filter) == 0 {
		return pushdown, nil
	}

	keys := make([]string, 0, len(filter))
	for k := range filter {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, field := range keys {
		val := filter[field]
		if isSafePushdownCondition(field, val) {
			pushdown.PushedFilter[field] = val
			continue
		}
		pushdown.ResidualFilter[field] = val
	}

	if len(pushdown.PushedFilter) == 0 {
		return pushdown, nil
	}

	where, ok, err := BuildWhere(pushdown.PushedFilter)
	if err != nil {
		return FilterPushdown{}, err
	}
	if !ok || where == nil {
		pushdown.ResidualFilter = mergeFilters(pushdown.PushedFilter, pushdown.ResidualFilter)
		pushdown.PushedFilter = bson.M{}
		pushdown.Where = &Where{SQL: "", Args: nil}
		return pushdown, nil
	}
	pushdown.Where = where
	return pushdown, nil
}

func BuildOrderBy(sortSpec bson.M) (string, bool, error) {
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
		if p == "" || !shared.IsSafeIdentifier(p) {
			return "", false
		}
	}

	// Special-case _id (stored in `id` column as JSON string literal).
	if len(parts) == 1 && parts[0] == "_id" {
		return "id", true
	}

	root := parts[0]
	col := shared.SQLColumnNameForField(root)
	if col == "" || col == "id" || col == "data" {
		return "", false
	}
	acc := col
	for _, p := range parts[1:] {
		acc += fmt.Sprintf("['%s']", p)
	}
	return acc, true
}

func isSafePushdownCondition(field string, val interface{}) bool {
	if field == "" || strings.HasPrefix(field, "$") {
		return false
	}
	if _, ok := relationalAccessor(field); !ok {
		return false
	}

	cond, ok := shared.CoerceBsonM(val)
	if !ok || !shared.DocHasOperatorKeys(cond) {
		return isSafePushdownValue(val)
	}

	if len(cond) == 0 {
		return false
	}
	for op, opVal := range cond {
		switch op {
		case "$gt", "$gte", "$lt", "$lte", "$ne":
			if !isSafePushdownValue(opVal) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func isSafePushdownValue(v interface{}) bool {
	if v == nil {
		return false
	}
	switch v.(type) {
	case string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		bson.ObjectId,
		time.Time:
		return true
	}
	if _, ok := shared.CoerceBsonM(v); ok {
		return false
	}
	if _, ok := shared.CoerceInterfaceSlice(v); ok {
		return false
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return false
	}
	switch rv.Kind() {
	case reflect.Map, reflect.Struct, reflect.Slice, reflect.Array:
		return false
	default:
		return true
	}
}

func mergeFilters(filters ...bson.M) bson.M {
	out := bson.M{}
	for _, filter := range filters {
		for k, v := range filter {
			out[k] = v
		}
	}
	return out
}
