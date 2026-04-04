// Package translator converts a subset of MongoDB query/update semantics into
// SQL statements over MonkDB/Postgres JSONB storage.
//
// The Translator is used by the Mongo wire-protocol handler to turn incoming
// command documents into parameterized SQL plus argument lists suitable for
// pgx. The translation is intentionally conservative: unsupported operators or
// unsafe field paths are rejected with an error rather than being guessed.
package translator

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/mgo.v2/bson"
)

// Translator translates MongoDB-like command documents into parameterized SQL.
//
// Methods generally return:
//   - a SQL string containing Postgres-style placeholders (`$1`, `$2`, ...),
//   - a corresponding argument slice in placeholder order, and
//   - an error when translation fails (unsupported operator, unsafe identifier,
//     invalid stage shape, etc).
//
// Translator is stateless; a single instance can be reused across requests.
type Translator struct{}

// New returns a new Translator.
func New() *Translator {
	return &Translator{}
}

// AggregatePlan describes a fully translated SQL plan for an aggregation
// pipeline.
//
// Fields lists the output column names in result order when the translation
// produces a structured projection rather than a raw JSONB document.
type AggregatePlan struct {
	SQL    string
	Args   []interface{}
	Fields []string // column names in result order
}

// AggregatePrefixPlan describes a partial "pushdown" translation for the
// prefix of an aggregation pipeline.
//
// The handler uses this to push supported leading stages into SQL while
// leaving the remaining stages to be executed in-memory.
type AggregatePrefixPlan struct {
	Plan           *AggregatePlan
	ConsumedStages int
}

// coerceDoc is a helper used by the adapter.
func coerceDoc(v interface{}) (bson.M, bool) {
	switch t := v.(type) {
	case bson.M:
		return t, true
	case map[string]interface{}:
		return bson.M(t), true
	case bson.D:
		m := bson.M{}
		for _, e := range t {
			m[e.Name] = e.Value
		}
		return m, true
	default:
		return nil, false
	}
}

// TranslateFind translates a MongoDB `find` filter into a `SELECT ... WHERE`
// query for the given collection.
//
// If filter is empty, the returned query selects all documents in the
// collection. Errors are returned for unsupported operators or unsafe field
// paths.
func (t *Translator) TranslateFind(collection string, filter bson.M) (string, []interface{}, error) {
	table := tableName(collection)
	if len(filter) == 0 {
		return fmt.Sprintf("SELECT data FROM %s", table), nil, nil
	}

	where, args, err := t.translateFilter(filter)
	if err != nil {
		return "", nil, err
	}

	query := fmt.Sprintf("SELECT data FROM %s WHERE %s", table, where)
	return query, args, nil
}

// TranslateFindWithOptions translates a MongoDB find filter with optional sort/skip/limit into SQL.
//
// Notes:
// - sort keys are applied in a deterministic (lexicographic) order, matching applySort's behavior.
// - skip is translated to OFFSET
// - limit is translated to LIMIT
func (t *Translator) TranslateFindWithOptions(collection string, filter bson.M, sortSpec bson.M, skip, limit int) (string, []interface{}, error) {
	table := tableName(collection)
	argCount := 1

	where := ""
	args := []interface{}(nil)
	if len(filter) > 0 {
		w, wargs, err := t.translateFilterWithArgs(filter, &argCount)
		if err != nil {
			return "", nil, err
		}
		if w != "" {
			where = " WHERE " + w
			args = append(args, wargs...)
		}
	}

	query := fmt.Sprintf("SELECT data FROM %s%s", table, where)

	if len(sortSpec) > 0 {
		var keys []string
		for k := range sortSpec {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		var sortParts []string
		for _, field := range keys {
			if !isSafePath(field) {
				return "", nil, fmt.Errorf("invalid sort field name: %s", field)
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
				return "", nil, fmt.Errorf("$sort direction must be an integer")
			}
			if dir == 0 {
				dir = 1
			}
			direction := "ASC"
			if dir < 0 {
				direction = "DESC"
			}
			sortParts = append(sortParts, fmt.Sprintf("%s %s", dataAccessor(field), direction))
		}
		if len(sortParts) > 0 {
			query = fmt.Sprintf("%s ORDER BY %s", query, strings.Join(sortParts, ", "))
		}
	}

	if skip < 0 {
		skip = 0
	}
	if limit < 0 {
		limit = 0
	}

	if limit > 0 {
		query = fmt.Sprintf("%s LIMIT %d", query, limit)
	}
	if skip > 0 {
		query = fmt.Sprintf("%s OFFSET %d", query, skip)
	}

	return query, args, nil
}

// TranslateCount translates a MongoDB `count` filter into a `SELECT COUNT(*)`
// query for the given collection.
//
// If filter is empty, the returned query counts all documents. Errors are
// returned for unsupported operators or unsafe field paths.
func (t *Translator) TranslateCount(collection string, filter bson.M) (string, []interface{}, error) {
	table := tableName(collection)
	if len(filter) == 0 {
		return fmt.Sprintf("SELECT COUNT(*) FROM %s", table), nil, nil
	}

	where, args, err := t.translateFilter(filter)
	if err != nil {
		return "", nil, err
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", table, where)
	return query, args, nil
}

// translateFilter is a helper used by the adapter.
func (t *Translator) translateFilter(filter bson.M) (string, []interface{}, error) {
	conditions := make([]string, 0, len(filter))
	args := make([]interface{}, 0, len(filter))
	argCount := 1

	for k, v := range filter {
		condition, val, err := t.translateCondition(k, v, &argCount)
		if err != nil {
			return "", nil, err
		}
		conditions = append(conditions, condition)
		args = append(args, val...)
	}

	return strings.Join(conditions, " AND "), args, nil
}

// translateCondition is a helper used by the adapter.
func (t *Translator) translateCondition(field string, val interface{}, argCount *int) (string, []interface{}, error) {
	if !isSafePath(field) {
		return "", nil, fmt.Errorf("invalid field name: %s", field)
	}
	acc := dataAccessor(field)

	// Handle special operators if val is a map
	if m, ok := coerceDoc(val); ok {
		hasOp := false
		for op, opVal := range m {
			if strings.HasPrefix(op, "$") {
				hasOp = true
			}
			switch op {
			case "$gt":
				cond := fmt.Sprintf("CAST(%s AS DOUBLE PRECISION) > $%d", acc, *argCount)
				*argCount++
				return cond, []interface{}{opVal}, nil
			case "$lt":
				cond := fmt.Sprintf("CAST(%s AS DOUBLE PRECISION) < $%d", acc, *argCount)
				*argCount++
				return cond, []interface{}{opVal}, nil
			case "$gte":
				cond := fmt.Sprintf("CAST(%s AS DOUBLE PRECISION) >= $%d", acc, *argCount)
				*argCount++
				return cond, []interface{}{opVal}, nil
			case "$lte":
				cond := fmt.Sprintf("CAST(%s AS DOUBLE PRECISION) <= $%d", acc, *argCount)
				*argCount++
				return cond, []interface{}{opVal}, nil
			case "$ne":
				cond := fmt.Sprintf("%s != $%d", acc, *argCount)
				*argCount++
				return cond, []interface{}{opVal}, nil
			case "$in":
				// Handle $in operator
				if inList, ok := opVal.([]interface{}); ok {
					placeholders := make([]string, 0, len(inList))
					vals := make([]interface{}, 0, len(inList))
					for _, item := range inList {
						placeholders = append(placeholders, fmt.Sprintf("$%d", *argCount))
						vals = append(vals, item)
						*argCount++
					}
					cond := fmt.Sprintf("%s IN (%s)", acc, strings.Join(placeholders, ", "))
					return cond, vals, nil
				}
				return "", nil, fmt.Errorf("$in must be an array")
			default:
				if hasOp && strings.HasPrefix(op, "$") {
					return "", nil, fmt.Errorf("unsupported operator: %s", op)
				}
			}
		}
		if hasOp {
			return "", nil, fmt.Errorf("unsupported operator document")
		}
	}

	cond := fmt.Sprintf("%s = $%d", acc, *argCount)
	*argCount++
	return cond, []interface{}{val}, nil
}

// TranslateInsert translates a MongoDB `insert` document into a SQL INSERT
// statement for the given collection.
//
// The returned SQL uses a single parameter cast to OBJECT so callers can pass a
// document value that is marshalled over the Postgres wire protocol. This
// method does not currently validate document keys; validation and normalization
// are expected to happen at higher layers.
func (t *Translator) TranslateInsert(collection string, doc bson.M) (string, []interface{}, error) {
	// Cast the parameter to OBJECT so callers can pass JSON text (works well over the Postgres wire protocol).
	query := fmt.Sprintf("INSERT INTO %s (data) VALUES (CAST($1 AS OBJECT))", tableName(collection))
	return query, []interface{}{doc}, nil
}

// TranslateUpdate translates a MongoDB `update` command into a SQL UPDATE
// statement.
//
// Supported operators:
//   - `$set` for setting individual fields
//   - `$inc` for numeric increments (missing/NULL values are treated as 0)
//
// If update contains no operator keys, it is treated as a replacement document
// and translated as a `$set` of top-level fields.
//
// Errors are returned for unsupported operators, invalid/unsafe field paths, or
// when the update document does not contain any supported assignments.
func (t *Translator) TranslateUpdate(collection string, filter bson.M, update bson.M) (string, []interface{}, error) {
	setDoc, _ := coerceDoc(update["$set"])
	incDoc, _ := coerceDoc(update["$inc"])

	// Treat a replacement document (no operators) as $set for now.
	if setDoc == nil && incDoc == nil {
		setDoc = update
	}

	argCount := 1
	where, args, err := t.translateFilterWithArgs(filter, &argCount)
	if err != nil {
		return "", nil, err
	}
	if where == "" {
		where = "TRUE"
	}

	var setParts []string

	// $inc: update numeric fields using the existing value.
	if incDoc != nil {
		var keys []string
		for k := range incDoc {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, field := range keys {
			if !isSafePath(field) {
				return "", nil, fmt.Errorf("invalid $inc field name: %s", field)
			}
			acc := dataAccessor(field)
			setParts = append(setParts, fmt.Sprintf("%s = COALESCE(CAST(%s AS DOUBLE PRECISION), 0) + $%d", acc, acc, argCount))
			args = append(args, incDoc[field])
			argCount++
		}
	}

	// $set: set individual fields (more compatible than object-merge across backends).
	if setDoc != nil && len(setDoc) > 0 {
		var keys []string
		for k := range setDoc {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, field := range keys {
			if !isSafePath(field) {
				return "", nil, fmt.Errorf("invalid $set field name: %s", field)
			}
			setParts = append(setParts, fmt.Sprintf("%s = $%d", dataAccessor(field), argCount))
			args = append(args, setDoc[field])
			argCount++
		}
	}

	if len(setParts) == 0 {
		return "", nil, fmt.Errorf("no supported update operators")
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableName(collection), strings.Join(setParts, ", "), where)
	return query, args, nil
}

// TranslateDelete translates a MongoDB `delete` filter into a SQL DELETE
// statement for the given collection.
//
// If filter is empty, the returned query deletes all rows in the collection.
// Errors are returned for unsupported operators or unsafe field paths.
func (t *Translator) TranslateDelete(collection string, filter bson.M) (string, []interface{}, error) {
	table := tableName(collection)
	if len(filter) == 0 {
		return fmt.Sprintf("DELETE FROM %s", table), nil, nil
	}

	where, args, err := t.translateFilter(filter)
	if err != nil {
		return "", nil, err
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s", table, where)
	return query, args, nil
}

// TranslateAggregate translates a MongoDB aggregation pipeline into a best-effort
// SQL query.
//
// This method supports a small subset of stages (currently `$match`, `$sort`,
// `$limit`) and builds nested subqueries as needed. For strict pushdown planning
// (with validation and richer projections), prefer TranslateAggregatePlan or
// TranslateAggregatePrefixPlan.
func (t *Translator) TranslateAggregate(collection string, pipeline []bson.M) (string, []interface{}, error) {
	var args []interface{}
	argCount := 1

	query := fmt.Sprintf("SELECT data FROM %s", tableName(collection))

	for _, stage := range pipeline {
		for op, val := range stage {
			switch op {
			case "$match":
				m, ok := val.(bson.M)
				if !ok {
					return "", nil, fmt.Errorf("$match stage must be a document")
				}
				var stageConditions []string
				for k, v := range m {
					condition, val, err := t.translateCondition(k, v, &argCount)
					if err != nil {
						return "", nil, err
					}
					stageConditions = append(stageConditions, condition)
					args = append(args, val...)
				}
				where := strings.Join(stageConditions, " AND ")
				query = fmt.Sprintf("SELECT * FROM (%s) sub WHERE %s", query, where)
			case "$limit":
				limit, ok := val.(int)
				if !ok {
					// Handle cases where limit might be int32 or int64
					if i32, ok := val.(int32); ok {
						limit = int(i32)
					} else if i64, ok := val.(int64); ok {
						limit = int(i64)
					} else {
						return "", nil, fmt.Errorf("$limit stage must be an integer")
					}
				}
				query = fmt.Sprintf("%s LIMIT %d", query, limit)
			case "$sort":
				s, ok := val.(bson.M)
				if !ok {
					return "", nil, fmt.Errorf("$sort stage must be a document")
				}
				var sortParts []string
				for field, dir := range s {
					if !isSafePath(field) {
						return "", nil, fmt.Errorf("invalid sort field name: %s", field)
					}
					direction := "ASC"
					if d, ok := dir.(int); ok && d < 0 {
						direction = "DESC"
					}
					sortParts = append(sortParts, fmt.Sprintf("%s %s", dataAccessor(field), direction))
				}
				query = fmt.Sprintf("%s ORDER BY %s", query, strings.Join(sortParts, ", "))
			}
		}
	}

	return query, args, nil
}

// TranslateAggregatePlan translates a supported aggregation pipeline into a SQL plan.
//
// Supported pipeline shape (strict):
//   - one or more leading $match stages (optional)
//   - optional $group
//   - optional $sort
//   - optional $limit
//
// Any other stage returns an error. This is intended for DB pushdown execution.
func (t *Translator) TranslateAggregatePlan(collection string, pipeline []bson.M) (*AggregatePlan, error) {
	argCount := 1
	whereParts := []string{}
	args := []interface{}{}

	table := tableName(collection)

	// Collect leading $match stages.
	stages := pipeline
	for len(stages) > 0 {
		m, ok := stages[0]["$match"].(bson.M)
		if !ok {
			break
		}
		where, wargs, err := t.translateFilterWithArgs(m, &argCount)
		if err != nil {
			return nil, err
		}
		if where != "" {
			whereParts = append(whereParts, where)
			args = append(args, wargs...)
		}
		stages = stages[1:]
	}

	whereClause := ""
	if len(whereParts) > 0 {
		whereClause = " WHERE " + strings.Join(whereParts, " AND ")
	}

	// No $group => return rows as documents, optionally sorted/limited.
	if len(stages) == 0 || stages[0]["$group"] == nil {
		query := fmt.Sprintf("SELECT data FROM %s%s", table, whereClause)

		for _, stage := range stages {
			switch {
			case stage["$sort"] != nil:
				s, ok := stage["$sort"].(bson.M)
				if !ok {
					return nil, fmt.Errorf("$sort stage must be a document")
				}
				var sortParts []string
				for field, dir := range s {
					if !isSafePath(field) {
						return nil, fmt.Errorf("invalid sort field name: %s", field)
					}
					direction := "ASC"
					if d, ok := dir.(int); ok && d < 0 {
						direction = "DESC"
					}
					sortParts = append(sortParts, fmt.Sprintf("%s %s", dataAccessor(field), direction))
				}
				query = fmt.Sprintf("%s ORDER BY %s", query, strings.Join(sortParts, ", "))
			case stage["$limit"] != nil:
				limit, ok := stage["$limit"].(int)
				if !ok {
					if i32, ok := stage["$limit"].(int32); ok {
						limit = int(i32)
					} else if i64, ok := stage["$limit"].(int64); ok {
						limit = int(i64)
					} else {
						return nil, fmt.Errorf("$limit stage must be an integer")
					}
				}
				query = fmt.Sprintf("%s LIMIT %d", query, limit)
			default:
				return nil, fmt.Errorf("unsupported aggregation stage for SQL pushdown: %v", stage)
			}
		}

		return &AggregatePlan{SQL: query, Args: args, Fields: []string{"data"}}, nil
	}

	// $group pushdown (with optional trailing $sort/$limit).
	groupSpec, ok := stages[0]["$group"].(bson.M)
	if !ok {
		return nil, fmt.Errorf("$group stage must be a document")
	}
	stages = stages[1:]

	rawID, hasID := groupSpec["_id"]
	if !hasID {
		return nil, fmt.Errorf("$group requires _id")
	}

	// Only support _id: "$path" or constant (including nil).
	idExpr := "NULL"
	groupByExpr := ""
	havingClause := ""
	if s, ok := rawID.(string); ok && strings.HasPrefix(s, "$") && len(s) > 1 {
		path := strings.TrimPrefix(s, "$")
		if !isSafePath(path) {
			return nil, fmt.Errorf("invalid group _id path: %s", path)
		}
		idExpr = dataAccessor(path)
		groupByExpr = idExpr
	} else if rawID != nil {
		// Constant id => single-group aggregation; avoid GROUP BY for backend compatibility.
		switch v := rawID.(type) {
		case string:
			idExpr = fmt.Sprintf("CAST($%d AS TEXT)", argCount)
			args = append(args, v)
			argCount++
		case int:
			idExpr = fmt.Sprintf("CAST($%d AS DOUBLE PRECISION)", argCount)
			args = append(args, v)
			argCount++
		case int32:
			idExpr = fmt.Sprintf("CAST($%d AS DOUBLE PRECISION)", argCount)
			args = append(args, int64(v))
			argCount++
		case int64:
			idExpr = fmt.Sprintf("CAST($%d AS DOUBLE PRECISION)", argCount)
			args = append(args, v)
			argCount++
		case float32:
			idExpr = fmt.Sprintf("CAST($%d AS DOUBLE PRECISION)", argCount)
			args = append(args, float64(v))
			argCount++
		case float64:
			idExpr = fmt.Sprintf("CAST($%d AS DOUBLE PRECISION)", argCount)
			args = append(args, v)
			argCount++
		case bool:
			idExpr = fmt.Sprintf("CAST($%d AS BOOLEAN)", argCount)
			args = append(args, v)
			argCount++
		default:
			return nil, fmt.Errorf("unsupported $group _id: %v", rawID)
		}
		// Match MongoDB semantics: if input is empty, $group emits no output.
		havingClause = " HAVING COUNT(*) > 0"
	} else {
		// _id: null => single-group aggregation; suppress the "empty input" row.
		idExpr = "NULL"
		havingClause = " HAVING COUNT(*) > 0"
	}

	outFields := []string{"_id"}
	selectParts := []string{fmt.Sprintf("%s AS _id", idExpr)}

	// Accumulators.
	for outField, v := range groupSpec {
		if outField == "_id" {
			continue
		}
		acc, ok := v.(bson.M)
		if !ok {
			return nil, fmt.Errorf("$group accumulator %q must be a document", outField)
		}
		if avgArg, ok := acc["$avg"]; ok {
			path, ok := avgArg.(string)
			if !ok || !strings.HasPrefix(path, "$") || len(path) < 2 {
				return nil, fmt.Errorf("$avg must reference a field path")
			}
			path = strings.TrimPrefix(path, "$")
			if !isSafePath(path) {
				return nil, fmt.Errorf("invalid $avg path: %s", path)
			}
			expr := fmt.Sprintf("AVG(CAST(%s AS DOUBLE PRECISION))", dataAccessor(path))
			selectParts = append(selectParts, fmt.Sprintf("%s AS %s", expr, outField))
			outFields = append(outFields, outField)
			continue
		}
		if sumArg, ok := acc["$sum"]; ok {
			// $sum: 1 => COUNT(*)
			switch sumArg.(type) {
			case int, int32, int64:
				if isNumericOne(sumArg) {
					selectParts = append(selectParts, fmt.Sprintf("COUNT(*) AS %s", outField))
					outFields = append(outFields, outField)
					continue
				}
			}

			path, ok := sumArg.(string)
			if !ok || !strings.HasPrefix(path, "$") || len(path) < 2 {
				return nil, fmt.Errorf("$sum must be 1 or a field path")
			}
			path = strings.TrimPrefix(path, "$")
			if !isSafePath(path) {
				return nil, fmt.Errorf("invalid $sum path: %s", path)
			}
			accExpr := dataAccessor(path)
			expr := fmt.Sprintf("SUM(COALESCE(CAST(%s AS DOUBLE PRECISION), 0))", accExpr)
			selectParts = append(selectParts, fmt.Sprintf("%s AS %s", expr, outField))
			outFields = append(outFields, outField)
			continue
		}
		if _, ok := acc["$addToSet"]; ok {
			return nil, fmt.Errorf("$addToSet is not supported for SQL pushdown")
		}
		return nil, fmt.Errorf("unsupported $group accumulator: %v", acc)
	}

	groupOrHaving := ""
	if groupByExpr != "" {
		groupOrHaving = " GROUP BY " + groupByExpr
	} else {
		groupOrHaving = havingClause
	}

	query := fmt.Sprintf(
		"SELECT %s FROM %s%s%s",
		strings.Join(selectParts, ", "),
		table,
		whereClause,
		groupOrHaving,
	)

	// Optional trailing $sort/$limit only.
	for _, stage := range stages {
		switch {
		case stage["$sort"] != nil:
			s, ok := stage["$sort"].(bson.M)
			if !ok {
				return nil, fmt.Errorf("$sort stage must be a document")
			}
			var sortParts []string
			for field, dir := range s {
				// Allow sorting by _id or accumulator aliases.
				if field != "_id" {
					found := false
					for _, f := range outFields {
						if f == field {
							found = true
							break
						}
					}
					if !found {
						return nil, fmt.Errorf("unsupported sort field after $group: %s", field)
					}
				}
				direction := "ASC"
				if d, ok := dir.(int); ok && d < 0 {
					direction = "DESC"
				} else if d, ok := dir.(int32); ok && d < 0 {
					direction = "DESC"
				} else if d, ok := dir.(int64); ok && d < 0 {
					direction = "DESC"
				}
				sortParts = append(sortParts, fmt.Sprintf("%s %s", field, direction))
			}
			query = fmt.Sprintf("%s ORDER BY %s", query, strings.Join(sortParts, ", "))
		case stage["$limit"] != nil:
			limit, ok := stage["$limit"].(int)
			if !ok {
				if i32, ok := stage["$limit"].(int32); ok {
					limit = int(i32)
				} else if i64, ok := stage["$limit"].(int64); ok {
					limit = int(i64)
				} else {
					return nil, fmt.Errorf("$limit stage must be an integer")
				}
			}
			query = fmt.Sprintf("%s LIMIT %d", query, limit)
		default:
			return nil, fmt.Errorf("unsupported aggregation stage for SQL pushdown: %v", stage)
		}
	}

	return &AggregatePlan{SQL: query, Args: args, Fields: outFields}, nil
}

// isNumericOne is a helper used by the adapter.
func isNumericOne(v interface{}) bool {
	switch x := v.(type) {
	case int:
		return x == 1
	case int32:
		return x == 1
	case int64:
		return x == 1
	default:
		return false
	}
}

// TranslateAggregatePrefixPlan attempts to translate the longest leading prefix of an aggregation pipeline into SQL.
//
// It returns:
// - Plan: SQL + args + output fields for the pushed-down prefix (nil if nothing can be pushed down)
// - ConsumedStages: number of pipeline stages consumed by the SQL plan
//
// Stages eligible for prefix pushdown:
// - one or more leading $match stages (merged)
// - optional $group (limited accumulators: $sum/$avg)
// - optional $sort
// - optional $limit
// - optional $count (stops the prefix)
//
// When a stage is not supported for SQL pushdown, translation stops and the remaining stages
// can be evaluated in-memory by the pipeline engine.
func (t *Translator) TranslateAggregatePrefixPlan(collection string, pipeline []bson.M) (*AggregatePrefixPlan, error) {
	if len(pipeline) == 0 {
		return &AggregatePrefixPlan{Plan: &AggregatePlan{
			SQL:    fmt.Sprintf("SELECT data FROM %s", tableName(collection)),
			Args:   nil,
			Fields: []string{"data"},
		}, ConsumedStages: 0}, nil
	}

	argCount := 1
	whereParts := []string{}
	args := []interface{}{}
	table := tableName(collection)

	consumed := 0
	rest := pipeline

	// Leading $match stages.
	for len(rest) > 0 {
		m, ok := rest[0]["$match"].(bson.M)
		if !ok {
			break
		}
		where, wargs, err := t.translateFilterWithArgs(m, &argCount)
		if err != nil {
			// Cannot translate match => cannot push down any prefix.
			return nil, err
		}
		if where != "" {
			whereParts = append(whereParts, where)
			args = append(args, wargs...)
		}
		consumed++
		rest = rest[1:]
	}

	whereClause := ""
	if len(whereParts) > 0 {
		whereClause = " WHERE " + strings.Join(whereParts, " AND ")
	}

	// If next is $count, push it down and stop.
	if len(rest) > 0 && rest[0]["$count"] != nil {
		field, ok := rest[0]["$count"].(string)
		if !ok || field == "" {
			return nil, fmt.Errorf("$count stage must be a non-empty string")
		}
		consumed++
		sql := fmt.Sprintf("SELECT COUNT(*) AS %s FROM %s%s", field, table, whereClause)
		return &AggregatePrefixPlan{
			Plan:           &AggregatePlan{SQL: sql, Args: args, Fields: []string{field}},
			ConsumedStages: consumed,
		}, nil
	}

	// Optional $group.
	groupPushed := false
	outFields := []string{"data"}
	query := fmt.Sprintf("SELECT data FROM %s%s", table, whereClause)
	if len(rest) > 0 && rest[0]["$group"] != nil {
		groupSpec, ok := rest[0]["$group"].(bson.M)
		if !ok {
			return nil, fmt.Errorf("$group stage must be a document")
		}

		rawID, hasID := groupSpec["_id"]
		if !hasID {
			return nil, fmt.Errorf("$group requires _id")
		}

		idExpr := "NULL"
		groupByExpr := ""
		havingClause := ""
		if s, ok := rawID.(string); ok && strings.HasPrefix(s, "$") && len(s) > 1 {
			path := strings.TrimPrefix(s, "$")
			if !isSafePath(path) {
				return nil, fmt.Errorf("invalid group _id path: %s", path)
			}
			idExpr = dataAccessor(path)
			groupByExpr = idExpr
		} else if rawID != nil {
			switch v := rawID.(type) {
			case string:
				idExpr = fmt.Sprintf("CAST($%d AS TEXT)", argCount)
				args = append(args, v)
				argCount++
			case int:
				idExpr = fmt.Sprintf("CAST($%d AS DOUBLE PRECISION)", argCount)
				args = append(args, v)
				argCount++
			case int32:
				idExpr = fmt.Sprintf("CAST($%d AS DOUBLE PRECISION)", argCount)
				args = append(args, int64(v))
				argCount++
			case int64:
				idExpr = fmt.Sprintf("CAST($%d AS DOUBLE PRECISION)", argCount)
				args = append(args, v)
				argCount++
			case float32:
				idExpr = fmt.Sprintf("CAST($%d AS DOUBLE PRECISION)", argCount)
				args = append(args, float64(v))
				argCount++
			case float64:
				idExpr = fmt.Sprintf("CAST($%d AS DOUBLE PRECISION)", argCount)
				args = append(args, v)
				argCount++
			case bool:
				idExpr = fmt.Sprintf("CAST($%d AS BOOLEAN)", argCount)
				args = append(args, v)
				argCount++
			default:
				return nil, fmt.Errorf("unsupported $group _id: %v", rawID)
			}
			havingClause = " HAVING COUNT(*) > 0"
		} else {
			idExpr = "NULL"
			havingClause = " HAVING COUNT(*) > 0"
		}

		selectParts := []string{fmt.Sprintf("%s AS _id", idExpr)}
		outFields = []string{"_id"}

		for outField, v := range groupSpec {
			if outField == "_id" {
				continue
			}
			acc, ok := v.(bson.M)
			if !ok {
				return nil, fmt.Errorf("$group accumulator %q must be a document", outField)
			}
			if avgArg, ok := acc["$avg"]; ok {
				path, ok := avgArg.(string)
				if !ok || !strings.HasPrefix(path, "$") || len(path) < 2 {
					return nil, fmt.Errorf("$avg must reference a field path")
				}
				path = strings.TrimPrefix(path, "$")
				if !isSafePath(path) {
					return nil, fmt.Errorf("invalid $avg path: %s", path)
				}
				expr := fmt.Sprintf("AVG(CAST(%s AS DOUBLE PRECISION))", dataAccessor(path))
				selectParts = append(selectParts, fmt.Sprintf("%s AS %s", expr, outField))
				outFields = append(outFields, outField)
				continue
			}
			if sumArg, ok := acc["$sum"]; ok {
				if isNumericOne(sumArg) {
					selectParts = append(selectParts, fmt.Sprintf("COUNT(*) AS %s", outField))
					outFields = append(outFields, outField)
					continue
				}

				path, ok := sumArg.(string)
				if !ok || !strings.HasPrefix(path, "$") || len(path) < 2 {
					return nil, fmt.Errorf("$sum must be 1 or a field path")
				}
				path = strings.TrimPrefix(path, "$")
				if !isSafePath(path) {
					return nil, fmt.Errorf("invalid $sum path: %s", path)
				}
				accExpr := dataAccessor(path)
				expr := fmt.Sprintf("SUM(COALESCE(CAST(%s AS DOUBLE PRECISION), 0))", accExpr)
				selectParts = append(selectParts, fmt.Sprintf("%s AS %s", expr, outField))
				outFields = append(outFields, outField)
				continue
			}
			if _, ok := acc["$addToSet"]; ok {
				return nil, fmt.Errorf("$addToSet is not supported for SQL pushdown")
			}
			return nil, fmt.Errorf("unsupported $group accumulator: %v", acc)
		}

		groupOrHaving := ""
		if groupByExpr != "" {
			groupOrHaving = " GROUP BY " + groupByExpr
		} else {
			groupOrHaving = havingClause
		}

		query = fmt.Sprintf(
			"SELECT %s FROM %s%s%s",
			strings.Join(selectParts, ", "),
			table,
			whereClause,
			groupOrHaving,
		)

		groupPushed = true
		consumed++
		rest = rest[1:]
	}

	// Optional $sort/$limit prefix (stops at first unsupported).
	for len(rest) > 0 {
		stage := rest[0]
		switch {
		case stage["$sort"] != nil:
			s, ok := stage["$sort"].(bson.M)
			if !ok {
				return nil, fmt.Errorf("$sort stage must be a document")
			}
			var sortParts []string
			for field, dir := range s {
				if groupPushed {
					allowed := field == "_id"
					for _, f := range outFields {
						if f == field {
							allowed = true
							break
						}
					}
					if !allowed {
						return &AggregatePrefixPlan{Plan: &AggregatePlan{SQL: query, Args: args, Fields: outFields}, ConsumedStages: consumed}, nil
					}
					direction := "ASC"
					if d, ok := dir.(int); ok && d < 0 {
						direction = "DESC"
					} else if d, ok := dir.(int32); ok && d < 0 {
						direction = "DESC"
					} else if d, ok := dir.(int64); ok && d < 0 {
						direction = "DESC"
					}
					sortParts = append(sortParts, fmt.Sprintf("%s %s", field, direction))
					continue
				}

				if !isSafePath(field) {
					return nil, fmt.Errorf("invalid sort field name: %s", field)
				}
				direction := "ASC"
				if d, ok := dir.(int); ok && d < 0 {
					direction = "DESC"
				} else if d, ok := dir.(int32); ok && d < 0 {
					direction = "DESC"
				} else if d, ok := dir.(int64); ok && d < 0 {
					direction = "DESC"
				}
				sortParts = append(sortParts, fmt.Sprintf("%s %s", dataAccessor(field), direction))
			}
			query = fmt.Sprintf("%s ORDER BY %s", query, strings.Join(sortParts, ", "))
			consumed++
			rest = rest[1:]
		case stage["$limit"] != nil:
			limit, ok := stage["$limit"].(int)
			if !ok {
				if i32, ok := stage["$limit"].(int32); ok {
					limit = int(i32)
				} else if i64, ok := stage["$limit"].(int64); ok {
					limit = int(i64)
				} else {
					return nil, fmt.Errorf("$limit stage must be an integer")
				}
			}
			query = fmt.Sprintf("%s LIMIT %d", query, limit)
			consumed++
			rest = rest[1:]
		default:
			// stop pushdown
			rest = rest
			goto done
		}
	}

done:
	if consumed == 0 {
		return nil, nil
	}
	if groupPushed {
		return &AggregatePrefixPlan{Plan: &AggregatePlan{SQL: query, Args: args, Fields: outFields}, ConsumedStages: consumed}, nil
	}
	return &AggregatePrefixPlan{Plan: &AggregatePlan{SQL: query, Args: args, Fields: []string{"data"}}, ConsumedStages: consumed}, nil
}

// tableName is a helper used by the adapter.
func tableName(collection string) string {
	return "doc." + collection
}

// isSafeIdentifier is a helper used by the adapter.
func isSafeIdentifier(name string) bool {
	return isSafeIdentifierSpan(name, 0, len(name))
}

// isSafePath is a helper used by the adapter.
func isSafePath(path string) bool {
	if path == "" {
		return false
	}
	segStart := 0
	for i := 0; i <= len(path); i++ {
		if i == len(path) || path[i] == '.' {
			if i == segStart {
				return false
			}
			if !isSafeIdentifierSpan(path, segStart, i) {
				return false
			}
			segStart = i + 1
		}
	}
	return true
}

// dataAccessor is a helper used by the adapter.
func dataAccessor(path string) string {
	// Expected pattern: "data['a']['b']..." for each '.'-separated path segment.
	// This is used in hot paths, so avoid strings.Split + repeated concatenation.
	var b strings.Builder
	b.WriteString("data")

	// Best-effort capacity guess: base + 6 bytes overhead per segment + segment bytes.
	// overhead per segment is "['" + "']" (4) plus brackets (2) => actually 4,
	// but keep a small cushion to reduce regrows.
	totalLen := 4 + len(path) + 4
	b.Grow(totalLen)

	segStart := 0
	for i := 0; i <= len(path); i++ {
		if i == len(path) || path[i] == '.' {
			b.WriteString("['")
			b.WriteString(path[segStart:i])
			b.WriteString("']")
			segStart = i + 1
		}
	}
	return b.String()
}

// translateFilterWithArgs is a helper used by the adapter.
func (t *Translator) translateFilterWithArgs(filter bson.M, argCount *int) (string, []interface{}, error) {
	conditions := make([]string, 0, len(filter))
	args := make([]interface{}, 0, len(filter))

	for k, v := range filter {
		condition, val, err := t.translateCondition(k, v, argCount)
		if err != nil {
			return "", nil, err
		}
		conditions = append(conditions, condition)
		args = append(args, val...)
	}

	return strings.Join(conditions, " AND "), args, nil
}

func isSafeIdentifierSpan(s string, start, end int) bool {
	if start >= end {
		return false
	}
	// Fast ASCII-only check: [A-Za-z0-9_]+
	for i := start; i < end; i++ {
		c := s[i]
		if c == '_' ||
			(c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') {
			continue
		}
		return false
	}
	return true
}
