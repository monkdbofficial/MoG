package mongo

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/mgo.v2/bson"

	mpipeline "mog/internal/mongo/pipeline"
)

type relationalAggPlan struct {
	SQL            string
	Args           []interface{}
	Fields         []string
	ConsumedStages int
}

// buildRelationalAggPrefixPlan attempts to translate the longest safe prefix of an aggregation pipeline
// into SQL over relational columns.
//
// Supported prefix:
// - one or more leading $match stages (merged; relationalWhere subset)
// - optional $group with _id: "$<field>" and $sum: 1 (COUNT)
// - optional $sort
// - optional $limit
//
// Any remaining stages should be evaluated in-memory with the pipeline engine.
func buildRelationalAggPrefixPlan(physical string, pipeline []bson.M) (*relationalAggPlan, bool, error) {
	if physical == "" || len(pipeline) == 0 {
		return nil, false, nil
	}

	consumed := 0
	rest := pipeline

	// Leading $match stages.
	mergedMatch := bson.M{}
	for len(rest) > 0 {
		m, ok := rest[0]["$match"].(bson.M)
		if !ok {
			break
		}
		for k, v := range m {
			mergedMatch[k] = v
		}
		consumed++
		rest = rest[1:]
	}

	whereSQL := ""
	args := []interface{}(nil)
	if len(mergedMatch) > 0 {
		where, ok, err := buildRelationalWhere(mergedMatch)
		if err != nil {
			return nil, false, err
		}
		if !ok || where == nil {
			return nil, false, nil
		}
		if where.SQL != "" {
			whereSQL = " WHERE " + where.SQL
			args = append(args, where.Args...)
		}
	}

	// Optional $group.
	fields := []string{"data"}
	query := "SELECT data FROM doc." + physical + whereSQL
	if len(rest) > 0 && rest[0]["$group"] != nil {
		groupSpec, ok := rest[0]["$group"].(bson.M)
		if !ok {
			return nil, false, fmt.Errorf("$group stage must be a document")
		}
		rawID, ok := groupSpec["_id"]
		if !ok {
			return nil, false, fmt.Errorf("$group requires _id")
		}
		idPath, ok := rawID.(string)
		if !ok || !strings.HasPrefix(idPath, "$") || len(idPath) < 2 {
			return nil, false, nil
		}
		idPath = strings.TrimPrefix(idPath, "$")
		if strings.Contains(idPath, ".") {
			return nil, false, nil
		}
		idCol := sqlColumnNameForField(idPath)
		if idCol == "" || idCol == "id" || idCol == "data" {
			return nil, false, nil
		}

		selectParts := []string{fmt.Sprintf("%s AS _id", idCol)}
		fields = []string{"_id"}

		// Only support $sum: 1 (COUNT) and $sum: "$field" (SUM numeric).
		outFields := make([]string, 0, len(groupSpec))
		for k := range groupSpec {
			if k != "_id" {
				outFields = append(outFields, k)
			}
		}
		sort.Strings(outFields)
		for _, outField := range outFields {
			acc, ok := groupSpec[outField].(bson.M)
			if !ok {
				return nil, false, nil
			}
			if sumArg, ok := acc["$sum"]; ok {
				if mpipeline.IsNumericOne(sumArg) {
					selectParts = append(selectParts, fmt.Sprintf("COUNT(*) AS %s", outField))
					fields = append(fields, outField)
					continue
				}
				p, ok := sumArg.(string)
				if !ok || !strings.HasPrefix(p, "$") || len(p) < 2 {
					return nil, false, nil
				}
				p = strings.TrimPrefix(p, "$")
				if strings.Contains(p, ".") {
					return nil, false, nil
				}
				c := sqlColumnNameForField(p)
				if c == "" || c == "id" || c == "data" {
					return nil, false, nil
				}
				selectParts = append(selectParts, fmt.Sprintf("SUM(COALESCE(CAST(%s AS DOUBLE PRECISION), 0)) AS %s", c, outField))
				fields = append(fields, outField)
				continue
			}
			return nil, false, nil
		}

		query = fmt.Sprintf(
			"SELECT %s FROM doc.%s%s GROUP BY %s",
			strings.Join(selectParts, ", "),
			physical,
			whereSQL,
			idCol,
		)
		consumed++
		rest = rest[1:]
	}

	// Optional $sort / $limit.
	for len(rest) > 0 {
		stage := rest[0]
		switch {
		case stage["$sort"] != nil:
			spec, ok := stage["$sort"].(bson.M)
			if !ok {
				return nil, false, fmt.Errorf("$sort stage must be a document")
			}
			keys := make([]string, 0, len(spec))
			for k := range spec {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var parts []string
			for _, k := range keys {
				dir := 1
				switch v := spec[k].(type) {
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
				}
				order := "ASC"
				if dir < 0 {
					order = "DESC"
				}
				// Allow ordering by computed/group fields and _id.
				parts = append(parts, fmt.Sprintf("%s %s", k, order))
			}
			if len(parts) > 0 {
				query += " ORDER BY " + strings.Join(parts, ", ")
			}
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
					return nil, false, fmt.Errorf("$limit stage must be an integer")
				}
			}
			if limit > 0 {
				query += fmt.Sprintf(" LIMIT %d", limit)
			}
			consumed++
			rest = rest[1:]
		default:
			goto done
		}
	}

done:
	if consumed == 0 {
		return nil, false, nil
	}
	return &relationalAggPlan{SQL: query, Args: args, Fields: fields, ConsumedStages: consumed}, true, nil
}
