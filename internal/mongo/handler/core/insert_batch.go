package mongo

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"go.uber.org/zap"
	"gopkg.in/mgo.v2/bson"

	"mog/internal/logging"
	"mog/internal/mongo/handler/shared"
	mpipeline "mog/internal/mongo/pipeline"
)

type insertPreparedDoc struct {
	doc   bson.M
	docID string
}

func (h *Handler) insertMany(ctx context.Context, physical string, rawDocs []interface{}) (seen int, inserted int64, err error) {
	if h.pool == nil {
		return 0, 0, fmt.Errorf("database pool is not configured")
	}
	if physical == "" {
		return 0, 0, fmt.Errorf("empty collection")
	}

	docs := make([]insertPreparedDoc, 0, len(rawDocs))
	for _, d := range rawDocs {
		seen++
		doc, ok := shared.CoerceBsonM(d)
		if !ok {
			continue
		}
		normalizeDocForStorage(doc)
		docID, err := encodeDocID(doc["_id"])
		if err != nil {
			return seen, inserted, err
		}
		if err := h.offloadBlobsInDoc(ctx, h.db(), physical, docID, doc); err != nil {
			return seen, inserted, err
		}
		docs = append(docs, insertPreparedDoc{doc: doc, docID: docID})
	}
	if len(docs) == 0 && seen > 0 {
		return seen, inserted, fmt.Errorf("no documents could be decoded for insert")
	}
	if len(docs) == 0 {
		return seen, inserted, nil
	}

	// Fast path: if no unique indexes exist, skip expensive duplicate scanning.
	hasUnique := len(h.listUniqueIndexes(physical)) > 0

	// Infer/ensure schema once for the whole batch.
	colTypes := map[string]string{}
	for _, pd := range docs {
		for k, v := range pd.doc {
			if k == "" || k == "_id" {
				continue
			}
			col := shared.SQLColumnNameForField(k)
			if col == "" || col == "id" || col == "data" {
				continue
			}
			sqlType := sqlTypeForField(k, v)
			if sqlType == "" {
				continue
			}
			if prev, ok := colTypes[col]; ok && prev != sqlType {
				// If types disagree across docs, fall back to TEXT (permissive).
				colTypes[col] = "TEXT"
				continue
			}
			colTypes[col] = sqlType
		}
	}

	cols := make([]string, 0, len(colTypes))
	for c := range colTypes {
		cols = append(cols, c)
	}
	sort.Strings(cols)

	// Ensure table exists and columns exist. Use a local cache for table/column existence
	// to avoid repeated DDL checks even if they are "IF NOT EXISTS".
	if err := h.ensureCollectionTableWithColumnsExec(ctx, h.db(), physical, colTypes); err != nil {
		return seen, inserted, err
	}

	// Keep raw `data` in sync whenever the table has a `data` column, even if
	// MOG_STORE_RAW_MONGO_JSON was toggled between runs.
	storeRaw := h.storeRawMongoJSON || h.schemaCache().hasColumn(physical, "data")

	insertCols := []string{"id"}
	if storeRaw {
		insertCols = append(insertCols, "data")
	}
	insertCols = append(insertCols, cols...)

	for _, c := range cols {
		if err := h.ensureColumn(ctx, physical, c, colTypes[c]); err != nil {
			return seen, inserted, err
		}
	}

	// SQL generation buffer to reduce allocations
	var sqlBuf strings.Builder
	sqlBuf.Grow(16384)

	// Chunk to avoid extremely large statements.
	const maxRowsPerStmt = 500
	for start := 0; start < len(docs); start += maxRowsPerStmt {
		end := start + maxRowsPerStmt
		if end > len(docs) {
			end = len(docs)
		}

		numRows := end - start
		args := make([]interface{}, 0, numRows*len(insertCols))
		rowExprs := make([]string, 0, numRows)
		argN := 0

		for _, pd := range docs[start:end] {
			if hasUnique {
				if dupMsg, dupName, ok := h.checkUniqueViolation(ctx, physical, pd.docID, pd.doc, ""); ok {
					return seen, inserted, fmt.Errorf("E11000 duplicate key error collection: %s index: %s dup key: %s", physical, dupName, dupMsg)
				}
			}

			valExprs := make([]string, 0, len(insertCols))
			for _, c := range insertCols {
				switch c {
				case "id":
					argN++
					args = append(args, pd.docID)
					valExprs = append(valExprs, fmt.Sprintf("$%d", argN))
				case "data":
					docJSON, err := shared.MarshalObject(pd.doc)
					if err != nil {
						return seen, inserted, err
					}
					argN++
					args = append(args, docJSON)
					valExprs = append(valExprs, fmt.Sprintf("CAST($%d AS OBJECT(DYNAMIC))", argN))
				default:
					field := shared.MongoFieldNameForColumn(c)
					v, exists := pd.doc[field]
					if !exists || v == nil {
						valExprs = append(valExprs, "NULL")
						continue
					}
					sqlType := colTypes[c]
					switch {
					case sqlType == "OBJECT(DYNAMIC)":
						js, err := shared.MarshalObject(v)
						if err != nil {
							return seen, inserted, err
						}
						argN++
						args = append(args, js)
						valExprs = append(valExprs, fmt.Sprintf("CAST($%d AS OBJECT(DYNAMIC))", argN))
					case strings.HasPrefix(sqlType, "ARRAY("):
						av, err := arrayArgForSQLType(v, sqlType)
						if err != nil {
							return seen, inserted, err
						}
						argN++
						args = append(args, av)
						valExprs = append(valExprs, fmt.Sprintf("CAST($%d AS %s)", argN, sqlType))
					case strings.HasPrefix(sqlType, "FLOAT_VECTOR("):
						lit, _, ok := floatVectorLiteral(v)
						if !ok {
							return seen, inserted, fmt.Errorf("invalid FLOAT_VECTOR value for field %q", field)
						}
						valExprs = append(valExprs, lit)
					case sqlType == "DOUBLE PRECISION":
						if f, ok := mpipeline.ToFloat64Match(v); ok {
							v = f
						}
						argN++
						args = append(args, v)
						valExprs = append(valExprs, fmt.Sprintf("$%d", argN))
					case sqlType == "TEXT":
						// Arrays/objects are stored as JSON-encoded text so they can be rehydrated on reads.
						if _, ok := shared.CoerceInterfaceSlice(v); ok {
							js, err := shared.MarshalObject(v)
							if err != nil {
								return seen, inserted, err
							}
							v = js
						} else if _, ok := shared.CoerceBsonM(v); ok {
							js, err := shared.MarshalObject(v)
							if err != nil {
								return seen, inserted, err
							}
							v = js
						}
						argN++
						args = append(args, v)
						valExprs = append(valExprs, fmt.Sprintf("$%d", argN))
					default:
						argN++
						args = append(args, v)
						valExprs = append(valExprs, fmt.Sprintf("$%d", argN))
					}
				}
			}
			rowExprs = append(rowExprs, "("+strings.Join(valExprs, ", ")+")")
		}

		sqlBuf.Reset()
		sqlBuf.WriteString("INSERT INTO doc.")
		sqlBuf.WriteString(physical)
		sqlBuf.WriteString(" (")
		sqlBuf.WriteString(strings.Join(insertCols, ", "))
		sqlBuf.WriteString(") VALUES ")
		sqlBuf.WriteString(strings.Join(rowExprs, ", "))

		tag, err := h.db().Exec(ctx, sqlBuf.String(), args...)
		if err != nil {
			return seen, inserted, err
		}
		inserted += tag.RowsAffected()
	}
	if logging.Logger() != nil {
		logging.Logger().Debug("insert batch",
			zap.String("physical", physical),
			zap.Int("docs_seen", seen),
			zap.Int("docs_decoded", len(docs)),
			zap.Int64("rows_affected", inserted),
		)
	}

	return seen, inserted, nil
}
