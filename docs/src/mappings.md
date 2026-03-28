# MongoDB → MonkDB mappings

This page explains how MoG maps MongoDB concepts to MonkDB tables/queries.

## Collections → tables

Mongo `(db, collection)` maps to a physical MonkDB table under schema `doc`:

- Table: `doc.<physical_name>`
- Column: `id TEXT` (Mongo `_id`)
- Columns: top-level fields become SQL columns (created on demand)
- Optional raw mirror: `data OBJECT(DYNAMIC)` when `MOG_STORE_RAW_MONGO_JSON=1`

MoG also maintains a small internal catalog collection:

- `doc.__monkdb_catalog`

## Filters → SQL

For `find`, `count`, and the leading `$match` in `aggregate`, MoG translates filters into SQL predicates over relational columns when it can:

```sql
user_id = $1
total >= $2
```

Supported operators:

- `=`
- `$gt`, `$gte`, `$lt`, `$lte`
- `$ne`
- `$in`

!!! note "Fallback to in-memory"
    MoG pushes `$in`, `sort`, `skip`, and `limit` down when possible. It falls back to fetching base documents and applying logic in Go only when it cannot translate your filter/sort into SQL.

## Inserts → SQL

Documents are inserted with `id` plus inferred typed columns. When enabled, MoG also mirrors the full raw document into `data`:

```sql
INSERT INTO doc.<table> (id, name, age, data)
VALUES ($1, $2, $3, CAST($4 AS OBJECT(DYNAMIC)))
```

## Updates → SQL / engine

Two main paths exist:

1. `internal/translator` handles common `$set` and `$inc` patterns by generating SQL.
2. `internal/mongo` contains additional “Mongo-ish” update logic for wire-protocol operations.

## Aggregation → hybrid execution

MoG uses a hybrid strategy:

- Push down the longest supported prefix into SQL (leading `$match`, then optional `$group`, `$sort`, `$limit`, or `$count`).
- Execute any remaining pipeline stages in Go (`internal/mongo/pipeline.go`).

This avoids backend SQL dialect mismatches for computed fields and preserves “Mongo-ish” behavior.

### Example: leading `$match` pushdown

If the pipeline begins with supported stages, MoG will include them in the SQL query:

```python
pipeline = [
  {"$match": {"status": "paid"}},
  {"$group": {"_id": "$user_id", "n": {"$sum": 1}}},
]
```

This reduces the base documents that need to be pulled into memory before grouping.
