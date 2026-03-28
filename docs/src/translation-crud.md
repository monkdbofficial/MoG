# CRUD translation

This section describes how MoG maps MongoDB-style operations to MonkDB SQL.

## Collections → tables

Mongo collection `db.coll` maps to a physical table in schema `doc`:

```sql
doc.<physical_collection_name>
```

Documents are stored in a single column:

- Always: `id TEXT` (Mongo `_id`)
- Plus: one SQL column per top-level field (created on demand)
- Optional: `data OBJECT(DYNAMIC)` raw document mirror when `MOG_STORE_RAW_MONGO_JSON=1`

## Find / count

Filters are converted into SQL predicates over relational columns when possible. If a filter/sort cannot
be safely translated, MoG falls back to scanning base rows and applying Mongo-ish semantics in Go.

Supported operators in filters (current subset):

- equality (`field: value`)
- `$gt`, `$gte`, `$lt`, `$lte` (numeric casting)
- `$ne`
- `$in`

See `internal/translator/translator.go` (`TranslateFind`, `TranslateCount`).

!!! note "Field/path safety"
    MoG validates field names and dot-paths before embedding them into SQL accessors.
    Paths must be made of letters/digits/underscore segments (see `isSafePath` in `internal/translator/translator.go`).

## Insert

Insert maps to:

```sql
INSERT INTO doc.<table> (id, <cols...>, data)
VALUES ($1, ..., CAST($N AS OBJECT(DYNAMIC))) -- data is optional
```

## Update

Updates are applied by rewriting full documents:

1. Load candidate rows with SQL (and/or in-memory filtering).
2. Apply Mongo-ish update semantics in Go.
3. Write the updated document back by `id`.

## Quick PyMongo examples

```python
col.insert_one({"_id": 1, "user_id": 42, "total": 19.99})
list(col.find({"user_id": 42, "total": {"$gte": 10}}))
col.update_one({"_id": 1}, {"$set": {"status": "paid"}})
col.update_one({"_id": 1}, {"$inc": {"retry_count": 1}})
col.delete_one({"_id": 1})
```

## Delete

Delete maps to:

```sql
DELETE FROM doc.<table> WHERE <translated_filter>
```
