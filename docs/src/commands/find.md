# `find`

## Usage

### mongosh (`db.runCommand`)

```javascript
db.runCommand({
  find: "orders",
  filter: { user_id: 42, total: { $gte: 10 } },
  sort: { total: -1 },
  limit: 10,
  skip: 0
})
```

### PyMongo

```python
list(col.find({"user_id": 42, "total": {"$gte": 10}}).sort("total", -1).limit(10))
```

## Behavior

MoG translates a subset of filters into SQL.

!!! note "Fallback to in-memory"
    If MoG cannot translate your filter/sort into SQL (unsupported operators or invalid field paths),
    it falls back to fetching base documents and applies filtering/sorting/pagination in Go.

## Mapping (MongoDB → SQL)

- MoG stores each collection in MonkDB as `doc.<physical>` with `id` plus inferred relational columns (and optional `data`).
- Physical table naming: `physical = <db> + "__" + <collection>` (example: `app.orders` → `doc.app__orders`).
- Field filters become SQL `WHERE` predicates over columns with **parameter placeholders** (`$1`, `$2`, …) when possible.
- `sort`, `skip`, and `limit` are pushed down as `ORDER BY`, `OFFSET`, and `LIMIT` when possible.
- `$in` is evaluated in-memory to preserve MongoDB array semantics (matching array fields when any element is in the `$in` list).
- When MoG chooses the in-memory path, it fetches base documents with a broad `SELECT`, then applies filtering/sorting/pagination in Go (`applyMatch`, `applySort`, slice skip/limit).

## Worked example (syntax + SQL)

MongoDB syntax:

```javascript
db.runCommand({
  find: "orders",
  filter: { user_id: 42, total: { $gte: 10 } }
})
```

SQL shape MoG emits (pushdown case):

```sql
SELECT * FROM doc.app__orders
WHERE user_id = $1
  AND total >= $2
```

Parameter example:

- `$1 = 42`
- `$2 = 10`

## Code pointers

- Handler: `internal/mongo/handler.go` (search for `useInMemory`)
- Translator: `internal/translator/translator.go` (`TranslateFindWithOptions`)
