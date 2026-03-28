# `update`

## Usage

```javascript
db.runCommand({
  update: "orders",
  updates: [{
    q: { _id: 1 },
    u: { $set: { status: "shipped" }, $inc: { retry_count: 1 } },
    multi: false,
    upsert: false
  }]
})
```

## Behavior (document rewrite)

MoG executes updates by rewriting full documents:

1. Load matching docs via translated `SELECT`.
2. Apply Mongo-ish update semantics in-process (Go).
3. Write the full document back by `_id`.

This enables dot-path updates and schema-less documents without relying on backend JSON update functions.

## Mapping (MongoDB → SQL)

- MoG translates the update filter into a SQL `SELECT` to find candidate documents.
- Updates are applied in Go (Mongo-ish semantics), then MoG writes back the full document by `_id`.
- This favors correctness/compatibility over a single SQL `UPDATE ... SET data = data || ...` style approach.

## Worked example (syntax + SQL)

MongoDB syntax:

```javascript
db.runCommand({
  update: "orders",
  updates: [{
    q: { _id: 1 },
    u: { $set: { status: "shipped" }, $inc: { retry_count: 1 } },
    multi: false,
    upsert: false
  }]
})
```

SQL shapes MoG uses:

Select matches:

```sql
SELECT data FROM doc.app__orders WHERE data['_id'] = $1
```

Write updated docs (per matched `_id`):

```sql
UPDATE doc.app__orders
SET data = CAST($1 AS OBJECT)
WHERE data['_id'] = $2
```

Upsert (when `upsert: true` and nothing matches):

```sql
INSERT INTO doc.app__orders (data) VALUES (CAST($1 AS OBJECT))
```

## Code pointers

- Command handler: `internal/mongo/handler.go`
- Update execution: `internal/mongo/handler_update.go`
- Update semantics: `internal/mongo/update_engine.go`
