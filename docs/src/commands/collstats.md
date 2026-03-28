# `collStats` / `dbStats`

## Usage

```javascript
db.runCommand({ collStats: "orders" })
db.runCommand({ dbStats: 1 })
```

## Behavior

Returns best-effort statistics for tooling compatibility.

## How it maps to MonkDB (implementation)

MoG returns “Mongo-ish” stats by combining:

1) **Count via SQL**:

```sql
SELECT COUNT(*) FROM doc.app__orders
```

2) **Approximate sizes** (best-effort):

MoG may load documents and approximate sizes by marshaling the returned documents to BSON and summing byte lengths:

```sql
SELECT data FROM doc.app__orders
```

This is logically consistent for clients even though the backend storage is not BSON.

## Code pointers

- `internal/mongo/handler.go`
