# `listIndexes` / `createIndexes` / `dropIndexes`

## Usage

```javascript
db.runCommand({ listIndexes: "orders" })
db.runCommand({ createIndexes: "orders", indexes: [{ name: "x", key: { x: 1 } }] })
db.runCommand({ dropIndexes: "orders", index: "*" })
```

## Behavior

- `listIndexes` returns a minimal `_id_` index document for client/tooling compatibility.
- `createIndexes` / `dropIndexes` are currently **accepted for compatibility** and return success responses, but MoG does **not** create or drop physical indexes in MonkDB today.

## Mapping (MongoDB → MonkDB)

Current state:

- MoG stores documents in `doc.<physical>` with `id TEXT` plus inferred relational columns.
- If `MOG_STORE_RAW_MONGO_JSON=1`, MoG also mirrors the raw document in `data OBJECT(DYNAMIC)` (optional).
- MoG does not emit DDL to create secondary indexes in MonkDB.
- MonkDB may create and use internal indexes/structures depending on its own engine behavior and configuration, but MoG does not manage them via MongoDB index commands.

If you need index-level performance, manage it directly in MonkDB (outside of MoG), or extend MoG to translate `createIndexes`/`dropIndexes` into the appropriate MonkDB DDL once the indexing strategy is defined.

MoG does not currently emit any MonkDB index DDL for these commands (no-op).

## Code pointers

- `internal/mongo/handler.go`
