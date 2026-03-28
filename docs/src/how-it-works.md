# How it works (A → Z)

This page walks through the full request lifecycle, from a MongoDB client connecting to MoG, to SQL executed against MonkDB, to the response being encoded back into Mongo wire protocol.

## 1) Client connects (TCP)

- MoG listens on `MOG_MONGO_PORT` (default `27017`).
- Each connection is handled independently.

Code pointers:

- TCP accept loop: `internal/server/server.go`
- Per-connection handler: `internal/server/server.go` → `internal/mongo/handler.go`

## 2) Wire protocol decode

MoG supports modern Mongo wire protocol messages via `OP_MSG` and implements the minimal framing needed to:

- read headers
- decode command documents (BSON)
- route commands to handler methods

Code pointers:

- Protocol framing: `internal/mongo/protocol.go`

## 3) Authentication (SCRAM-SHA-256)

For clients that authenticate:

- `saslStart` begins the SCRAM exchange
- `saslContinue` completes it

Once done, MoG marks the connection authenticated and enables a more “Mongo-like” command surface.

Code pointers:

- SCRAM: `internal/mongo/auth.go`
- Command routing: `internal/mongo/handler.go` (look for `saslStart` / `saslContinue`)

## 4) Command routing

Incoming `OP_MSG` commands are examined and dispatched (examples):

- `find`, `insert`, `update`, `delete`
- `count` (some clients implement count via aggregation)
- `aggregate` (evaluated mostly in-memory; see below)
- collection bookkeeping (create/drop) mapped to MonkDB tables

Code pointers:

- Main command switch: `internal/mongo/handler.go`

## 5) Storage model in MonkDB

MoG stores each MongoDB collection as a MonkDB table:

- schema: `doc`
- table name: derived from `(db, collection)` to a physical name
- always-present column: `id TEXT` (Mongo `_id`, stored as a JSON-encoded string)
- additional columns: one SQL column per top-level Mongo field (created on demand)
- optional raw mirror: `data OBJECT(DYNAMIC)` (enabled with `MOG_STORE_RAW_MONGO_JSON=1`)

Example DDL (simplified):

```sql
CREATE TABLE IF NOT EXISTS doc.<physical> (id TEXT PRIMARY KEY);
ALTER TABLE doc.<physical> ADD COLUMN name TEXT;
ALTER TABLE doc.<physical> ADD COLUMN age DOUBLE PRECISION;
ALTER TABLE doc.<physical> ADD COLUMN address OBJECT(DYNAMIC);
ALTER TABLE doc.<physical> ADD COLUMN data OBJECT(DYNAMIC); -- optional
```

Code pointers:

- Table creation: `internal/mongo/handler.go` (search for `CREATE TABLE IF NOT EXISTS doc.`)
- Collection naming: `internal/mongo/handler.go` (physical name helpers)

## 6) Translation to SQL (CRUD)

For CRUD-style operations:

- MoG prefers translating filters/sorts into SQL over relational columns
- inserts become `INSERT ... (id, <cols...>) VALUES (...)` with inferred column types
- updates rewrite full documents (load → apply update semantics in Go → write back by `id`)
- if `MOG_STORE_RAW_MONGO_JSON=1`, MoG also mirrors the full raw document into `data OBJECT(DYNAMIC)` (best-effort)

Code pointers:

- Translator: `internal/translator/translator.go`

### `find` pushdown vs in-memory operations

MoG pushes `find` down to SQL when it can translate your filter and sort:

- filter → `WHERE`
- sort → `ORDER BY`
- skip/limit → `OFFSET` / `LIMIT`

If MoG cannot translate your filter/sort (unsupported operators or invalid field paths), it falls back to fetching base documents with a broad SQL query and applies filtering/sorting/pagination in Go.

Code pointers:

- Find handler logic: `internal/mongo/handler.go` (search for `useInMemory`)

## 7) Aggregation pipelines

Aggregation is handled in two layers:

1. The **longest supported prefix** is pushed down into SQL (leading `$match`, then optional `$group`, `$sort`, `$limit`, or `$count`).
2. Any remaining stages are evaluated **in memory** (Go), to avoid SQL dialect edge cases.

Supported stages include (see [Aggregation](translation-aggregation.md)):

- `$match`, `$project`, `$addFields`/`$set`, `$unset`
- `$group`, `$sort`, `$limit`
- `$lookup`, `$unwind`
- `$sample`, `$count`

Code pointers:

- Aggregate handler: `internal/mongo/handler.go` (search for `aggregate base docs`)
- Pipeline evaluator: `internal/mongo/pipeline.go`

## 8) Encode the response

Once a command completes:

- results are normalized to match Mongo-ish expectations
- response BSON is encoded
- MoG writes an `OP_MSG` response back to the client

Code pointers:

- Response building: `internal/mongo/handler.go` (`newMsg`, `newMsgError`)

## 9) Metrics exporter (Prometheus)

MoG exposes a Prometheus exporter on `MOG_METRICS_PORT` (default `8080`):

- `GET /metrics`

Important: this is **not Prometheus** (no `/api/v1/query`).

Code pointers:

- Metrics server: `internal/server/metrics.go`
