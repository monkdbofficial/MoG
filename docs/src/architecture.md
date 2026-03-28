# Architecture

## Components

1. **Mongo wire server** (`internal/server`, `internal/mongo`)
   - Accepts TCP connections
   - Parses `OP_MSG`
   - Handles Mongo commands
2. **Translator** (`internal/translator`)
   - Converts a subset of Mongo filters and updates into SQL
3. **Backend pool** (`internal/monkdb`)
   - `pgxpool` connections to MonkDB
4. **Aggregation pipeline engine** (`internal/mongo/pipeline.go`)
   - Applies aggregation stages in-memory when they can’t be pushed down to SQL
5. **Observability** (`internal/server/metrics.go`, `prometheus/`, `grafana/`)

## Ports

- Mongo wire protocol: `MOG_MONGO_PORT` (default `27017`)
- Metrics exporter: `MOG_METRICS_PORT` (default `8080`, path `/metrics`)

## Data model (MonkDB)

MoG stores each collection as a MonkDB table:

- Schema: `doc`
- Table: `doc.<physical_collection>`
- Column: `id TEXT` (Mongo `_id`, JSON-encoded string)
- Columns: one SQL column per top-level Mongo field (created on demand)
- Optional raw mirror: `data OBJECT(DYNAMIC)` when `MOG_STORE_RAW_MONGO_JSON=1`

MoG also maintains a small internal catalog table so it can answer `listDatabases`/`listCollections` without scanning MonkDB:

- `doc.__monkdb_catalog`

## Request flow (high-level)

```text
Mongo client
  │  (OP_MSG over TCP)
  ▼
MoG (binary: mog) :27017
  │  decode BSON command
  │  auth (SCRAM) if needed
  │  route command
  │  execute via SQL and/or in-memory pipeline
  ▼
MonkDB (Postgres wire)
  │  returns rows (documents)
  ▼
MoG encodes OP_MSG response
  ▼
Mongo client
```

## Execution paths (important for developers)

### Path A: SQL-first (CRUD)

For many CRUD commands, MoG translates directly to SQL:

- `find` / `count`: translate filters and sorts to SQL when safe (otherwise falls back to in-memory filtering)
- `insert`: inserts into `doc.<physical>` with inferred columns; always writes `id`, and optionally mirrors raw JSON into `data`
- `update`: loads matching docs, applies update semantics in Go, and writes back by `id`
- `delete`: delete by `id` when possible (or by scanning + filtering when not)

Code pointers:

- Command routing: `internal/mongo/handler.go`
- SQL translation: `internal/translator/translator.go`

### Path B: Hybrid (aggregation)

For `aggregate`:

1. The longest supported prefix is pushed down into SQL (leading `$match`, then optional `$group`, `$sort`, `$limit`, or `$count`).
2. Any remaining stages run in Go (`internal/mongo/pipeline.go`).

This is intentionally “developer-friendly”: you can add stage/expression support by extending the Go evaluator and adding tests.

## Architecture diagrams (more detailed)

### Component diagram

```text
                ┌──────────────────────────────────────────────────────────┐
                │                          MoG                             │
                │                    (Go process: `mog`)                   │
                │                                                          │
Mongo clients   │   ┌──────────────┐       ┌──────────────────────────┐    │
(mongosh/       │   │ TCP server   │       │ Mongo wire layer         │    │
drivers)        │   │ :27017       │──────▶│ - OP_MSG decode/encode   │    │
    │           │   └──────────────┘       │ - command routing        │    │
    │           │                           └───────────┬─────────────┘    │
    │           │                                       │                  │
    │           │                                       │ CRUD / metadata  │
    │           │                                       ▼                  │
    │           │                           ┌──────────────────────────┐   │
    │           │                           │ SQL translator           │   │
    │           │                           │ (filters/updates subset) │   │
    │           │                           └───────────┬─────────────┘   │
    │           │                                       │ SQL              │
    │           │                                       ▼                  │
    │           │                           ┌──────────────────────────┐   │
    │           │                           │ MonkDB pool (pgxpool)    │   │
    │           │                           └───────────┬─────────────┘   │
    │           │                                       │                  │
    │           │                   Hybrid aggregate     │                  │
    │           │                   (in-memory)          │                  │
    │           │                     ┌──────────────────▼──────────────┐  │
    │           │                     │ Pipeline evaluator (Go)          │  │
    │           │                     │ $match/$project/$group/...       │  │
    │           │                     └─────────────────────────────────┘  │
    │           │                                                          │
    │           │   ┌──────────────┐                                       │
    └───────────┼──▶│ Metrics HTTP │  GET /metrics                         │
                │   │ :8080        │◀───────────────────────────────┐      │
                │   └──────────────┘                                │      │
                └───────────────────────────────────────────────────┼──────┘
                                                                    │
                                                          Prometheus scrapes
                                                          Grafana queries Prometheus
```

### Sequence: `find` (typical)

```text
Client          MoG                          MonkDB
  │ hello/ping   │                              │
  │─────────────▶│                              │
  │              │ (compat responses)            │
  │ find         │                              │
  │─────────────▶│ translate filter → SQL        │
  │              │ SELECT data FROM doc.<t> ...  │
  │              │─────────────────────────────▶│
  │              │ rows                          │
  │              │◀─────────────────────────────│
  │              │ normalize docs, build cursor  │
  │◀─────────────│                              │
```

### Sequence: `update` (document rewrite)

```text
Client          MoG                                   MonkDB
  │ update        │                                     │
  │──────────────▶│ SELECT matching docs (translated)    │
  │               │────────────────────────────────────▶│
  │               │ docs                                 │
  │               │◀────────────────────────────────────│
  │               │ ApplyUpdate(...) in Go                │
  │               │ UPDATE doc.<t> SET data=... WHERE _id │
  │               │────────────────────────────────────▶│
  │◀──────────────│                                     │
```

### Sequence: `aggregate` (hybrid)

```text
Client          MoG                                      MonkDB
  │ aggregate     │                                        │
  │──────────────▶│ (optional) push down leading $match     │
  │               │ SELECT data FROM doc.<t> WHERE ...      │
  │               │───────────────────────────────────────▶│
  │               │ base docs                               │
  │               │◀───────────────────────────────────────│
  │               │ applyPipeline(...) in Go                │
  │◀──────────────│ firstBatch                              │
```
