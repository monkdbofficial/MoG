# Limitations (detailed)

MoG is a pragmatic compatibility layer, not a full MongoDB server. This page calls out the common gaps you’ll run into while developing.

## Command surface is a subset

MoG only implements what exists in `internal/mongo/handler.go`.

Examples of “real MongoDB” behavior you should not assume:

- Full admin command suite
- Full index management semantics
- All wire protocol variants

See [Supported](supported.md) for the current list.

## Query operators are a subset

The SQL translator supports a limited set of filter operators (`internal/translator/translator.go`):

- equality
- `$gt`, `$gte`, `$lt`, `$lte`
- `$ne`
- `$in`

Notably missing (non-exhaustive):

- `$or`, `$and`, `$not`
- `$exists`, `$type`
- regex
- geospatial operators

## Updates are partial and sometimes rewritten

MoG supports a limited set of update modifiers and favors compatibility:

- Common `$set` / `$inc` are supported.
- Many update forms are applied by loading documents, applying update logic in Go, and writing full documents back by `_id` (see `internal/mongo/handler_update.go`).

Notably missing (non-exhaustive):

- array filters and positional operators
- complex modifiers (`$push`, `$pull`, `$addToSet`, etc) unless implemented in the update engine

## Aggregation support is incremental

Aggregation is hybrid:

- pushes down the longest supported prefix into SQL (leading `$match`, then optional `$group`, `$sort`, `$limit`, or `$count`)
- then evaluates remaining stages in Go

Supported stages are listed in [Supported](supported.md) and documented per-stage under the [Aggregation](aggregation/index.md) section.

Computed expressions inside `$addFields` / `$set` are incremental. If you see:

> `unsupported $addFields expression: ...`

you’ll need to implement the expression in `internal/mongo/pipeline.go` and add tests.

## Authentication is development-grade

SCRAM-SHA-256 is implemented, but:

- credentials are stored in-memory
- the salt is fixed

See `internal/mongo/auth.go`.

## Performance and correctness tradeoffs

MoG deliberately chooses correctness/compatibility over pushing everything into SQL:

- `find` falls back to in-memory evaluation only when it cannot translate your filter/sort into SQL
- `aggregate` pushes down a supported prefix, but complex stages/expressions still run in-memory
- `update` often rewrites whole documents

For large collections, this can increase memory use and latency.

## Transaction semantics are limited

MoG accepts transaction commands for driver compatibility, but MonkDB does not provide MongoDB-style transaction guarantees in this setup. Treat transaction usage as best-effort and validate with your workload.
