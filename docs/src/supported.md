# Supported features

This list reflects what is currently implemented in the codebase (not MongoDB-complete).

## Commands

Implemented command surface includes:

- `ping`
- `hello` / `isMaster`
- `buildInfo`, `getParameter`, `featureCompatibilityVersion`
- `connectionStatus`, `hostInfo`, `serverStatus`, `getCmdLineOpts`
- `listDatabases`, `listCollections`
- `create`, `drop`, `dropDatabase`
- `collStats`, `dbStats`
- `listIndexes`, `createIndexes`, `dropIndexes`
- `getLog` (only `startupWarnings`)
- `find`, `count`, `insert`, `update`, `delete`
- `aggregate`
- Transactions (compatibility only): `startTransaction`, `commitTransaction`, `abortTransaction`
- Authentication: `saslStart`, `saslContinue` (SCRAM-SHA-256)

Code pointer: `internal/mongo/handler.go`.

!!! tip "In-memory behavior you should know about"
    `find` is usually pushed down to SQL (filter + sort + skip/limit).
    MoG falls back to in-memory evaluation only when it cannot translate your filter/sort into SQL.

## Query operators (SQL translator)

Filter operators supported by the SQL translator:

- Equality (`field: value`)
- `$gt`, `$gte`, `$lt`, `$lte` (numeric cast)
- `$ne`
- `$in` (note: evaluated in-memory by `find` for array semantics)

Code pointer: `internal/translator/translator.go` (`translateCondition`).

## Update operators (SQL translator)

- `$set`
- `$inc`
- Replacement document treated as `$set` (compat shortcut)

Code pointer: `internal/translator/translator.go` (`TranslateUpdate`).

## Aggregation stages (in-memory pipeline)

The aggregate command is **hybrid**:

- Pushes down the longest supported prefix into SQL (leading `$match`, then optional `$group`, `$sort`, `$limit`, or `$count`)
- Evaluates remaining stages in Go using the in-memory pipeline engine

Stages implemented in the in-memory pipeline:

- `$match`
- `$project`
- `$addFields` and `$set`
- `$unset`
- `$lookup`
- `$unwind`
- `$group`
- `$count`
- `$sort`
- `$limit`
- `$sample`

Code pointer: `internal/mongo/pipeline.go`.

## `$addFields` computed expressions

Supported expressions in `$addFields` / `$set`:

- field reference (e.g. `"$a.b"`)
- `$size`
- `$isArray`
- `$cond` (array form and object form)

Code pointer: `internal/mongo/pipeline.go` (`evalAddFieldsValue`).
