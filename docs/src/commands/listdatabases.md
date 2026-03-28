# `listDatabases`

## Usage

```javascript
db.adminCommand({ listDatabases: 1 })
```

## Behavior

MoG returns databases using a small internal catalog, then (best-effort) merges in database names derived from existing MonkDB tables.

## How it maps to MonkDB (implementation)

1) **Read the catalog table** (documents):

```sql
SELECT db FROM doc.__monkdb_catalog
```

MoG scans the returned rows and collects unique `db` values.

2) **Merge in DB names from the backend schema** (best-effort):

MoG lists `doc` schema tables via information schema queries like:

```sql
SELECT table_name FROM information_schema.tables WHERE table_schema = 'doc'
```

Then it derives `db` by splitting table names of the form:

```text
<db>__<collection>
```

Example: `doc.app__orders` → database `app`.

## Code pointers

- `internal/mongo/handler.go` (catalog list)
