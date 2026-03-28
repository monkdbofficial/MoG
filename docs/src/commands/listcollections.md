# `listCollections`

## Usage

```javascript
db.runCommand({ listCollections: 1 })
```

## Behavior

MoG returns collections for a database using its internal catalog, and merges in collection names derived from existing backend tables.

## How it maps to MonkDB (implementation)

1) **Read catalog entries for a DB**:

MongoDB syntax:

```javascript
db.runCommand({ listCollections: 1, $db: "app" })
```

MonkDB query shape:

```sql
SELECT coll FROM doc.__monkdb_catalog WHERE db = $1
```

Parameter example:

- `$1 = "app"`

MoG scans those catalog rows and returns `coll`.

2) **Merge in collections from backend tables** (best-effort):

MoG lists `doc` schema tables, then keeps those prefixed with:

```text
app__
```

Example: `doc.app__orders` → collection `orders`.

## Code pointers

- `internal/mongo/handler.go` (catalog list)
