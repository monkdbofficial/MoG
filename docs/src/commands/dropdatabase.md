# `dropDatabase`

## Usage

```javascript
db.runCommand({ dropDatabase: 1 })
```

## Behavior

- Drops tables for collections in the database (as known to MoG’s catalog).
- Removes catalog entries.

## How it maps to MonkDB (implementation)

MoG uses its catalog to discover collections, then issues a best-effort `DROP TABLE` per collection table.

Example (MongoDB syntax):

```javascript
db.runCommand({ dropDatabase: 1, $db: "app" })
```

Example DDL shapes:

```sql
DROP TABLE IF EXISTS doc.app__orders
```

```sql
DROP TABLE IF EXISTS doc.app__items
```

## Code pointers

- `internal/mongo/handler.go`
