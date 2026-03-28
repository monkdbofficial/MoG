# `create`

## Usage

```javascript
db.runCommand({ create: "orders" })
```

## Behavior

Creates a backing MonkDB table if it doesn’t exist.

## Worked example (MongoDB syntax + DDL)

MongoDB syntax:

```javascript
db.runCommand({ create: "orders" })
```

MonkDB DDL shape MoG emits:

```sql
CREATE TABLE IF NOT EXISTS doc.<physical> (id TEXT PRIMARY KEY)
-- plus (optional) ALTER TABLE ... ADD COLUMN data OBJECT(DYNAMIC) when MOG_STORE_RAW_MONGO_JSON=1
```

## Code pointers

- `internal/mongo/handler.go` (ensure collection table)
