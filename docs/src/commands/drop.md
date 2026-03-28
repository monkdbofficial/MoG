# `drop`

## Usage

```javascript
db.runCommand({ drop: "orders" })
```

## Behavior

Drops the backing table for the collection.

## Worked example (MongoDB syntax + DDL)

MongoDB syntax:

```javascript
db.runCommand({ drop: "orders" })
```

MonkDB DDL shape MoG emits:

```sql
DROP TABLE IF EXISTS doc.<physical>
```

## Code pointers

- `internal/mongo/handler.go`
