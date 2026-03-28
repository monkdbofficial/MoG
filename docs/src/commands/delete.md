# `delete`

## Usage

```javascript
db.runCommand({
  delete: "orders",
  deletes: [{ q: { status: "cancelled" }, limit: 0 }]
})
```

## Behavior

Deletes documents matching the translated filter.

## Mapping (MongoDB → SQL)

- MoG translates the delete filter into a SQL `WHERE` predicate over `data #> '{path}'` / `data #>> '{path}'`.
- Each delete spec becomes a `DELETE FROM ... WHERE ...` call.

## Worked example (syntax + SQL)

MongoDB syntax:

```javascript
db.runCommand({
  delete: "orders",
  deletes: [{ q: { status: "cancelled" }, limit: 0 }]
})
```

SQL shape MoG emits:

```sql
DELETE FROM doc.app__orders WHERE data #> '{status}' = to_jsonb($1)
```

## Code pointers

- Translator: `internal/translator/translator.go` (`TranslateDelete`)
- Handler: `internal/mongo/handler.go`
