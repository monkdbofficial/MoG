# `count`

## Usage

```javascript
db.runCommand({ count: "orders", query: { status: "paid" } })
```

## Behavior

Returns `{ n: <count>, ok: 1.0 }`. Some drivers may use aggregation for counting; MoG supports both paths.

## Mapping (MongoDB → SQL)

- The `query` document is translated into a SQL `WHERE` predicate over `data #> '{path}'` / `data #>> '{path}'`.
- MoG executes a single-row `COUNT(*)` query.

## Worked example (syntax + SQL)

MongoDB syntax:

```javascript
db.runCommand({ count: "orders", query: { status: "paid" } })
```

SQL shape MoG emits:

```sql
SELECT COUNT(*) FROM doc.app__orders WHERE data #> '{status}' = to_jsonb($1)
```

Parameter example:

- `$1 = "paid"`

## Code pointers

- Translator: `internal/translator/translator.go` (`TranslateCount`)
- Handler: `internal/mongo/handler.go`
