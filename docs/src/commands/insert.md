# `insert`

## Usage

```javascript
db.runCommand({
  insert: "orders",
  documents: [{ _id: 1, user_id: 42, status: "paid", total: 19.99 }]
})
```

## Behavior

- Normalizes documents before storage.
- If the backing table doesn’t exist yet, MoG creates it and retries.

## Mapping (MongoDB → SQL)

- Documents are stored as a single `data` object value in MonkDB (`OBJECT(DYNAMIC)`).
- MoG inserts documents using a parameter cast: `CAST($1 AS OBJECT)`.

## Worked example (syntax + SQL)

MongoDB syntax:

```javascript
db.runCommand({
  insert: "orders",
  documents: [{ _id: 1, user_id: 42, status: "paid", total: 19.99 }]
})
```

SQL shape MoG emits:

```sql
INSERT INTO doc.app__orders (data) VALUES (CAST($1 AS OBJECT))
```

Parameter example:

- `$1` is the document serialized as an object payload (MoG marshals the BSON into an object representation acceptable to MonkDB).

## Code pointers

- Handler: `internal/mongo/handler.go`
- SQL template: `internal/translator/translator.go` (`TranslateInsert`)
