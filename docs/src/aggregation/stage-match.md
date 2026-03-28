# Stage: `$match`

## What it does

Filters documents by a predicate.

## MoG behavior

- If `$match` is the **first stage**, MoG pushes it down into SQL to reduce base documents.
- Otherwise, `$match` runs in-memory as part of the pipeline evaluator.

## Mapping (MongoDB → MoG)

- **Leading `$match`**: translated using the same filter translator used by `find`/`count`, then pushed down into the base `SELECT`.
- **Non-leading `$match`**: evaluated in-memory over already-fetched documents.

## Example

```python
[{"$match": {"status": "paid"}}]
```

## Worked example (syntax + SQL)

MongoDB syntax:

```python
pipeline = [
  {"$match": {"status": "paid", "total": {"$gte": 10}}},
  {"$limit": 10},
]
```

SQL MoG can execute when `$match` is the first stage:

```sql
SELECT data FROM doc.app__orders
WHERE data #> '{status}' = to_jsonb($1)
  AND CAST(data #>> '{total}' AS DOUBLE PRECISION) >= $2
```

Parameter example:

- `$1 = "paid"`
- `$2 = 10`

## Code pointers

- Aggregate handler: `internal/mongo/handler.go`
- Evaluator: `internal/mongo/pipeline.go` (`applyMatch`)
