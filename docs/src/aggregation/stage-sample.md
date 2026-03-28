# Stage: `$sample`

## What it does

Selects a random sample of documents.

## MoG behavior

- If `$sample` is the first stage, MoG attempts to push it down by using `ORDER BY random() LIMIT n` when fetching base docs.

## Mapping (MongoDB → MoG)

- **Leading `$sample`**: best-effort SQL pushdown.
- **Non-leading `$sample`**: executed in-memory by the pipeline evaluator.

## Worked example (syntax + SQL pushdown)

MongoDB syntax:

```python
pipeline = [
  {"$match": {"status": "paid"}},
  {"$sample": {"size": 5}},
]
```

SQL shape MoG may emit (pushdown of `$match` and `$sample`):

```sql
SELECT data FROM doc.app__orders
WHERE data #> '{status}' = to_jsonb($1)
ORDER BY random()
LIMIT 5
```

## Example

```python
[{"$sample": {"size": 5}}]
```

## Code pointers

- Aggregate handler: `internal/mongo/handler.go` (pushdown)
- Evaluator: `internal/mongo/pipeline.go` (`applySample`)
