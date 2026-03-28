# Stage: `$count`

## What it does

Counts the number of documents and returns a single document with the provided field name.

## Mapping (MongoDB → MoG)

- **SQL pushdown (preferred)** when `$count` is the first non-`$match` stage in the pipeline: MoG emits a `SELECT COUNT(*) ...` query.
- Otherwise, executed **in-memory** by MoG’s pipeline evaluator over the already-fetched documents.
- If you only need a simple count, consider using the `count` command (or driver helpers that call it) which maps directly to SQL `COUNT(*)`.

## Example

```python
[
  {"$match": {"status": "paid"}},
  {"$count": "n"},
]
```

## Code pointers

- Evaluator: `internal/mongo/pipeline.go`
