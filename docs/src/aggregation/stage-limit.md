# Stage: `$limit`

## What it does

Limits the number of documents in the stream.

## Mapping (MongoDB → MoG)

- **SQL pushdown (preferred)** when `$limit` is part of the supported pushed-down prefix for `aggregate` (or when using `find`).
- Otherwise, executed **in-memory** by MoG’s pipeline evaluator.

## Example

```python
[{"$limit": 10}]
```

## Code pointers

- Evaluator: `internal/mongo/pipeline.go` (`applyPipeline`)
