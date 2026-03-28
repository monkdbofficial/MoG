# Stage: `$sort`

## What it does

Sorts documents by one or more fields.

## Mapping (MongoDB → MoG)

- **SQL pushdown (preferred)** when `$sort` is part of the supported pushed-down prefix for `aggregate`.
- Otherwise, executed **in-memory** by MoG’s pipeline evaluator for aggregation pipelines.
- For `find`, sorting is pushed down as `ORDER BY` when possible and falls back to in-memory only when translation fails (see `docs/commands/find.md`).

## Example

```python
[{"$sort": {"total": -1}}]
```

## Code pointers

- Evaluator: `internal/mongo/pipeline.go` (`applySort`)
