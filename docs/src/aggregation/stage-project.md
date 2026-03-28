# Stage: `$project`

## What it does

Includes/excludes fields from documents.

## Mapping (MongoDB → MoG)

- Executed **in-memory** by MoG’s pipeline evaluator.
- No SQL pushdown for `$project` today; it operates on the documents already fetched as the aggregation base set.

## Example

```python
[{"$project": {"user_id": 1, "total": 1, "_id": 0}}]
```

## Code pointers

- Evaluator: `internal/mongo/pipeline.go` (`applyProject`)
