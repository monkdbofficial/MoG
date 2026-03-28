# Stage: `$unset`

## What it does

Removes one or more fields from documents.

## Mapping (MongoDB → MoG)

- Executed **in-memory** by MoG’s pipeline evaluator.
- No SQL pushdown for `$unset` today.

## Example

```python
[{"$unset": "debug"}]
```

## Code pointers

- Evaluator: `internal/mongo/pipeline.go` (`applyUnsetStage`)
