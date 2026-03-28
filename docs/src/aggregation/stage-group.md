# Stage: `$group`

## What it does

Groups documents by `_id` and computes accumulators.

## Mapping (MongoDB → MoG)

- **SQL pushdown (preferred)** when `$group` appears in the supported pushed-down prefix (after optional leading `$match`): MoG emits a `SELECT ... GROUP BY ...` query for a limited subset of `$group` shapes.
- Otherwise, executed **in-memory** by MoG’s pipeline evaluator.

## Example

```python
[
  {"$group": {"_id": "$user_id", "n": {"$sum": 1}}},
  {"$sort": {"n": -1}},
]
```

## Code pointers

- Evaluator: `internal/mongo/pipeline.go` (`applyGroup`)
