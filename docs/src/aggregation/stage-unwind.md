# Stage: `$unwind`

## What it does

Flattens an array field, outputting one document per array element.

## Mapping (MongoDB → MoG)

- Executed **in-memory** by MoG’s pipeline evaluator.
- Includes “preserve null/empty” handling per MoG’s current semantics (see pipeline tests).

## Example

```python
[
  {"$lookup": {"from": "items", "localField": "item_id", "foreignField": "_id", "as": "item"}},
  {"$unwind": "$item"},
]
```

## Code pointers

- Evaluator: `internal/mongo/pipeline.go` (`applyUnwind`)
