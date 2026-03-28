# Stage: `$lookup`

## What it does

Performs a join-like lookup from another collection and adds results as an array field.

## Mapping (MongoDB → MoG)

- Executed **in-memory** by MoG’s pipeline evaluator.
- The “right side” collection is fetched from MonkDB via `SELECT data FROM doc.<fromPhysical>`.
- MoG caches lookup results within a single aggregation request.

## Worked example (syntax + SQL fetch)

MongoDB syntax:

```python
pipeline = [
  {"$lookup": {"from": "items", "localField": "item_id", "foreignField": "_id", "as": "item"}},
]
```

SQL shape MoG uses to fetch the lookup collection:

```sql
SELECT data FROM doc.app__items
```

## Example

```python
[
  {"$lookup": {"from": "items", "localField": "item_id", "foreignField": "_id", "as": "item"}},
]
```

## MoG behavior

- MoG fetches lookup collections from MonkDB and caches them per aggregation request for efficiency.

## Code pointers

- Aggregate handler lookup resolver: `internal/mongo/handler.go`
- Evaluator: `internal/mongo/pipeline.go` (`applyLookup`)
