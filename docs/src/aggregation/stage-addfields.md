# Stage: `$addFields` / `$set`

## What it does

Adds or overwrites fields on each document.

MoG supports a growing subset of computed expressions. See [Supported](../supported.md).

## Mapping (MongoDB → MoG)

- Executed **in-memory** by MoG’s pipeline evaluator.
- Computed expression support is incremental; unsupported expressions return errors like `unsupported $addFields expression: ...`.

## Examples

Field reference:

```python
[{"$addFields": {"user_id_copy": "$user_id"}}]
```

Guarded array length:

```python
[{"$addFields": {"n": {"$cond": [{"$isArray": "$items"}, {"$size": "$items"}, 0]}}}]
```

Nested assignment (dot path):

```python
[{"$addFields": {"address.city": "LA"}}]
```

## Code pointers

- Evaluator: `internal/mongo/pipeline.go` (`applyAddFields`, `evalAddFieldsValue`)
