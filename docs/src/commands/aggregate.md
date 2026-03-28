# `aggregate`

## Usage

```javascript
db.runCommand({
  aggregate: "orders",
  pipeline: [
    { $match: { status: "paid" } },
    { $group: { _id: "$user_id", n: { $sum: 1 } } },
    { $sort: { n: -1 } },
    { $limit: 10 }
  ],
  cursor: {}
})
```

## Behavior (hybrid: pushdown when possible)

MoG executes aggregation in two modes:

1) **Full SQL pushdown** (fast path)

If the entire pipeline is supported by the SQL pushdown planner (currently a strict subset), MoG executes it fully in MonkDB and returns rows directly.

2) **Hybrid fallback** (compat path)

If the pipeline includes unsupported stages/expressions, MoG pushes down the **longest supported prefix** (currently: leading `$match`, then optional `$group`, optional `$sort`, optional `$limit`, or a `$count`) to reduce work, then evaluates the remaining stages in Go.

This is intentionally developer-friendly: adding a stage/expression is usually “edit evaluator + add a test”.

## Mapping (MongoDB → SQL + in-memory)

- SQL pushdown planner: `internal/translator/translator.go` (`TranslateAggregatePlan`, `TranslateAggregatePrefixPlan`)
- In-memory pipeline engine: `internal/mongo/pipeline.go` (`applyPipelineWithLookup`)

## Worked example (syntax + SQL executed)

MongoDB syntax:

```javascript
db.runCommand({
  aggregate: "orders",
  pipeline: [
    { $match: { status: "paid" } },
    { $group: { _id: "$user_id", n: { $sum: 1 } } },
    { $sort: { n: -1 } },
    { $limit: 10 }
  ],
  cursor: {}
})
```

SQL MoG executes for this pipeline (full pushdown):

```sql
SELECT data['user_id'] AS _id, COUNT(*) AS n
FROM doc.app__orders
WHERE data['status'] = $1
GROUP BY data['user_id']
ORDER BY n DESC
LIMIT 10
```

Parameter example:

- `$1 = "paid"`

## Code pointers

- Handler: `internal/mongo/handler.go`
- Pipeline engine: `internal/mongo/pipeline.go`
