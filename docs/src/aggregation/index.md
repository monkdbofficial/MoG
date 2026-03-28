# Aggregation

MoG implements aggregation with a hybrid execution model:

1. **Longest supported prefix pushdown** into SQL (leading `$match`, then optional `$group`, `$sort`, `$limit`, or `$count`)
2. **In-memory pipeline evaluation** in Go for any remaining stages

This section documents each supported stage as its own page so the sidebar can expand/collapse.

Code pointers:

- Aggregate handler: `internal/mongo/handler.go` (search for `aggregate`)
- Pipeline engine: `internal/mongo/pipeline.go`
