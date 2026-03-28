# Aggregation translation (overview)

MoG evaluates aggregation pipelines in **hybrid** mode: it pushes down a supported prefix into SQL, then evaluates any remaining stages in-memory (Go).

## Why in-memory?

MongoDB aggregation semantics and SQL dialects can diverge quickly for computed fields. MoG takes a pragmatic approach:

- Push down the longest supported prefix into SQL (leading `$match`, then optional `$group`, `$sort`, `$limit`, or `$count`)
- Evaluate the rest in Go for correctness and portability

See:

- Aggregation section: [Aggregation](aggregation/index.md)
- Pipeline engine: `internal/mongo/pipeline.go`

## Supported stages (current)

The in-memory pipeline evaluator supports a growing subset:

- `$match`
- `$project`
- `$addFields` and `$set`
- `$unset`
- `$lookup`
- `$unwind`
- `$group`
- `$count`
- `$sort`
- `$limit`
- `$sample`

## Computed expressions

Support is incremental. When you see errors like:

> `unsupported $addFields expression: ...`

it means the expression hasn’t been implemented in `internal/mongo/pipeline.go` yet.

Recently added examples:

- `$cond` (array form and object form)
- `$isArray`
- `$size`

## Stage-by-stage docs

The stage-by-stage docs and complex query recipes are split into individual pages so they can expand/collapse in the sidebar:

- `$match`: [Aggregation/$match](aggregation/stage-match.md)
- `$project`: [Aggregation/$project](aggregation/stage-project.md)
- `$addFields`/`$set`: [Aggregation/$addFields](aggregation/stage-addfields.md)
- `$unset`: [Aggregation/$unset](aggregation/stage-unset.md)
- `$group`: [Aggregation/$group](aggregation/stage-group.md)
- `$sort`: [Aggregation/$sort](aggregation/stage-sort.md)
- `$limit`: [Aggregation/$limit](aggregation/stage-limit.md)
- `$sample`: [Aggregation/$sample](aggregation/stage-sample.md)
- `$count`: [Aggregation/$count](aggregation/stage-count.md)
- `$lookup`: [Aggregation/$lookup](aggregation/stage-lookup.md)
- `$unwind`: [Aggregation/$unwind](aggregation/stage-unwind.md)
- Recipes: [Aggregation recipes](aggregation/recipes.md)
