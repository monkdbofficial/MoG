# Aggregation recipes (complex queries)

These are “bigger” end-to-end pipelines you can copy/paste and adapt.

Each recipe includes:

- MongoDB pipeline syntax (PyMongo)
- What MoG runs in SQL vs what runs in-memory
- The SQL MoG executes (full pushdown when supported; otherwise base-document fetch and lookups)

## Top users by paid orders

```python
pipeline = [
  {"$match": {"status": "paid"}},
  {"$group": {"_id": "$user_id", "n": {"$sum": 1}}},
  {"$sort": {"n": -1}},
  {"$limit": 20},
]
list(db.orders.aggregate(pipeline))
```

What runs where:

- SQL: `$match` + `$group` + `$sort` + `$limit` (full pushdown)

SQL MoG executes:

```sql
SELECT data['user_id'] AS _id, COUNT(*) AS n
FROM doc.app__orders
WHERE data['status'] = $1
GROUP BY data['user_id']
ORDER BY n DESC
LIMIT 20
```

Parameter example:

- `$1 = "paid"`

## Join + flatten with `$lookup` + `$unwind`

```python
pipeline = [
  {"$lookup": {"from": "items", "localField": "item_id", "foreignField": "_id", "as": "item"}},
  {"$unwind": "$item"},
  {"$project": {"item.name": 1, "status": 1, "_id": 0}},
]
list(db.orders.aggregate(pipeline))
```

What runs where:

- SQL (base docs): `SELECT data FROM doc.<ordersPhysical>`
- SQL (lookup fetch): `SELECT data FROM doc.<itemsPhysical>`
- In-memory: `$lookup` join, `$unwind`, `$project`

SQL shapes MoG uses:

```sql
-- base docs
SELECT data FROM doc.app__orders
```

```sql
-- lookup collection fetch
SELECT data FROM doc.app__items
```

## Add derived field: guarded array length

```python
pipeline = [
  {"$addFields": {"item_count": {"$cond": [{"$isArray": "$items"}, {"$size": "$items"}, 0]}}},
  {"$match": {"item_count": {"$gte": 1}}},
]
list(db.orders.aggregate(pipeline))
```

What runs where:

- SQL (base docs): none (unless you add a leading `$match`)
- In-memory: `$addFields` (computed), `$match`

Note: In this example, the second `$match` runs in-memory (because it is not the first stage).

## Paid order sample (pushdown `$match` + `$sample`)

```python
pipeline = [
  {"$match": {"status": "paid"}},
  {"$sample": {"size": 5}},
]
list(db.orders.aggregate(pipeline))
```

What runs where:

- SQL (base docs): `$match` + `$sample` may be pushed down (best-effort)

SQL MoG may execute (best-effort pushdown of `$match` + `$sample`):

```sql
SELECT data FROM doc.app__orders
WHERE data['status'] = $1
ORDER BY random()
LIMIT 5
```

Parameter example:

- `$1 = "paid"`

## Revenue + average per user (group `$sum` + `$avg`)

```python
pipeline = [
  {"$match": {"status": "paid"}},
  {"$group": {"_id": "$user_id", "revenue": {"$sum": "$total"}, "avg_total": {"$avg": "$total"}}},
  {"$sort": {"revenue": -1}},
  {"$limit": 20},
]
list(db.orders.aggregate(pipeline))
```

What runs where:

- SQL: `$match` + `$group` + `$sort` + `$limit` (full pushdown)

SQL MoG executes:

```sql
SELECT data['user_id'] AS _id,
       SUM(COALESCE(CAST(data['total'] AS DOUBLE PRECISION), 0)) AS revenue,
       AVG(CAST(data['total'] AS DOUBLE PRECISION)) AS avg_total
FROM doc.app__orders
WHERE data['status'] = $1
GROUP BY data['user_id']
ORDER BY revenue DESC
LIMIT 20
```

Parameter example:

- `$1 = "paid"`

## Distinct users set + count (group `$addToSet` + `$size`)

```python
pipeline = [
  {"$match": {"status": "paid"}},
  {"$group": {"_id": None, "users": {"$addToSet": "$user_id"}}},
  {"$addFields": {"n_users": {"$size": "$users"}}},
  {"$project": {"users": 1, "n_users": 1, "_id": 0}},
]
list(db.orders.aggregate(pipeline))
```

What runs where:

- SQL (base docs): leading `$match`
- In-memory: `$group` (`$addToSet`), `$addFields` (`$size`), `$project`

SQL MoG executes (base fetch; the rest is in-memory):

```sql
SELECT data FROM doc.app__orders WHERE data['status'] = $1
```

Parameter example:

- `$1 = "paid"`

## Join + group by lookup field (category leaderboard)

```python
pipeline = [
  {"$match": {"status": "paid"}},
  {"$lookup": {"from": "items", "localField": "item_id", "foreignField": "_id", "as": "item"}},
  {"$unwind": "$item"},
  {"$group": {"_id": "$item.category", "n": {"$sum": 1}}},
  {"$sort": {"n": -1}},
  {"$limit": 10},
]
list(db.orders.aggregate(pipeline))
```

What runs where:

- SQL (base docs): leading `$match`
- SQL (lookup fetch): items collection fetch
- In-memory: `$lookup`, `$unwind`, `$group`, `$sort`, `$limit`

SQL shapes MoG uses:

```sql
SELECT data FROM doc.app__orders WHERE data['status'] = $1
```

Parameter example:

- `$1 = "paid"`

```sql
SELECT data FROM doc.app__items
```
