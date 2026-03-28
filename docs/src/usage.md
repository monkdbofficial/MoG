# Usage

This page focuses on how to run **MoG** (binary: `mog`), connect to it, and what common queries look like.

## Start MoG

```bash
export MOG_DB_HOST=localhost
export MOG_DB_PORT=5432
export MOG_DB_USER=monkdb
export MOG_DB_PASSWORD=monkdb
export MOG_DB_NAME=monkdb

export MOG_MONGO_PORT=27017
export MOG_MONGO_USER=user
export MOG_MONGO_PASSWORD=password

export MOG_METRICS_PORT=8080
export MOG_LOG_LEVEL=info

go run ./cmd/mog
```

## Verify it’s up

Metrics exporter:

```bash
curl http://localhost:8080/metrics
```

Mongo wire port (quick check):

```bash
nc -vz localhost 27017
```

## Connect (mongosh)

```bash
mongosh "mongodb://$MOG_MONGO_USER:$MOG_MONGO_PASSWORD@127.0.0.1:27017/admin"
```

## Connect (PyMongo)

```python
from pymongo import MongoClient

client = MongoClient("mongodb://user:password@127.0.0.1:27017/admin")
db = client["app"]
col = db["orders"]
```

## CRUD examples

!!! tip "Create the collection table up front (optional)"
    Most clients will implicitly create collections by inserting, but you can also run:

    ```javascript
    use app
    db.runCommand({create: "orders"})
    ```

Insert:

```python
col.insert_one({"_id": 1, "user_id": 42, "total": 19.99, "status": "paid"})
```

Insert many:

```python
col.insert_many([
  {"_id": 2, "user_id": 42, "total": 5, "status": "paid"},
  {"_id": 3, "user_id": 7, "total": 120, "status": "pending"},
])
```

Find:

```python
list(col.find({"user_id": 42}))
```

Find with operators (supported subset):

```python
list(col.find({"total": {"$gte": 10, "$lt": 50}}))
list(col.find({"status": {"$in": ["paid", "shipped"]}}))
```

Sort / limit / skip:

```python
list(col.find({"status": "paid"}).sort("total", -1).skip(0).limit(10))
```

Update:

```python
col.update_one({"_id": 1}, {"$set": {"status": "shipped"}})
col.update_one({"_id": 1}, {"$inc": {"retry_count": 1}})
```

Delete:

```python
col.delete_one({"_id": 1})
```

## Aggregation examples

Basic `$match` + `$group`:

```python
pipeline = [
  {"$match": {"status": "paid"}},
  {"$group": {"_id": "$user_id", "n": {"$sum": 1}}},
  {"$sort": {"n": -1}},
  {"$limit": 10},
]
list(col.aggregate(pipeline))
```

Project + add a field reference:

```python
pipeline = [
  {"$project": {"user_id": 1, "total": 1, "_id": 0}},
  {"$addFields": {"user_id_copy": "$user_id"}},
]
list(col.aggregate(pipeline))
```

!!! note "Expression support is incremental"
    If you hit `unsupported ... expression`, check [Supported](supported.md) and `internal/mongo/pipeline.go`.

`$addFields` with common guard pattern (array length):

```python
pipeline = [
  {"$addFields": {
    "item_count": {
      "$cond": [
        {"$isArray": "$orders.items"},
        {"$size": "$orders.items"},
        0
      ]
    }
  }}
]
list(col.aggregate(pipeline))
```

Lookup + unwind (join-like behavior):

```python
orders = db["orders"]
items = db["items"]

items.insert_many([
  {"_id": "sku-1", "name": "Keyboard"},
  {"_id": "sku-2", "name": "Mouse"},
])
orders.insert_one({"_id": 10, "item_id": "sku-1"})

pipeline = [
  {"$lookup": {"from": "items", "localField": "item_id", "foreignField": "_id", "as": "item"}},
  {"$unwind": "$item"},
]
list(orders.aggregate(pipeline))
```

Sample:

```python
list(col.aggregate([{"$sample": {"size": 5}}]))
```
