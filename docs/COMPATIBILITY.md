# Compatibility notes

MoG is a pragmatic compatibility layer, not a full MongoDB server.

## Arrays with `MOG_STORE_RAW_MONGO_JSON=0`

When raw document mirroring is disabled, MoG stores arrays in relational columns as JSON encoded `TEXT`.
On reads, MoG rehydrates JSON arrays from those columns back into Mongo-style arrays.

This makes queries like:

- `{"tags": {"$in": ["a"]}}`

work without requiring a `data OBJECT(DYNAMIC)` raw document column.

