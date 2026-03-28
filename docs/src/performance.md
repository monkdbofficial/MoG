# Performance

MoG is a compatibility layer. It will not match native MongoDB performance for all workloads, but the server is designed to avoid obvious bottlenecks (table scans, repeated DDL) for common CRUD.

## Practical tips

- Prefer stable document shapes for high-throughput writes.
- If you are load testing, ensure MonkDB itself is sized appropriately (CPU/RAM/disk).
- Use `insert_many` (bulk) instead of many `insert_one` calls.

## Implementation notes

- Inserts are batched into multi-row `INSERT` statements inside a single SQL transaction.
- Table/column DDL is cached in-process to avoid running `ALTER TABLE ... ADD COLUMN` repeatedly.
- `find` and `count` attempt SQL pushdown for simple filters (including dotted paths like `addr.city`); `$in` fields are excluded from SQL pushdown and applied in-memory for Mongo array semantics.
