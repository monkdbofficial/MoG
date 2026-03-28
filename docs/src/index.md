# MoG documentation

**MoG** is a Go service that accepts **MongoDB client connections** (wire protocol) and proxies them to **MonkDB** by translating operations into SQL over a document table model.

> Repo/binary name: `mog` (paths like `cmd/mog/` and imports use `mog`).

## At a glance

- **MongoDB client port:** `27017` (default)
- **Metrics exporter:** `8080/metrics` (default)
- **Backend:** MonkDB via Postgres wire protocol (`pgx`)
- **Version info:** `mog --version`

## What MoG is (and isn’t)

MoG aims to be a pragmatic compatibility layer for applications that “speak MongoDB”, while storing documents in MonkDB.

It is **not** a full MongoDB server implementation. Only a subset of commands, query operators, and aggregation stages are supported (see [Limitations](limitations.md)).

## Repo layout

- `cmd/mog/`: main entry point (starts TCP server + metrics server)
- `internal/server/`: TCP accept loop and connection lifecycle
- `internal/mongo/`: Mongo wire protocol parsing/encoding + command handlers
- `internal/translator/`: translates a subset of Mongo-style filters/updates to SQL
- `internal/monkdb/`: MonkDB connection pool
- `prometheus/`, `grafana/`: optional local observability setup

## Quick links

- New here? Start with [Getting Started](getting-started.md).
- Want the end-to-end flow? Read [How It Works](how-it-works.md).
