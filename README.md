<p align="center">
  <img src="docs/src/imgs/monkdb_logo_v3.png" alt="MoG Logo" width="300"/>
</p>

<h1 align="center">MoG (MonkDB Gateway)</h1>

<p align="center">
  <b>MongoDB wire protocol proxy for MonkDB</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/MonkDB-MoG-blue" alt="MonkDB MoG" />
  <img src="https://img.shields.io/badge/version-v0.1.0-green" alt="Version" />
  <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License" />
  <img src="https://img.shields.io/badge/go-1.25.5-00ADD8" alt="Go" />
  <img src="https://img.shields.io/badge/PRs-welcome-brightgreen" alt="PRs Welcome" />
</p>

<p align="center">
  <img src="https://readme-typing-svg.herokuapp.com?font=Fira+Code&pause=100&color=F7DF1E&center=true&vCenter=true&width=435&lines=MongoDB+Wire+Protocol+Proxy;MonkDB+SQL+Translation;Hybrid+Aggregation+Engine;Cloud-Native+Observability" alt="Typing SVG" />
</p>

---

**MoG** is a MongoDB wire-protocol proxy for **MonkDB** built so you can use **MongoDB drivers, tools, and queries** to work with MonkDB **without changing application code**.

In practice:
- Keep your existing driver calls (`find`, `insert`, `update`, `aggregate`, …).
- Change only the connection string: point your app to **MoG** instead of MongoDB.
- MoG translates supported MongoDB commands into SQL over MonkDB’s document-table model, and returns Mongo-shaped responses.

**Current version:** `v0.1.0` (first open-source release). Use `mog --version` to see the exact build/commit you’re running.

## Why MoG?

MoG is designed for teams that like MongoDB’s developer experience, but want MonkDB as the backend.

- **Drop-in for existing apps**: no rewrites; swap the MongoDB endpoint for MoG.
- **Pragmatic compatibility**: implements a useful subset of the wire protocol and commands.
- **MonkDB-native storage**: documents are stored in MonkDB tables; optional raw document mirroring via `MOG_STORE_RAW_MONGO_JSON`.
- **Hybrid performance**: pushes down what’s safe to SQL and evaluates the rest in Go.

## Table of Contents

- [Key Features](#key-features)
- [Version & Build Info](#version--build-info)
- [Architecture](#architecture)
- [Supported Features](#supported-features)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Observability](#observability)
- [Testing](#testing)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)
- [Citation](#citation)

## Key Features

- **MongoDB Wire Protocol**: Implements `OP_MSG` and a subset of legacy commands for compatibility.
- **Full CRUD Support**: Standard MongoDB `find`, `insert`, `update`, `delete`, and `count` operations.
- **Smart Aggregation**: A hybrid pipeline engine that pushes matching, grouping, and sorting to SQL while evaluating the rest in Go.
- **Dynamic Schema Management**: Automatically maps MongoDB documents to relational tables, creating columns as needed.
- **BLOB Offload (Optional)**: Stores large `BinData` payloads in MonkDB **BLOB tables** via `/_blobs` HTTP endpoints, keeping documents SQL-safe while preserving Mongo semantics.
- **Secure Authentication**: Built-in support for `SCRAM-SHA-256` authentication for secure client connections.
- **High Availability**: Built with Go's standard library and `pgxpool` for robust connection management and scaling.
- **Cloud-Native**: Includes a Prometheus exporter and pre-configured Grafana dashboards for monitoring performance.

## Version & Build Info

MoG binaries include a semantic version and build metadata:

```bash
go run ./cmd/mog --version
```

- **Version:** `v0.1.0`
- **Build ID:** git commit SHA (embedded at build time)

To produce a fully stamped build:

```bash
go build -o mog -ldflags "-X mog/internal/version.Version=v0.1.0 -X mog/internal/version.Commit=$(git rev-parse --short HEAD) -X mog/internal/version.BuildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)" ./cmd/mog
```

## Architecture

MoG acts as a bridge between the MongoDB world and the MonkDB relational world.

<p align="center">
  <img src="internal/data/images/arch.png" alt="MoG Architecture" width="800"/>
</p>

## Supported Features

MoG implements a growing subset of MongoDB features. While not yet 100% MongoDB-complete, it supports the core command surface needed for most applications.

### Command Surface
| Category | Supported Commands |
| :--- | :--- |
| **CRUD** | `find`, `insert`, `update`, `delete`, `count` |
| **Metadata** | `listDatabases`, `listCollections`, `collStats`, `dbStats` |
| **Collections** | `create`, `drop`, `dropDatabase` |
| **Indexes** | `listIndexes`, `createIndexes`, `dropIndexes` |
| **Auth** | `saslStart`, `saslContinue` (SCRAM-SHA-256) |
| **System** | `ping`, `hello`, `serverStatus`, `buildInfo`, `getParameter`, `hostInfo`, `getCmdLineOpts` |

### Query & Update
- **Filter Operators**: Equality, `$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$in`, `$all`, `$exists`, `$type`
- **Update Operators**: `$set`, `$inc`, `$unset`, `$push`, `$pull`, `$addToSet`, replacement documents

### Hybrid Aggregation Pipeline
MoG uses a **hybrid aggregation engine**. It pushes the longest possible prefix down to SQL (leading `$match`, then optional `$group`, `$sort`, `$limit`, or `$count`) and evaluates remaining stages in memory (Go) for correctness.

#### Supported Stages
`$match`, `$project`, `$addFields`, `$set`, `$unset`, `$group`, `$sort`, `$limit`, `$sample`, `$count`, `$lookup`, `$unwind`, `$facet`, `$sortByCount`, `$graphLookup`, `$setWindowFields` (subset), `$replaceRoot`, `$replaceWith`, `$unionWith`

| Expression Category | Supported Operators |
| :--- | :--- |
| **Arithmetic** | `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`, `$abs`, `$ceil`, `$floor`, `$round`, `$trunc`, `$exp`, `$ln`, `$log`, `$log10`, `$pow`, `$sqrt` |
| **String** | `$concat`, `$split`, `$strLenBytes`, `$strLenCP`, `$toLower`, `$toUpper`, `$trim`, `$ltrim`, `$rtrim`, `$replaceAll`, `$replaceOne`, `$substr`, `$indexOfBytes`, `$indexOfCP`, `$regexMatch`, `$regexFind`, `$regexFindAll`, `$strcasecmp` |
| **Array** | `$arrayElemAt`, `$concatArrays`, `$first`, `$last`, `$in`, `$isArray`, `$range`, `$reverseArray`, `$size`, `$slice`, `$zip`, `$map`, `$filter`, `$sortArray`, `$allElementsTrue`, `$anyElementTrue`, `$reduce`, `$arrayToObject`, `$objectToArray`, `$indexOfArray` |
| **Date** | `$toDate`, `$dayOfMonth`, `$dayOfWeek`, `$dayOfYear`, `$hour`, `$millisecond`, `$minute`, `$month`, `$second`, `$week`, `$year`, `$dateToString`, `$dateFromString`, `$dateTrunc`, `$dateAdd`, `$dateSubtract`, `$dateDiff` |
| **Comparison** | `$cmp`, `$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$ne` |
| **Conditional** | `$cond`, `$ifNull`, `$switch` |
| **Type/Object** | `$convert`, `$type`, `$toBool`, `$toDouble`, `$toInt`, `$toLong`, `$toString`, `$getField`, `$setField`, `$unsetField`, `$mergeObjects` |
| **Misc/Sets** | `$literal`, `$rand`, `$meta`, `$let`, `$setDifference`, `$setEquals`, `$setIntersection`, `$setIsSubset`, `$setUnion` |

## Getting Started

### Prerequisites

- [Go 1.25+](https://go.dev/dl/)
- A running [MonkDB](https://www.monkdb.com) instance

### Installation & Run

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/monkdbofficial/mog.git
    cd mog
    ```

2.  **Configure environment:**
    Copy `.env.example` to `.env` and update the `MOG_DB_*` variables to point to your MonkDB instance.
    ```bash
    cp .env.example .env
    ```

3.  **Run MoG:**
    ```bash
    go run ./cmd/mog
    ```

4.  **Connect with a client:**
    ```bash
    mongosh "mongodb://user:password@localhost:27017/admin"
    ```

## Configuration

MoG is configured via environment variables or a `.env` file.

| Variable | Description | Default |
|----------|-------------|---------|
| `MOG_MONGO_PORT` | Port for the MongoDB wire protocol listener. | `27017` |
| `MOG_MONGO_USER` | Username for client authentication. | `user` |
| `MOG_MONGO_PASSWORD` | Password for client authentication. | `password` |
| `MOG_DB_HOST` | MonkDB backend host. | `localhost` |
| `MOG_DB_PORT` | MonkDB backend port. | `5432` |
| `MOG_DB_USER` | MonkDB backend username. | `monkdb` |
| `MOG_DB_PASSWORD` | MonkDB backend password. | `monkdb` |
| `MOG_DB_NAME` | MonkDB backend database name. | `monkdb` |
| `MOG_LOG_LEVEL` | Logging level (`debug`, `info`, `warn`, `error`). | `info` |
| `MOG_METRICS_PORT` | Port for the Prometheus metrics exporter. | `8080` |
| `MOG_STORE_RAW_MONGO_JSON` | Mirror full document into a `data` column (`OBJECT(DYNAMIC)`). | `0` |
| `MOG_STABLE_FIELD_ORDER` | Sort document fields for consistent output. | `0` |
| `MOG_INFO_LOG_WRITES` | Enable info-level logs for write operations. | `0` |
| `MOG_BLOB_TABLE` | Enable BLOB offload by setting a MonkDB BLOB table name. | _(empty)_ |
| `MOG_BLOB_HTTP_BASE` | MonkDB HTTP base used for `/_blobs/<table>/<sha1>` PUT/GET/DELETE. | `http://localhost:6000` |
| `MOG_BLOB_SHARDS` | Shard count used for best-effort BLOB table creation. | `3` |
| `MOG_BLOB_MIN_BYTES` | Only offload binaries at or above this size (bytes). | `256` |
| `MOG_BLOB_METADATA` | Enable creating/updating relational metadata rows for offloaded blobs. | `0` |
| `MOG_BLOB_METADATA_TABLE` | Relational table used for blob metadata when enabled. | `doc.blob_metadata` |
| `MOG_BLOB_INLINE_READS` | Inline blob content back into replies (dereference pointers). | `0` |
| `MOG_BLOB_INLINE_MAX_BYTES` | Max bytes to inline per blob when inline reads enabled. | `1048576` |
| `MOG_BLOB_INLINE_STRICT` | If `1`, error when blob can’t be inlined; if `0`, return pointer. | `0` |

### BLOB Storage (Optional)

MoG can offload large `BinData` fields to MonkDB BLOB tables to avoid oversized inline base64 payloads.

1. Create a BLOB table:
   ```sql
   CREATE BLOB TABLE media CLUSTERED INTO 3 SHARDS;
   ```
2. Configure MoG:
   ```bash
   export MOG_BLOB_TABLE=media
   export MOG_BLOB_HTTP_BASE=http://localhost:6000
   ```

## Observability

MoG includes built-in support for Prometheus and Grafana.

- **Metrics Endpoint**: `http://localhost:8080/metrics`
- **Prometheus & Grafana**: A pre-configured Docker Compose setup is available for local monitoring.
  ```bash
  docker compose -f docker-compose.host.yml up
  ```
  Access Grafana at `http://localhost:3000` (Default: `admin`/`admin`).

## Testing

We maintain high quality through rigorous testing. Run the full test suite with:

```bash
go test ./...
```

## Documentation

Detailed documentation is available in the `docs/` folder or can be served locally:

```bash
# Using MkDocs
pip install -r docs/requirements-docs.txt
mkdocs serve -f docs/mkdocs.yml
```

## Contributing

Contributions are welcome! Please read our [CONTRIBUTING.md](CONTRIBUTING.md) and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for details on our code of conduct and the process for submitting pull requests.

## Security

If you discover a security vulnerability, please refer to [SECURITY.md](SECURITY.md).

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE.md](LICENSE.md) file for details.

## Citation

If you use MoG (MonkDB Gateway) in your research or project, please cite it as follows:

```bibtex
@software{mog2026,
  author = {MonkDB Team},
  title = {MoG: High-performance MongoDB wire protocol proxy for MonkDB},
  year = {2026},
  url = {https://github.com/monkdbofficial/mog}
}
```

---

<p align="center">
  Made with <img src="https://img.shields.io/badge/-%E2%9D%A4-red" alt="heart" /> by <b><a href="https://www.monkdb.com">MonkDB</a></b>
  <br>
</p>
