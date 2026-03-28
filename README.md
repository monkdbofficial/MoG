
<p align="center">
  <img src="docs/src/imgs/monkdb_logo_v3.png" alt="MoG Logo" width="300"/>
  <br>
  <font size="+5"><b>MoG (MonkDB Gateway)</b></font>
  <br>
  <b>MongoDB wire protocol proxy for MonkDB</b>
  <br>
  <br>
  <img src="https://img.shields.io/badge/MonkDB-MoG-blue" alt="MonkDB MoG" />
  <img src="https://img.shields.io/badge/version-v0.1.0-green" alt="Version" />
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License" />
  <img src="https://img.shields.io/badge/go-1.25.5-00ADD8" alt="Go" />
  <img src="https://img.shields.io/badge/PRs-welcome-brightgreen" alt="PRs Welcome" />
  <br>
  <br>
  <img src="https://readme-typing-svg.herokuapp.com?font=Fira+Code&pause=100&color=F7DF1E&center=true&vCenter=true&width=435&lines=MongoDB+Wire+Protocol+Proxy;MonkDB+SQL+Translation;Hybrid+Aggregation+Engine;Cloud-Native+Observability" alt="Typing SVG" />
  <br>
</p>

**MoG** is a MongoDB wire-protocol proxy for **MonkDB** built so you can use **MongoDB drivers, tools, and queries** to work with MonkDB **without changing application code**.

In practice:

- Keep your existing driver calls (`find`, `insert`, `update`, `aggregate`, …).
- Change only the connection string: point your app to **MoG** instead of MongoDB.
- MoG translates supported MongoDB commands into SQL over MonkDB’s document-table model, and returns Mongo-shaped responses.

**Current version:** `v0.1.0` (first open-source release). Use `mog --version` to see the exact build/commit you’re running.

## Why MoG?

MoG is designed for teams that like MongoDB’s developer experience, but want MonkDB as the backend.

- **Drop-in for existing apps**: no rewrites; swap the MongoDB endpoint for MoG.
- **Pragmatic compatibility**: implements a useful subset of the wire protocol and commands (see [Supported](docs/src/supported.md)).
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

## Key Features

- **MongoDB Wire Protocol**: Implements `OP_MSG` and a subset of legacy commands for compatibility.
- **Full CRUD Support**: Standard MongoDB `find`, `insert`, `update`, `delete`, and `count` operations.
- **Smart Aggregation**: A hybrid pipeline engine that pushes matching, grouping, and sorting to SQL while evaluating the rest in Go.
- **Dynamic Schema Management**: Automatically maps MongoDB documents to relational tables, creating columns as needed.
- **Secure Authentication**: Built-in support for `SCRAM-SHA-256` authentication for secure client connections.
- **High Availability**: Built with Go's standard library and `pgxpool` for robust connection management and scaling.
- **Cloud-Native**: Includes a Prometheus exporter and pre-configured Grafana dashboards for monitoring performance.

## Version & build info

MoG binaries include a semantic version and build metadata:

```bash
go run ./cmd/mog --version
```

- **Version:** `v0.1.0` (first open-source release)
- **Build ID:** git commit SHA (embedded at build time when provided)

To produce a fully stamped build (recommended for releases):

```bash
go build -o mog -ldflags "-X mog/internal/version.Version=v0.1.0 -X mog/internal/version.Commit=$(git rev-parse --short HEAD) -X mog/internal/version.BuildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)" ./cmd/mog
./mog --version
```

## Architecture

MoG acts as a bridge between the MongoDB world and the MonkDB relational world.

```text
                ┌──────────────────────────────────────────────────────────┐
                │                          MoG                             │
                │                    (Go process: `mog`)                   │
                │                                                          │
Mongo clients   │   ┌──────────────┐       ┌──────────────────────────┐    │
(mongosh/───────┼──▶│ TCP server   │       │ Mongo wire layer         │    │
drivers)        │   │ :27017       │──────▶│ - OP_MSG decode/encode   │    │
                │   └──────────────┘       │ - command routing        │    │
                │                          └────────────┬─────────────┘    │
                │                                       │                  │
                │                                       │ CRUD / metadata  │
                │                                       ▼                  │
                │                           ┌──────────────────────────┐   │
                │                           │ SQL translator           │   │
                │                           │ (filters/updates subset) │   │
                │                           └───────────┬──────────────┘   │
                │                                       │ SQL              │
                │                                       ▼                  │
                │                           ┌─────────────────────────┐    │
                │                           │ MonkDB pool (pgxpool)   │    │
                │                           └────────────┬────────────┘    │
                │                                        │                 │
                │                   Hybrid aggregate     │                 │
                │                   (in-memory)          │                 │
                │                     ┌──────────────────▼──────────────┐  │
                │                     │ Pipeline evaluator (Go)         │  │
                │                     │ $match/$project/$group/...      │  │
                │                     └─────────────────────────────────┘  │
                │                                                          │
                │   ┌──────────────┐                                       │
                │   │ Metrics HTTP │  GET /metrics                         │
                │   │ :8080        │◀───────────────────────────────┐      │
                │   └──────────────┘                                │      │
                └───────────────────────────────────────────────────┼──────┘
                                                                    │
                                                          Prometheus scrapes
                                                          Grafana queries Prometheus
```

## Supported Features

MoG implements a growing subset of MongoDB features. While not yet 100% MongoDB-complete, it supports the core command surface needed for many applications.

### Core Commands
- **CRUD**: `find`, `insert`, `update`, `delete`, `count`
- **Metadata**: `listDatabases`, `listCollections`, `collStats`, `dbStats`
- **Collections**: `create`, `drop`, `dropDatabase`
- **Indexes**: `listIndexes`, `createIndexes`, `dropIndexes`
- **Auth**: `saslStart`, `saslContinue` (SCRAM-SHA-256)
- **Other**: `ping`, `hello`, `serverStatus`, `buildInfo`, `getParameter`, `hostInfo`

### Query & Update Operators
- **Filters**: Equality, `$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$in`
- **Updates**: `$set`, `$inc`, replacement documents

### Hybrid Aggregation Pipeline
MoG uses a **hybrid aggregation engine**. It pushes the longest possible prefix down to SQL (leading `$match`, then optional `$group`, `$sort`, `$limit`, or `$count`) and evaluates remaining stages in memory (Go) for correctness.

**Supported Stages:**
- `$match`, `$project`, `$addFields`, `$set`, `$unset`
- `$group`, `$sort`, `$limit`, `$sample`, `$count`
- `$lookup`, `$unwind`, `$facet`, `$sortByCount`
- `$graphLookup`, `$setWindowFields` (subset)
- `$replaceRoot`, `$replaceWith`, `$unionWith`

<details>
<summary><b>Supported Pipeline Expressions (Click to expand)</b></summary>

- **Arithmetic:** `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`, `$abs`, `$ceil`, `$floor`, `$round`, `$trunc`, `$exp`, `$ln`, `$log`, `$log10`, `$pow`, `$sqrt`
- **String:** `$concat`, `$split`, `$strLenBytes`, `$strLenCP`, `$toLower`, `$toUpper`, `$trim`, `$ltrim`, `$rtrim`, `$replaceAll`, `$replaceOne`, `$substr`, `$indexOfBytes`, `$indexOfCP`, `$regexMatch`, `$regexFind`, `$regexFindAll`, `$strcasecmp`
- **Array:** `$arrayElemAt`, `$concatArrays`, `$first`, `$last`, `$in`, `$isArray`, `$range`, `$reverseArray`, `$size`, `$slice`, `$zip`, `$map`, `$filter`, `$sortArray`, `$allElementsTrue`, `$anyElementTrue`, `$reduce`
- **Date:** `$toDate`, `$dayOfMonth`, `$dayOfWeek`, `$dayOfYear`, `$hour`, `$millisecond`, `$minute`, `$month`, `$second`, `$week`, `$year`, `$dateToString`, `$dateFromString`, `$dateTrunc`, `$dateAdd`, `$dateSubtract`, `$dateDiff`
- **Comparison:** `$cmp`, `$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$ne`
- **Conditional:** `$cond`, `$ifNull`, `$switch`
- **Type/Object:** `$convert`, `$type`, `$toBool`, `$toDouble`, `$toInt`, `$toLong`, `$toString`, `$getField`, `$setField`, `$unsetField`, `$mergeObjects`, `$objectToArray`, `$arrayToObject`
- **Misc:** `$literal`, `$rand`, `$meta`, `$let`
</details>

## Getting Started

### Prerequisites

- [Go 1.25+](https://go.dev/dl/)
- A running [MonkDB](https://github.com/monkdb/monkdb) instance (or Postgres-compatible backend).

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

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

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
