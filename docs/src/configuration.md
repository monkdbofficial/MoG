# Configuration

MoG reads configuration from environment variables (see `internal/config/config.go`).

## Core variables

| Variable | Meaning | Default |
|---|---|---|
| `MOG_DB_HOST` | MonkDB host | `localhost` |
| `MOG_DB_PORT` | MonkDB port | `5432` |
| `MOG_DB_USER` | MonkDB username | `monkdb` |
| `MOG_DB_PASSWORD` | MonkDB password | `monkdb` |
| `MOG_DB_NAME` | MonkDB database | `monkdb` |
| `MOG_MONGO_PORT` | TCP port for Mongo wire protocol | `27017` |
| `MOG_MONGO_USER` | Username MoG accepts (SCRAM) | `user` |
| `MOG_MONGO_PASSWORD` | Password MoG accepts (SCRAM) | `password` |
| `MOG_LOG_LEVEL` | `debug`/`info`/`warn`/`error` | `info` |
| `LOG_FILE` | Log file path (empty = STDOUT) | empty |
| `MOG_METRICS_PORT` | HTTP metrics exporter port | `8080` |

## Storage & behavior

| Variable | Meaning | Default |
|---|---|---|
| `MOG_STORE_RAW_MONGO_JSON` | Mirror full document into a `data` column (`OBJECT(DYNAMIC)`) | `0` |
| `MOG_STABLE_FIELD_ORDER` | Sort fields for consistent output | `0` |
| `MOG_INFO_LOG_WRITES` | Info-level logs for write operations | `0` |

## Notes

- `MOG_MONGO_USER`/`MOG_MONGO_PASSWORD` are validated via a **server-side SCRAM-SHA-256** exchange (see `internal/mongo/auth.go`).
- The authentication implementation currently uses a fixed salt and an in-memory credential store; treat it as development-grade unless you harden it.
- The repo’s `.env.example` sets `MOG_STORE_RAW_MONGO_JSON=1` for a more “document-like” storage mode out of the box.
