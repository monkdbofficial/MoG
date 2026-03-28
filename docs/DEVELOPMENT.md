# Development

## Requirements

- Go 1.25+
- A running MonkDB instance (Postgres wire protocol)

## Setup

1. Copy `.env.example` to `.env` and adjust `MOG_DB_*` variables.
2. Run:
   - `go test ./...`
3. Start the server:
   - `go run ./cmd/mog`

## Useful env vars

- `MOG_LOG_LEVEL=debug`
- `MOG_ENV_FILE=/path/to/.env`
