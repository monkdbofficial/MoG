# Getting started

## Run locally (terminal)

1. Export (or edit) env vars:

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
```

2. Start MoG:

```bash
go run ./cmd/mog
```

To print version/build info:

```bash
go run ./cmd/mog --version
```

## Run options (Docker / binary / source)

=== "Docker"

    ```bash
    docker compose up --build
    ```

=== "Build a binary"

    ```bash
    go build -o mog ./cmd/mog
    ./mog
    ```

=== "Run from source"

    ```bash
    go run ./cmd/mog
    ```

3. Verify metrics:

```bash
curl http://localhost:8080/metrics
```

4. Connect with a MongoDB client:

```bash
mongosh "mongodb://$MOG_MONGO_USER:$MOG_MONGO_PASSWORD@localhost:27017/admin"
```

## Common next steps

- For sample CRUD + aggregation queries, see [Usage](usage.md).
- For a precise list of what is currently implemented, see [Supported](supported.md).

## Run with Docker Compose (optional)

If you want Prometheus + Grafana for local inspection:

```bash
docker compose up --build
```

See [Deployment](deployment.md) for details.
