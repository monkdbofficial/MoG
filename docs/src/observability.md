# Observability

## Prometheus exporter

MoG exposes Prometheus metrics at:

- `GET http://localhost:8080/metrics` (default)

You can change the port with `MOG_METRICS_PORT`.

This endpoint is an **exporter**, not Prometheus itself.

## Prometheus vs exporter vs Grafana

- **Exporter (MoG)**: `/metrics` only
- **Prometheus**: scrapes exporters and exposes the query API (`/api/v1/query`)
- **Grafana**: queries Prometheus (not exporters) to render dashboards

See `internal/server/metrics.go` and `prometheus/`.

## Local Prometheus + Grafana

If `mog` runs on your host (terminal) and you want Prometheus + Grafana in Docker:

```bash
docker compose -f docker-compose.host.yml up
```

Grafana provisions:
- Prometheus datasource
- Dashboard: **MoG Overview**

## Minimal logs

`MOG_LOG_LEVEL` controls the global log level (`debug`/`info`/`warn`/`error`).

To enable additional info-level logs for write commands:

- `MOG_INFO_LOG_WRITES=1`
