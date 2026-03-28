# Deployment

## Docker Compose (recommended for local stacks)

This repo includes `docker-compose.yml` with:

- `mog` (MoG service)
- `prometheus`
- `grafana`

Start it:

```bash
docker compose up --build
```

Endpoints:

- MoG Mongo wire: `localhost:27017`
- MoG metrics: `http://localhost:8080/metrics`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

## Host-run MoG + Docker Grafana

If you run MoG on your host (terminal) but Prometheus/Grafana in Docker, use:

- `prometheus/prometheus.host.yml` (targets `host.docker.internal:8080`)
