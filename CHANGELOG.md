# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.1.0] - 2026-04-04

First public open-source release of **MoG** (Mongo wire-protocol adapter for MonkDB).

### Added

- Mongo wire-protocol server with basic auth (SCRAM-SHA-256, dev-grade).
- Query translation + pushdown pipeline for MonkDB.
- Built-in Prometheus metrics exporter and example Grafana dashboards.
- Optional BLOB offload support for large `BinData` payloads.
- Diagnostic logging controls via `MOG_SLOW_*_THRESHOLD_MS` (optional; set `0` to disable).

