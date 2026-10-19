# Transit Lakehouse — Streaming Data Pipeline

A streaming lakehouse that answers one question: **which transit routes are chronically late, by how much, and at what times of day?**

```
GTFS-realtime feed → poller → Redpanda → Spark Structured Streaming
  → bronze Iceberg → silver Iceberg (delay_seconds) → dbt gold → Airflow + Grafana
```

The full spec and phase plan are in `PRD + Phase Build Plan — Streaming Data Pipeline.md`.

## Run locally

Prerequisites: Docker Desktop with Compose v2 and roughly 8–12 GB of RAM for the full stack.

```bash
cp .env.example .env      # optional: every value has a default
docker compose up -d
docker compose ps         # redpanda + minio should show (healthy)
```

| Service | URL | Login |
|---|---|---|
| Redpanda console | http://localhost:8080 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka (from host) | localhost:19092 | — |

Stop with `docker compose down`, or wipe all data with `docker compose down -v`.

## Build status

| Phase | Status |
|---|---|
| 0 — Infra skeleton | ✅ |
| 1 — GTFS poller → Kafka | ⏳ |
| 2 — Bronze streaming | ⏳ |
| 3 — Silver join + delay | ⏳ |
| 4 — Gold models + quality | ⏳ |
| 5 — Airflow + observability | ⏳ |
| 6 — Load test + K8s | ⏳ |

## Measured numbers

_Filled in Phase 6._
