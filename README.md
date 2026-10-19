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
| Poller metrics | http://localhost:8000/metrics | — |

Stop with `docker compose down`, or wipe all data with `docker compose down -v`.

## Data source

**MBTA (Boston)** publishes its feeds with no API key required:

| Feed | URL | Refresh |
|---|---|---|
| Vehicle positions (GTFS-rt) | https://cdn.mbta.com/realtime/VehiclePositions.pb | polled every 15 s |
| Trip updates / predictions (GTFS-rt) | https://cdn.mbta.com/realtime/TripUpdates.pb | polled every 15 s |
| Static schedule (GTFS zip) | https://cdn.mbta.com/MBTA_GTFS.zip | ETag checked hourly, reloaded on change or after 24 h |

## Ingestion (phase 1)

`ingestion/poller.py` fetches both realtime feeds, decodes the protobuf, and produces one JSON record per entity. The record contract is in the poller's docstring.

| Topic | Partitions | Key | Contents |
|---|---|---|---|
| `vehicle_positions.raw` | 6 | `route_id` | one record per vehicle |
| `trip_updates.raw` | 6 | `route_id` | one record per trip, with a nested `stop_time_updates[]` array |
| `*.dlq` | 1 | feed / entity id | fetch, decode, and validation failures, each with the error and payload |

- Entities that are byte-identical to the previous poll are skipped, because they're polling artifacts rather than new events.
- `schedule_loader.py` writes the static schedule to `s3://lakehouse/reference/gtfs_static/<table>/` for Spark. That covers stops, routes, trips, stop_times, calendar, and calendar_dates.
- GTFS times past midnight, such as `25:11:00`, are kept as `arrival_secs` = seconds since the start of the service day.

Run the unit tests with `docker compose run --rm poller python -m pytest -q tests`.

## Build status

| Phase | Status |
|---|---|
| 0 — Infra skeleton | ✅ |
| 1 — GTFS poller → Kafka | ✅ |
| 2 — Bronze streaming | ⏳ |
| 3 — Silver join + delay | ⏳ |
| 4 — Gold models + quality | ⏳ |
| 5 — Airflow + observability | ⏳ |
| 6 — Load test + K8s | ⏳ |

## Measured numbers

_Filled in Phase 6._
