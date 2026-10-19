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
| Spark UI (bronze job) | http://localhost:4040 | — |
| Spark UI (silver job) | http://localhost:4041 | — |

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

## Bronze streaming (phase 2)

`streaming/bronze_job.py` runs two Spark Structured Streaming queries that land each raw topic in Iceberg unchanged. Each row is parsed against an explicit schema and carries Kafka lineage columns: `kafka_partition`, `kafka_offset`, and `kafka_timestamp`.

| Table | Source topic | Partitioning |
|---|---|---|
| `bronze.vehicle_positions` | `vehicle_positions.raw` | `days(event_time)` |
| `bronze.trip_updates` | `trip_updates.raw` | `days(event_time)`; `stop_time_updates` stays a nested array |

- **Catalog:** an Iceberg JDBC catalog in Postgres (`iceberg-catalog-db`), with the data in `s3a://lakehouse/warehouse/`. The simpler file-based catalog was ruled out because MinIO has no atomic rename, so concurrent writers would be unsafe.
- **Checkpoints:** stored in `s3a://lakehouse/checkpoints/bronze_<feed>/`. Exactly-once comes from Kafka offsets in the checkpoint plus Iceberg's per-batch commit tracking. This was verified with zero duplicate `(partition, offset)` pairs after both a clean restart and a `kill -9`.
- **Triggers:** micro-batches every 10 s, each coalesced into one file per table per commit.

Query the lakehouse with:

```bash
docker compose exec spark python sql.py "SELECT count(*) FROM bronze.vehicle_positions"
```

## Silver: stop delays (phase 3)

`streaming/silver_job.py` turns `bronze.trip_updates` into **`silver.stop_delays`**, with one row per observed stop visit. The table is partitioned by `service_date` and `bucket(8, route_id)`.

```
bronze.trip_updates ─▶ watermark(event_time, 15 min) ─▶ dedup(trip, start_date, vehicle, event_time)
  ─▶ explode stops ─▶ per-trip state machine (trip_state.py) ─▶ join active schedule (broadcast)
  ─▶ delay_seconds ─▶ idempotent Iceberg append
```

**What "observed arrival" means.** MBTA publishes only *predicted* times, and it drops a stop from a trip's update once the vehicle has passed it. Silver therefore records the **last prediction made before the stop disappeared**. Validated against GPS: of 14,394 visits where the vehicle reported `STOPPED_AT` that stop, the median difference was 6 s and **99.6% were within 60 s**. Passed stops average about 94 predictions each.

`delay_seconds = observed_time − scheduled_time`

- **Scheduled time and times past midnight.** The scheduled time is the service-day start plus `arrival_secs`. The service-day start follows the GTFS definition, "noon minus 12 h" local time on the trip's `start_date`, which stays correct on DST days. So `25:10:00` on Oct 19 means 01:10 on Oct 20.
- **Join key.** Silver joins on `(service_date, trip_id, stop_sequence)` rather than `stop_id`, because realtime data reports platforms (`Alewife-02`) where the schedule has stations (`70061`). This key matches 99.97% of scheduled stops. ADDED trips have no schedule, so their `delay_seconds` is null.
- **Schedule size.** The broadcast schedule is only the trips running on the needed service days, filtered by `calendar` and `calendar_dates`: about 440k rows per day instead of 3.1M.

### Late data: what happens to an event that arrives 20 minutes late?

- **The watermark** is the maximum `event_time` seen so far minus 15 minutes, advanced after every micro-batch. An event 20 minutes behind the newest event is older than the watermark, so the first stateful operator **drops it from silver** and counts it in `numRowsDroppedByWatermark`, which appears in the silver logs and the Spark UI.
- **It isn't lost.** Bronze has no watermark, so the raw event is still in `bronze.trip_updates` (and in Kafka for 48 h). It can be backfilled by re-running silver over that bronze range with a fresh checkpoint.
- **It usually couldn't change anything anyway.** A late update is an old *prediction*. For each stop, silver keeps the newest prediction by event time, so an older one never wins. In addition, the stop has typically been released already, and updates for released stops are ignored.
- **Events less than 15 minutes late are fully counted.** A passed stop is held until the watermark reaches the moment it was passed, so a late correction still lands. `tests/replay_reorder.py` checks this by replaying real bronze data with 2-minute chunks reversed inside 10-minute windows. The output is **identical row for row** to the in-order run, and 25 injected 20-minute-late events were all dropped.
- **Why 15 minutes.** In real MBTA data, `event_time` lags `ingest_time` by p99 = 95 s and at most 12 min, so 15 minutes covers every real event observed. So far `dropped_late = 0` in production.
- **The trade-off is latency.** A stop reaches silver about 16.5 min (p50) after the vehicle passes, because of the watermark. Bronze stays near real time at p50 ≈ 5 s.

### Guarantees and state
- **Exactly-once.** Each Iceberg append stores the query id and batch id in its snapshot summary, and a replayed batch is skipped. A `kill -9` produced 0 duplicate stop visits.
- **State is bounded.** Spark keeps about 2.5k trips and about 150k dedup keys (the 15-minute window) in RocksDB, flat over hours. A trip's state is removed one hour after its last update.

Run the tests with:

```bash
docker compose run --rm --no-deps spark python -m pytest -q tests     # 212 unit tests
docker compose run --rm --no-deps spark python tests/replay_reorder.py # reorder + late-data replay (~8 min)
```

## Build status

| Phase | Status |
|---|---|
| 0 — Infra skeleton | ✅ |
| 1 — GTFS poller → Kafka | ✅ |
| 2 — Bronze streaming | ✅ |
| 3 — Silver join + delay | ✅ |
| 4 — Gold models + quality | ⏳ |
| 5 — Airflow + observability | ⏳ |
| 6 — Load test + K8s | ⏳ |

## Measured numbers

_Filled in Phase 6._
