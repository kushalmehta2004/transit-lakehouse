# Transit Lakehouse — Streaming Data Pipeline

A streaming lakehouse that answers one question: **which transit routes are chronically late, by how much, and at what times of day?**

```
GTFS-realtime feed → poller → Redpanda → Spark Structured Streaming
  → bronze Iceberg → silver Iceberg (delay_seconds) → dbt gold → Airflow + Grafana
```

**Watch the 1-minute demo video:** [docs/demo/transit-lakehouse-demo.mp4](docs/demo/transit-lakehouse-demo.mp4)

**See it working:** the [feature demo](docs/demo/README.md) has real screenshots of every component (Kafka, Spark, Iceberg, dbt, Airflow, Grafana, the failure drills, and Kubernetes).

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
| Spark Thrift (JDBC, for dbt/BI) | jdbc:hive2://localhost:10000 · UI http://localhost:4042 | — |
| dbt docs (when served) | http://localhost:8085 | — |
| Airflow | http://localhost:8081 | admin / admin |
| Grafana dashboard | http://localhost:3000 | anonymous view; admin / admin to edit |
| Prometheus (alerts at /alerts) | http://localhost:9090 | — |

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
  ─▶ delay_seconds ─▶ insert-only MERGE on (service_date, trip_id, stop_sequence)
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
- **Exactly-once, with one row per stop visit enforced by the table.** The sink writes with `MERGE … WHEN NOT MATCHED THEN INSERT` on `(service_date, trip_id, stop_sequence)`, so a replayed batch inserts nothing. A stop emitted twice is never duplicated either; that happened before the tombstone fix (see phase 4). Spark plans an insert-only MERGE as anti-join + append, so no files are rewritten. A `kill -9` produced 0 duplicate stop visits.
- **State is bounded.** Spark keeps about 2.5k trips and about 150k dedup keys (the 15-minute window) in RocksDB, flat over hours. A trip's state is removed one hour after its last update.

Run the tests with:

```bash
docker compose run --rm --no-deps spark python -m pytest -q tests     # 212 unit tests
docker compose run --rm --no-deps spark python tests/replay_reorder.py # reorder + late-data replay (~8 min)
```

## Gold + quality gates (phase 4)

dbt connects to the lakehouse through a **Spark Thrift server** (`spark-thrift`, `jdbc:hive2://localhost:10000`). The server is started from the same `build_spark()` factory as the streaming jobs, so Spark is configured in exactly one place.

```
silver.stop_delays ─▶ stg_stop_delays (ephemeral: passed, schedule-matched, regular trips)
                        ─▶ gold.route_lateness_by_hour   (incremental insert_overwrite: route × service day × hour)
                              ─▶ gold.chronically_late_routes (rolling 7 days, thresholds in dbt_project.yml)
static GTFS routes ─▶ gold.dim_routes
```

| Model | Grain | What it answers |
|---|---|---|
| `route_lateness_by_hour` | route × service day × hour of day (local) | avg, median, and p90 delay; share > 5 min late; share on time (−1..+5 min) |
| `chronically_late_routes` | route | routes with avg delay ≥ 3 min or ≥ 25% of visits > 5 min late over 7 days, ranked, with worst hour |
| `dim_routes` | route | names and mode (bus, subway, …) |

`route_lateness_by_hour` is incremental. Each run re-aggregates only the trailing service day and **overwrites those `service_date` partitions** (dynamic partition overwrite), so the full history is never rebuilt. It started as a MERGE, but the phase 5 drill showed why that was wrong: a MERGE only upserts groups present in the new aggregate, so a group whose silver rows were deleted lingered in gold forever.

*Caveat:* MBTA runs the subway by frequency rather than strict timetable, so "delay vs. schedule" is most meaningful for buses and commuter rail.

### Quality gates: 39 checks that fail the pipeline

- **dbt: 24 tests.** These are `not_null` on keys, `unique` / `unique_combination` (one row per stop visit), `relationships` from route_id to `dim_routes`, and `accepted_values` on `hour_of_day`, `route_type`, and `finalized_by`. There's also a custom generic test, **`delay_in_plausible_range`**: a passed stop's delay must be within −1 h..+3 h, since real data spans −16..+43 min.
- **Great Expectations: 15 expectations.** The suite is `quality/expectations/silver_stop_delays.json`, run as a checkpoint over silver rows written in the last 6 h. It covers non-null keys, plausible delays, uniqueness, and timestamp monotonicity (`processed_time ≥ ingest_time`, `final_event_time ≥ first_event_time`, `passed_at > final_event_time`).
- **Drill (`quality/bad_row_drill.py`).** It injects a row with a 10-hour delay. Both the dbt test and GE **fail**; after the row is removed, both **pass**.
- **A real catch on the first run.** The uniqueness test found a **duplicate stop visit** in silver. MBTA had announced a skipped stop 4 hours ahead, the trip vanished from the feed, and it came back 2 hours later. By then the tracker's 1-hour tombstone had expired, so the stop was emitted twice. The fix in `trip_state.py` keeps tombstones until the end of the service day, and a regression test covers it.

```bash
docker compose run --rm dbt dbt build                                   # models + 24 tests
docker compose run --rm dbt python /work/quality/run_checks.py          # GE checkpoint
docker compose run --rm dbt python /work/quality/bad_row_drill.py       # prove the gates catch bad data
docker compose run --rm dbt dbt docs generate
docker compose run --rm -p 8085:8085 dbt dbt docs serve --host 0.0.0.0 --port 8085 --no-browser
#   → http://localhost:8085 (lineage graph: silver → gold)
```

*dbt-spark + Iceberg note:* Iceberg v2 tables don't support `SHOW TABLE EXTENDED` (SPARK-33393). Because of that, dbt-spark's fallback produced an empty docs catalog. `dbt/macros/describe_table_extended_without_caching.sql` fixes the fallback's output format so the docs show every column.

## Orchestration + observability (phase 5)

### Airflow (`orchestration/`)

Airflow runs as **one container** using `airflow standalone` with LocalExecutor. Its metadata lives in the existing Postgres, and dbt and Great Expectations run in an isolated venv inside the image.

| DAG | Schedule | Tasks |
|---|---|---|
| `transit_lakehouse_pipeline` | every 15 min | `dbt_run` → `dbt_test` → `ge_checkpoint` → `mark_refresh`; `alert_on_failure` runs only if something failed |
| `lakehouse_maintenance` | hourly | per table: `rewrite_data_files` (compaction) → `expire_snapshots` (> 24 h, keep ≥ 50) |

- **Safe to retry.** dbt merges by key, the tests only read, and `mark_refresh` MERGEs on `run_id` into `gold.pipeline_refreshes`. `max_active_runs=1` prevents overlapping runs.
- **Failures are red.** When something fails, `alert_on_failure` logs an `ALERT` line and fails itself. Because it's the DAG's only leaf, the whole run ends **red**. That run is then picked up by the `PipelineDagFailed` alert through Airflow's StatsD metrics.
- **Compaction keeps tables healthy.** Streaming writes leave about 1,000 small files per table every few hours, and each maintenance run brings that down to a handful (bronze.vehicle_positions went from 1,043 files to 2). Silver reads bronze as a stream and skips compaction snapshots, so it's unaffected.

### Prometheus + Grafana (`observability/`)

| Source | What it provides |
|---|---|
| poller `/metrics` | events produced, poll errors and latency, DLQ sends, feed age |
| Spark `PrometheusServlet` | per query: input and processing rate, batch duration, watermark, state rows; driver heap |
| **pipeline-exporter** (read-only) | **Kafka consumer lag** (Kafka high watermark minus the offsets in the Spark checkpoint, since Spark doesn't commit to a consumer group); **DLQ depth**; **end-to-end latency** (ingest → bronze, ingest → silver); **freshness**; latest quality-gate result |
| statsd-exporter | Airflow DAG and task outcomes and durations |

The Grafana dashboard "Transit lakehouse pipeline" (http://localhost:3000) is generated by `observability/grafana/build_dashboard.py`. It's laid out as questions:
1. Is data coming in?
2. Is Spark keeping up?
3. How old is the data?
4. Is silver's state bounded?
5. Are the DAG and its quality gates healthy?

**Alerts** (`observability/alerts.yml`, shown at http://localhost:9090/alerts and in the dashboard):

| Alert | Fires when |
|---|---|
| `BronzeLagHigh` (the PRD's lag alert) | no fresh data in bronze for > 2 min, sustained 5 min |
| `SilverLagHigh` | silver is well beyond its designed ~16 min: no writes for 10 min, or p50 > 25 min |
| `KafkaConsumerLagHigh` | bronze is > 20k messages behind Kafka for 5 min |
| `DeadLetterQueueGrowing` | anything lands in a DLQ |
| `PipelineDagFailed` / `QualityGateFailed` | a pipeline run or the GE checkpoint failed |

**How to read a slowdown.** If events/sec drops to 0 while freshness climbs and consumer lag stays at 0, the **source stopped**. If events/sec is normal while consumer lag climbs, **Spark is behind**. If both are normal but silver freshness climbs, **the silver job stopped**.

### Acceptance drills

**Drill 1: forced quality failure.** A silver row with a 10-hour delay was injected, then the pipeline was triggered.

| Time (UTC) | Result |
|---|---|
| 21:26:47 | `dbt_test` FAILED: `delay_in_plausible_range` caught it in silver and again in gold |
| 21:26:48 | `ge_checkpoint` and `mark_refresh` `upstream_failed`; `alert_on_failure` failed, so the run went **red** |
| 21:27:15 | `PipelineDagFailed` **firing** in Prometheus and Grafana |
| 21:28:15 | GE checkpoint `FAILED (14/15)` → `quality_gate_success = 0` → `QualityGateFailed` **firing** |
| 21:35:15 | after removing the row: GE 15/15 and the next run **green** |

The task log line was: `ALERT transit_lakehouse_pipeline run drill1_… FAILED; failed tasks: ['ge_checkpoint', 'dbt_test', 'mark_refresh']`

The drill exposed a real bug in gold. The MERGE-based incremental model kept the bad row's aggregate after the row was deleted, which is why the model now uses `insert_overwrite`.

**Drill 2: pause the poller and diagnose from the dashboard alone.**

| Time after `docker compose stop poller` | Events/sec | Bronze freshness | Kafka consumer lag | `BronzeLagHigh` |
|---|---|---|---|---|
| 0 | ~150 | 14 s | 0 | – |
| +1.5 min | 0 / no data | 89 s | 0 | – |
| +3 min | 0 | 179 s | 0 | **pending** |
| +8 min | 0 | 478 s | 0 | **firing** |
| restart +30 s | ~156 | 15 s (recovered in 28 s) | 0 | resolved |

Events/sec fell to 0, freshness climbed second for second, and consumer lag stayed at 0, which means **the source stopped, not Spark**. The DLQ was unchanged across the restart.


### Measured footprint
The whole stack uses **about 11.3 GB**:
- spark-silver 4.3 GB
- spark-thrift 2.7 GB
- spark bronze 1.7 GB
- airflow 1.1 GB
- minio 0.8 GB
- everything else under 0.3 GB each

Phase 5 added only about 1.3 GB for Airflow, Prometheus, Grafana, and both exporters.

## Build status

| Phase | Status |
|---|---|
| 0 — Infra skeleton | ✅ |
| 1 — GTFS poller → Kafka | ✅ |
| 2 — Bronze streaming | ✅ |
| 3 — Silver join + delay | ✅ |
| 4 — Gold models + quality | ✅ |
| 5 — Airflow + observability | ✅ |
| 6 — Load test + K8s | ✅ |

## Measured numbers

These were measured on one Windows laptop (12 cores, 15.5 GB for Docker), with the whole stack running and live MBTA data. Full method and raw data: [`scripts/results/phase6_perf.md`](scripts/results/phase6_perf.md).

| Metric | Result |
|---|---|
| Live feed volume | ~150 events/s (≈540 vehicles + 1,340 trips, every 15 s) |
| **Sustained throughput** (ingest → bronze) | **10,000 events/s** with flat Kafka lag. It breaks at 15k |
| Throughput fix | 2k → 10k events/s (**~4×**). The bronze write was coalesced to 1 task, so the job ran single-threaded; `BRONZE_WRITE_PARTITIONS=6` restored per-partition parallelism |
| **End-to-end latency, ingest → bronze** | **p50 5.0 s · p99 9.9 s** (the same at 10k events/s) |
| End-to-end latency, ingest → silver | p50 16.5 min · p99 18.6 min. This is **by design**: the 15-min watermark makes late events count |
| **Partitioning speedup** (one route, 7 days, 30-day table) | **1.9× faster (−47%)**: 0.67 s → 0.35 s, data read 51 MB → 4 MB |
| Partitioning, all routes, 7 days | 1.44× faster (−30%). A full-history single-route query is 11% *slower*, the over-partitioning trade-off |
| **Consumer recovery** after 5 min down | bronze caught up in **32 s** (40k-message backlog); silver in **101 s** |
| Backlog drain | 1.6M messages in ~80 s |
| Quality gates | **39** (24 dbt tests + 15 Great Expectations). They caught a real duplicate-stop bug and a stale-aggregate bug in gold |
| Correctness vs. GPS | 99.6% of observed arrivals within 60 s of the vehicle's `STOPPED_AT` GPS report |
| Memory footprint | ~11.3 GB for the full stack |

### Kubernetes (deployed once to `kind`; see [`k8s/`](k8s/README.md))

The core pipeline was deployed to a single-node kind cluster: Redpanda, MinIO, Postgres, the poller, and the bronze, silver and Thrift Spark jobs. **Every pod was Running and Ready with 0 restarts on the first apply**, and data flowed end to end: 104k trip updates went into bronze and 2.6k stop visits into silver, with 0 duplicates and a median delay of +133 s, the same as in compose. Evidence: [`k8s/results/deploy-final.txt`](k8s/results/deploy-final.txt).

- **Readiness and liveness probes on every service.** The Spark jobs use `Recreate` rollouts so two writers never share a checkpoint.
- **One HPA, on the stateless Thrift SQL server** (CPU 70%, 1→2 replicas). It scaled to 2 under a real analytical query. The poller and the Spark streaming jobs deliberately have no HPA: two pollers would duplicate events, and two writers would corrupt a single-writer checkpoint.
- **Break-it drill** ([`k8s/results/breakit.md`](k8s/results/breakit.md)):
  - **Bad readiness port:** the pod stayed `0/1`, its Service had no endpoints, and the app kept running unseen. `rollout undo` fixed it in 13 s.
  - **Bad liveness path:** this caused *nothing*, because the metrics server answers 200 on every path. That's a lesson in what a probe actually checks.
  - **Bad liveness port:** this produced **CrashLoopBackOff** after 6 restarts, and `rollout undo` fixed it.
- **What broke during deploy:** `kind load` fails on multi-platform public images with Docker Desktop's containerd store. The fix was to load only the locally built images and let the node pull the rest. Silver also peaked at 4.17 Gi against a 4.5 Gi limit, so its limit was raised to 5 Gi.

## Resume bullets

- Built an end-to-end streaming lakehouse (Kafka → Spark Structured Streaming → Iceberg → dbt → Airflow) on live MBTA transit data. It sustains **10,000 events/sec** at **p99 9.9 s** ingest-to-lakehouse latency, with a 4× throughput gain after finding a single-threaded write bottleneck in a load test.
- Implemented a stateful stream-static join with a 15-minute event-time watermark and an idempotent MERGE sink. The output was identical row-for-row under reordered replay, and 99.6% of computed arrivals matched GPS within 60 s.
- Cut lateness-query latency **47%** with Iceberg partitioning and incremental dbt models, and measured where over-partitioning backfires.
- Added **39** dbt and Great Expectations quality gates that fail the Airflow pipeline on bad data. They caught two real correctness bugs before they reached analytics.
- Instrumented with Prometheus and Grafana (consumer lag, freshness, DLQ, DAG health, with 6 alerts), and deployed on Kubernetes with readiness probes and an HPA. Recovery from a 5-minute consumer outage takes 32 s.

Blog post: [docs/blog-post.md](docs/blog-post.md)
