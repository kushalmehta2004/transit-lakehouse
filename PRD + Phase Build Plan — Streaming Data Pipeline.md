# PRD + Phase Build Plan — Streaming Data Pipeline

Oct 18, 2026 · @Kushal

## Read this first — rules for the coding agent

This doc is written to be built **one phase at a time**. Follow these rules and the code won't break out from under you.

1. **Build phases in order. One phase per working session.** Do not start Phase N+1 until Phase N's acceptance check passes.
2. **Each phase ends with an acceptance check. It is a gate.** If it doesn't pass, fix the current phase — do not move on and do not "work around" it in a later phase.
3. **Commit after every passing phase.** One git commit per phase, message `phase N: <name>`. This gives you a known-good point to roll back to.
4. **Additive only. Never refactor a phase that already passed** unless this doc's later phase explicitly says to change a specific file. New phases add new files or extend existing ones at marked points — they do not rewrite working modules.
5. **Respect the repo layout** (see the Repo layout section). Each concern lives in its own module so phases stay isolated. Don't collapse everything into one file.
6. **Pin versions** from the Tech stack section. Don't upgrade a dependency mid-build to fix an error — find the real cause.
7. **Each phase lists "Do not touch."** Those files/services are load-bearing for earlier phases; leave them alone.
8. **When stuck, surface the exact error and the acceptance check you're failing** — don't silently change scope.

**Agent prompt template for each phase:**

> "We're on Phase N of the build plan doc. Here is the phase spec: \[paste the phase\]. Only implement this phase. Touch only the files it lists. When done, run the acceptance check and show me the output. Do not modify anything from earlier phases."

## Problem, goal, definition of done

**Problem.** Public transit agencies publish live vehicle positions and arrival predictions as GTFS-realtime feeds, but the operationally useful question — *which routes are chronically late, by how much, and at what times of day?* — requires ingesting that stream continuously, joining it to the static schedule, and modeling it. That's a real-time data-engineering problem end to end.

**Goal.** A production-shaped streaming lakehouse: ingest the live feed → process it in a streaming engine with correct late-data handling → land it in a versioned lakehouse → model it with dbt → orchestrate and quality-gate with Airflow → observe it in Grafana → deploy it to Kubernetes once. Everything reproducible with `docker compose up`.

**Primary feed.** GTFS-realtime from a large agency. Good choices: a feed with no API key if possible, or one with a free key. The static GTFS schedule (`.zip` of `stops.txt`, `routes.txt`, `trips.txt`, `stop_times.txt`) comes from the same agency. Pick the agency in Phase 1 and pin its URLs in config.

**Definition of done:**

- [ ] `docker compose up` brings the whole stack up; data flows within \~1 minute.
- [ ] Live positions + trip updates land in a bronze Iceberg table.
- [ ] A silver table has per-stop delay computed from a stream-static join with correct watermarking.
- [ ] dbt gold tables answer the lateness question and refresh on an Airflow schedule.
- [ ] Data-quality checks run in the DAG and fail it on bad data.
- [ ] Grafana shows throughput, end-to-end lag, DLQ depth, and DAG health.
- [ ] README has measured numbers: events/sec, p50/p99 end-to-end latency, partitioning speedup.
- [ ] Deployed to K8s once, with a short writeup of what broke.

## Architecture

**Data flow:**

```
GTFS-realtime feed ──poll──> poller ──produce──> Kafka/Redpanda
                                                    │
                                      Spark Structured Streaming
                                                    │
                         ┌──────────────────────────┴───────────┐
                     bronze (raw)                          silver (joined+delay)
                     Iceberg/MinIO                          Iceberg/MinIO
                                                                 │
                                                              dbt (gold)
                                                                 │
                                                             Grafana / BI

Airflow orchestrates dbt + quality checks.   Prometheus scrapes all services.
```

**Components and their contract (input → output):**

1. **Poller** (`ingestion/`). Polls the GTFS-realtime protobuf endpoint on an interval; loads the static schedule less often. Decodes protobuf, stamps `event_time` and `ingest_time`, produces JSON to Kafka. On decode/parse failure → dead-letter topic. **Out:** `vehicle_positions.raw`, `trip_updates.raw`.
2. **Kafka / Redpanda** (`infra/compose`). Durable, replayable log. Topics partitioned by `route_id` so one route's events stay ordered. Plus `*.dlq`. **Out:** ordered, replayable events.
3. **Spark Structured Streaming** (`streaming/`). Reads raw topics; parses; dedups (same vehicle + same timestamp dropped); joins trip updates to the static schedule to compute `delay_seconds = actual_arrival − scheduled_arrival`; watermarks for late data. **Out:** bronze (raw landed) + silver (clean, enriched) Iceberg tables.
4. **Iceberg on MinIO** (`infra/`). Bronze = exactly what arrived; silver = cleaned + delay-annotated. Schema evolution + time-travel. **Out:** versioned, queryable tables.
5. **dbt** (`dbt/`). Silver → gold: `route_lateness_by_hour`, `chronically_late_routes`. dbt tests live here. **Out:** analytics tables + lineage docs.
6. **Airflow** (`orchestration/`). DAG: `dbt run` → data-quality checks → mark refresh → alert on failure. **Out:** scheduled, monitored runs.
7. **Prometheus + Grafana** (`observability/`). Scrape poller `/metrics`, Kafka/Spark lag, DAG status. **Out:** dashboards + a lag alert.

**Medallion pattern** (bronze/silver/gold) is deliberate — it's how real lakehouses are structured, so each layer is a defensible design choice in an interview.

## Tech stack & pinned versions

Pin these. If a newer version is out when you build, it's fine to bump the patch, but keep the majors here to avoid API drift the agent can't reason about.

| Tool | Version | Role |
| --- | --- | --- |
| Python | 3.11 | Poller, Spark jobs, dbt, Airflow |
| Redpanda | 24.x | Kafka-compatible message bus (lighter than Kafka locally) |
| Apache Spark | 3.5.x (PySpark) | Structured Streaming compute |
| Apache Iceberg | 1.5.x (Spark runtime jar) | Lakehouse table format |
| MinIO | latest stable | S3-compatible object store (local) |
| dbt-core + dbt-spark | 1.8.x | Transformation + tests |
| Apache Airflow | 2.9.x | Orchestration |
| Great Expectations | 0.18.x | Data-quality checks |
| Prometheus | 2.5x.x | Metrics scrape |
| Grafana | 11.x | Dashboards |
| gtfs-realtime-bindings | latest | Protobuf decode for the feed |
| Docker Compose | v2 | Local orchestration |
| kind or k3d | latest | Local K8s for the deploy phase |

**Key library notes for the agent:**

- Use `confluent-kafka` (not `kafka-python`) for the producer — it's the one real systems use and handles backpressure better.
- Spark reads Kafka via the `spark-sql-kafka` package and writes Iceberg via the Iceberg Spark runtime jar; both are declared as `--packages` in the Spark session. Pin the exact coordinates in `streaming/spark_session.py`.
- dbt talks to the lakehouse via the Spark Thrift server (or `dbt-duckdb` against the Iceberg catalog if you want a lighter path — decide in Phase 4, default to dbt-spark).

## Repo layout

The agent creates this structure in Phase 0 and keeps each concern in its own folder. Phase isolation depends on this.

```
transit-lakehouse/
├── docker-compose.yml          # all services; grows one block per phase
├── .env.example                # feed URLs, keys, bucket names
├── README.md                   # metrics + run instructions
├── ingestion/
│   ├── poller.py               # GTFS poll loop → Kafka
│   ├── schedule_loader.py      # static GTFS zip → reference table
│   ├── metrics.py              # Prometheus /metrics
│   └── requirements.txt
├── streaming/
│   ├── spark_session.py        # Spark+Kafka+Iceberg session factory
│   ├── bronze_job.py           # raw topics → bronze Iceberg
│   ├── silver_job.py           # join + delay → silver Iceberg
│   └── requirements.txt
├── dbt/
│   ├── dbt_project.yml
│   ├── models/gold/*.sql
│   └── tests/
├── quality/
│   └── expectations/           # Great Expectations suites
├── orchestration/
│   └── dags/pipeline_dag.py    # Airflow DAG
├── observability/
│   ├── prometheus.yml
│   └── grafana/dashboards/*.json
└── k8s/                        # manifests, added in Phase 6
    └── *.yaml
```

**Rule:** a phase touches only its own folder plus the one marked insertion point in `docker-compose.yml`. The compose file grows by appending a service block per phase — never rewrite existing blocks.

## Phase 0 — Scaffold + infra skeleton

**Goal:** the repo exists, Redpanda + MinIO come up cleanly, nothing else. A foundation you can't break because there's no logic yet.

**Prerequisites:** Docker Desktop running.

**Create:** the full folder tree (empty files with `# TODO phase N` headers), `docker-compose.yml` with only Redpanda + MinIO + the Redpanda console, `.env.example`, `README.md` skeleton, `.gitignore`.

**Steps:**

1. Scaffold the tree from the Repo layout section.
2. Add Redpanda (with its console UI on a mapped port) and MinIO (with console) to compose, each with a `healthcheck`.
3. Create a MinIO bucket `lakehouse` on startup (an init container or a documented one-liner).
4. Write the README "run locally" stub.

**Acceptance check (gate):**

- `docker compose up -d` → both services report healthy.
- Redpanda console loads in the browser; you can create a test topic.
- MinIO console loads; the `lakehouse` bucket exists.
- `docker compose down && docker compose up -d` works twice in a row (reproducibility).

**Do not touch after this phase:** the Redpanda and MinIO service definitions. Later phases append new services; they don't edit these.

**Commit:** `phase 0: infra skeleton`.

## Phase 1 — Ingestion: GTFS poller → Kafka

**Goal:** real transit data flowing into Kafka, with metrics and a dead-letter path.

**Prerequisites:** Phase 0 green.

**Create:** `ingestion/poller.py`, `ingestion/schedule_loader.py`, `ingestion/metrics.py`, `ingestion/requirements.txt`, a `poller` service block in compose.

**Steps:**

1. Choose the agency; put the realtime URL, static GTFS zip URL, and poll interval in `.env`.
2. `schedule_loader.py`: download + unzip the static GTFS, parse `stops/routes/trips/stop_times` into a reference form, cache to a local parquet (refresh daily). This is read-reference, not streamed.
3. `poller.py`: loop every N seconds → fetch protobuf → decode with `gtfs-realtime-bindings` → for each entity, build a JSON record with `event_time`, `ingest_time`, `route_id`, `trip_id`, `vehicle_id`, position/arrival fields → produce to `vehicle_positions.raw` / `trip_updates.raw` keyed by `route_id`. Any decode/validation failure → `*.dlq` with the error.
4. `metrics.py`: expose `/metrics` (events produced, poll errors, poll latency).

**Acceptance check (gate):**

- `docker compose up` → within a minute, `kafka-console-consumer` (or the Redpanda console) shows real records on both raw topics.
- Each record has non-null `event_time`, `ingest_time`, and keys.
- Kill the feed URL (bad env) → records go to DLQ, poller stays alive, error metric increments.
- `/metrics` returns Prometheus format.

**Do not touch:** Phase 0 services.

**Commit:** `phase 1: gtfs poller`.

## Phase 2 — Streaming: raw topics → bronze Iceberg

**Goal:** Spark Structured Streaming consumes the raw topics and lands them, untransformed, into bronze Iceberg tables on MinIO.

**Prerequisites:** Phase 1 green (real data on topics).

**Create:** `streaming/spark_session.py`, `streaming/bronze_job.py`, `streaming/requirements.txt`, a `spark` service block in compose.

**Steps:**

1. `spark_session.py`: one factory that builds a SparkSession with the Kafka + Iceberg packages, the Iceberg catalog pointed at MinIO (S3A config: endpoint, path-style access, credentials from env). Every job imports this — don't configure Spark in two places.
2. `bronze_job.py`: `readStream` from both raw topics → parse the JSON value against an explicit schema (no `inferSchema` on a stream) → `writeStream` to Iceberg bronze tables (`bronze.vehicle_positions`, `bronze.trip_updates`) with checkpointing to MinIO.
3. Trigger: processing-time micro-batch (e.g. 10s). Keep bronze a faithful copy — no dedup, no join yet.

**Acceptance check (gate):**

- Spark job runs without crashing; the streaming query stays `ACTIVE`.
- `SELECT count(*)` on bronze tables grows over time.
- Stop and restart the job → it resumes from checkpoint, no duplicate reprocessing of old offsets.
- The bronze schema is explicit and matches the producer's fields.

**Do not touch:** the poller, Phase 0 services. `spark_session.py` becomes shared infra — later phases import it, don't rewrite it.

**Commit:** `phase 2: bronze streaming`.

## Phase 3 — Silver: stream-static join + delay (the hard one)

**Goal:** the core data-engineering work — join live trip updates to the static schedule, compute per-stop delay, handle late data correctly, write silver.

**Prerequisites:** Phase 2 green (bronze filling).

**Create:** `streaming/silver_job.py`. Extend compose only if you run it as a separate Spark submit.

**Steps:**

1. Load the static schedule (from Phase 1's reference parquet) as a Spark DataFrame; broadcast it (it's small and slow-changing).
2. `readStream` bronze trip\_updates → for each predicted/actual stop arrival, look up the scheduled arrival from the broadcast schedule by `(trip_id, stop_id)` → compute `delay_seconds`.
3. Dedup: drop repeated `(vehicle_id, trip_id, stop_id, event_time)` within a window.
4. Watermark on `event_time` (e.g. 15 min) so late events are still counted but unbounded state is bounded.
5. Write to `silver.stop_delays` (partition by `route_id`, `date`).

**Acceptance check (gate):**

- `silver.stop_delays` populates with sane `delay_seconds` (sanity-check a few against the live feed; no 10-hour delays from bad joins).
- Replay a chunk of bronze with some events reordered → delays still correct (watermark handles late data, not drops it).
- Document: "what happens to an event that arrives 20 min late" — you must be able to answer this; it's the interview question.
- State store size stays bounded over a long run (watch the Spark UI).

**Do not touch:** bronze job, poller, `spark_session.py` internals.

**Commit:** `phase 3: silver join + delay`.

## Phase 4 — Gold: dbt models + data-quality gates

**Goal:** silver → analytics tables that answer the lateness question, with tests that fail on bad data.

**Prerequisites:** Phase 3 green (silver has real delays).

**Create:** `dbt/` project, `dbt/models/gold/*.sql`, `dbt/tests/`, `quality/expectations/`, a `dbt` runner path (CLI run for now; Airflow wires it in Phase 5).

**Steps:**

1. Configure dbt against the lakehouse (dbt-spark via the Spark Thrift server; the Iceberg catalog is the source).
2. Models: `route_lateness_by_hour` (avg/median/p90 delay per route per hour-of-day), `chronically_late_routes` (routes above a delay threshold across a rolling window). Prefer incremental models — don't rebuild the world each run.
3. dbt tests: `not_null` on keys, `accepted_values` on `route_id`, a custom test that `delay_seconds` sits in a plausible range.
4. One Great Expectations suite on silver (e.g. non-null positions, timestamp monotonicity) that returns pass/fail.

**Acceptance check (gate):**

- `dbt run` builds gold tables; `dbt test` passes on good data.
- Inject a bad row (impossible delay) → the custom dbt test and the GE suite both fail. This is the key check — quality gates must actually catch things.
- `dbt docs generate` produces lineage showing silver → gold.

**Do not touch:** all streaming/ingestion code.

**Commit:** `phase 4: gold models + quality`.

## Phase 5 — Orchestration + observability

**Goal:** Airflow owns the periodic model layer and quality gates; Grafana makes the whole pipeline visible.

**Prerequisites:** Phase 4 green.

**Create:** `orchestration/dags/pipeline_dag.py`, `observability/prometheus.yml`, `observability/grafana/dashboards/*.json`, plus `airflow`, `prometheus`, `grafana` service blocks in compose.

**Steps:**

1. Airflow DAG, scheduled (e.g. every 15 min): task `dbt run` → task `dbt test` → task `great_expectations checkpoint` → on any failure, the DAG fails and fires an alert. Make tasks idempotent and retrying.
2. Prometheus scrape config for the poller `/metrics`, Spark metrics, and Airflow's metrics endpoint.
3. Grafana dashboard panels: events/sec (poller), consumer lag (Spark/Kafka), end-to-end lag (ingest\_time → silver write time), DLQ depth, DAG success rate.
4. One Grafana/Prometheus alert rule: lag above threshold for 5 min.

**Acceptance check (gate):**

- Airflow UI shows the DAG running green on schedule.
- Force a quality failure → the DAG goes red and the alert fires.
- Grafana shows live throughput and lag; you can diagnose a slowdown from the dashboard alone (test by pausing the poller and watching lag climb).

**Do not touch:** streaming/ingestion/dbt logic — this phase only orchestrates and observes what already works.

**Commit:** `phase 5: airflow + observability`.

## Phase 6 — Load, measure, deploy

**Goal:** find the breaking point, record real numbers, and deploy to Kubernetes once.

**Prerequisites:** Phases 0–5 green; the full pipeline runs end to end.

**Create:** a load script (`scripts/replay.py` that republishes historical/accelerated events), `k8s/*.yaml` manifests.

**Steps:**

1. **Load test:** accelerate the poller or replay a backlog to push events/sec up until lag grows unbounded or something falls over. Record: sustained events/sec, p50/p99 end-to-end latency, where it broke, and the fix (more partitions, Spark tuning, state TTL).
2. **Partitioning benchmark:** time a lateness query before vs after Iceberg partitioning; record the speedup.
3. **Recovery test:** kill the Spark consumer, restart, measure catch-up time.
4. **K8s:** write manifests (Deployments, Services, ConfigMaps, a readiness probe per service, one HPA on the poller or Spark). Deploy to a local `kind`/`k3d` cluster or a cheap cloud cluster.
5. **Break it on purpose:** misconfigure a readiness probe, watch the CrashLoopBackOff, read logs, fix it. Note it for the blog post.
6. Fill the README with all numbers; write the \~1000-word blog post.

**Acceptance check (final gate):**

- README shows real measured numbers, not placeholders.
- The stack runs on K8s (screenshot `kubectl get pods` all Running).
- Blog post drafted, linked from README.

**Commit:** `phase 6: load test + k8s`. Tag the repo `v1.0`.

## Metrics & resume bullets

**Capture (into the README):** sustained events/sec; p50/p99 end-to-end latency; partitioning query speedup; number of quality gates + a real catch; consumer recovery time.

**Resume bullets** (fill real numbers):

- Built an end-to-end streaming lakehouse (Kafka → Spark Structured Streaming → Iceberg → dbt → Airflow) processing \_\_ events/sec at p99 \_\_ ms end-to-end latency.
- Cut lateness-query latency \_\_% via Iceberg partitioning and incremental dbt models.
- Added \_\_ dbt + Great Expectations quality gates that fail the pipeline on malformed feed data.
- Instrumented with Prometheus/Grafana and deployed on Kubernetes with health probes and an HPA.

**Interview-ready stories you'll have earned:** the stream-static join under late data; the throughput ceiling and its fix; schema evolution on a live feed; a K8s crash-loop you debugged.
