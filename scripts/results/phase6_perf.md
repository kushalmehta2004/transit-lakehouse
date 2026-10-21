# Phase 6: performance measurements

**Measured:** 2026-10-20, on one Windows laptop (12 cores, Docker Desktop with 15.5 GB). The whole stack, the load generator and the benchmarks all shared that machine.

**Raw data:** `*_steps.jsonl`, `*_docker_stats.csv`, `replay_e2e_latency.txt`, `recovery_*.json` and `partition_bench.json`, all in this folder.

## 1. Load test: ingest → Kafka → bronze

**How it works.** `scripts/replay.py` re-publishes historical raw events to the same Kafka topics at a controlled rate:
- It splits the load 30/70 between vehicle_positions and trip_updates.
- It restamps `ingest_time` and tags `source = replay:<run>`.
- It leaves `event_time` unchanged, so silver drops these rows as late (they're outside its 15-minute watermark). The test therefore targets the **ingest → bronze** path, which is the one with a latency requirement.

**Breaking criterion.** Each rate holds for 3 minutes. A step is "broken" when Kafka consumer lag grows by more than 200 msg/s over the step.

### Baseline (`BRONZE_WRITE_PARTITIONS=1`, the Phase 2 default)

| Target ev/s | Achieved | Lag slope (msg/s) | Batch avg / max (10 s trigger) | Spark CPU avg / max | Bronze e2e p50 / p99 |
|---|---|---|---|---|---|
| 500 | 500 | 0 | 2.0 / 2.8 s | 64 / 213 % | 5.0 / 9.9 s |
| 1,000 | 994 | +4 | 2.2 / 2.7 s | 49 / 213 % | 5.0 / 9.9 s |
| 2,000 | 1,989 | 0 | 2.8 / 3.1 s | 66 / 193 % | 5.0 / 9.9 s |
| **5,000** | 4,990 | **+913 ❌** | **14.3 / 34.2 s** | 130 / 250 % | 6.2 / 31.9 s |

### Breaking point, cause, and fix

**Breaking point:** between 2k and 5k ev/s. At 5k, the average batch takes 14 s against a 10 s trigger, and lag grows without bound.

**Cause:** Spark used only about 1.3 of 12 cores. In the Spark UI, the stage that does all the work (Kafka read, JSON parse, Iceberg write) ran as **1 task** per batch. That's the Phase 2 `coalesce(1)`, which was added to write one file per commit. Coalesce merges the *upstream* partitions too, so the whole bronze job was single-threaded.

**Fix:** set `BRONZE_WRITE_PARTITIONS=6` in `.env`. The bronze job already reads this setting; it just wasn't passed through by compose. That gives one task per Kafka partition. The cost is up to 6 files per commit instead of 1, and the hourly `lakehouse_maintenance` compaction from Phase 5 absorbs them. No streaming code changed. **This setting is kept.**

### After the fix (`BRONZE_WRITE_PARTITIONS=6`)

| Target ev/s | Achieved | Lag slope (msg/s) | Batch avg / max | Spark CPU avg / max | Bronze e2e p50 / p99 |
|---|---|---|---|---|---|
| 5,000 | 4,999 | −22 ✅ | 12.0 / 19.6 s | 290 / 735 % | 5.9 / 17.2 s |
| **10,000** | 9,942 | **−1 ✅** | **5.4 / 7.9 s** | 162 / 656 % | **5.0 / 9.9 s** |
| 15,000 | 14,906 | +7,945 ❌ | 41.8 / 92.7 s | 347 / 627 % | 16.0 / 110.2 s |
| 20,000 | 19,861 | +9,734 ❌ | 33.3 / 87.1 s | 344 / 699 % | 11.6 / 88.1 s |

| | Before the fix | After the fix |
|---|---|---|
| Highest passing step | 2k ev/s | **10k ev/s** |
| First failing step | 5k ev/s | 15k ev/s |
| Bronze e2e at the highest sustained step | p50 5.0 s, p99 9.9 s | **p50 5.0 s, p99 9.9 s** |

That's **about 4× higher sustained throughput**. Latency at 10k ev/s is the same as under light load: it's set by the 10 s trigger.

The 40k step was not run, because the ramp stops at the first broken step: 20k (and then 15k, run separately) already broke.

**What limits it next.** CPU stays at about 3.5 of 12 cores, because 6 Kafka partitions allow at most 6 parallel tasks. The load generator, silver and MinIO also compete for the same laptop. The next levers are more Kafka partitions (12 or more) or `BRONZE_MAX_OFFSETS_PER_TRIGGER`, which bounds batch size so recovery stays smooth.

**Backlog drain.** After the 20k step, a backlog of 1.6M messages drained in about 80 s, in batches of up to 443k rows, with no OOM on the 1 GB driver.

**Cleanup.** About 11.4M replayed rows were deleted from bronze afterwards (`DELETE … WHERE source LIKE 'replay:%'`, with snapshot isolation enabled on both bronze tables). Silver still had 0 duplicate stop visits, and both streams kept running.

## 2. Recovery: stop a streaming job for 5 minutes, then restart it

| Job | Downtime | Peak backlog | Restart → first batch | Restart → caught up |
|---|---|---|---|---|
| **bronze** (`spark`) | 300 s | 40,280 messages of Kafka lag | **14.6 s** | **32 s** (lag back to its normal sawtooth, max 2k) |
| **silver** (`spark-silver`) | 300 s | 362 s of freshness lag | **18.8 s** | **101 s** (freshness < 60 s, watermark delay back to ~990 s) |

Both resumed from their checkpoints; offsets and RocksDB state carried over, with no reprocessing from zero. *Silver note:* the script's automatic baseline was inflated, because the bronze outage just before had starved silver of input. The silver row above is corrected from Prometheus history; see `recovery_silver.json` → `corrected`.

## 3. Iceberg partitioning benchmark

**Setup.** `silver.stop_delays` was replicated across **30 service days**, giving 5.97M rows. It was written two ways:
- `stop_delays_flat`: unpartitioned, 1 file, 51 MB
- `stop_delays_part`: partitioned by `(service_date, bucket(8, route_id))`, the same spec as silver, 248 files, 145 MB

Both were compacted first. Each query ran 6 times; the first run was discarded and the table shows the median.

| Query | Flat | Partitioned | Speedup | Files read (partitioned) | Data read: flat → partitioned |
|---|---|---|---|---|---|
| **One route (Orange Line), last 7 days, by hour** | 0.671 s | 0.353 s | **1.90× (−47%)** | 7 of 248 | 51 MB / 5.97M rows → **4 MB / 165k rows** |
| All routes, last 7 days | 1.073 s | 0.747 s | **1.44× (−30%)** | 56 of 248 | 51 MB → 29 MB |
| One route, full history | 0.397 s | 0.441 s | 0.90× (+11%) | 31 of 248 | 51 MB → 20 MB |

**Takeaways:**
- Date pruning gives the win. The PRD's core question ("which routes are late over the last N days") reads **92% fewer bytes** and runs **1.9× faster**.
- When only the route bucket can prune (full history), many small partition files cost more than they save. That's the classic over-partitioning trade-off.
- At 6M rows everything fits in memory, so the gap grows with data volume. Bytes read scales with data size; the fixed per-file cost does not.

## 4. Incidents found while load testing (and fixed)

| Incident | Cause | Fix |
|---|---|---|
| **The Airflow webserver died at startup.** The scheduler kept running DAGs (all scheduled runs stayed green), but the UI and API were down. | On the loaded laptop, gunicorn took longer than the default 120 s `web_server_master_timeout` to boot, and `airflow standalone` doesn't restart a webserver that gave up | `AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT=600` and `…WORKER_TIMEOUT=300` (`docker-compose.yml`, airflow block) |
| **The Spark Thrift server crashed with a Java heap OOM** at the 01:00 maintenance run, after the load test. Its SparkContext stopped, but the container kept "running" as unhealthy, so dbt, GE and the exporter couldn't connect. | Three `rewrite_data_files` compactions ran in parallel, each with Iceberg's default 5 concurrent file-group rewrites, inside one 1.5 GB-heap JVM, right after large bronze rewrites from the replay cleanup. The Python wrapper outlived the dead JVM, so Docker never restarted it. | The maintenance DAG now uses `max_active_tasks=1`; compaction uses `max-concurrent-file-group-rewrites=1`; `thrift_server.py` exits when its SparkContext stops, so `restart: unless-stopped` recovers it. Re-run: all 6 maintenance tasks green (bronze trip_updates 496 → 9 files, vehicle_positions 499 → 5, silver 1,882 → 42), and the pipeline DAG green. |

## Code and config changed in Phase 6

| File | Change |
|---|---|
| `scripts/replay.py`, `scripts/load_ramp.sh`, `scripts/measure.py`, `scripts/recovery_test.py`, `scripts/partition_bench.py` | new |
| `docker-compose.yml` (spark block) | passes through `BRONZE_WRITE_PARTITIONS` (default 1) and `BRONZE_MAX_OFFSETS_PER_TRIGGER` (default unlimited); the bronze job already read both |
| `docker-compose.yml` (airflow block) | webserver master and worker timeouts |
| `.env` and `.env.example` | `BRONZE_WRITE_PARTITIONS=6` **(kept)**; `.env.example` documents both settings |
| bronze tables | `delete.isolation-level = snapshot`, so maintenance deletes can commit alongside streaming appends |
| `orchestration/dags/maintenance_dag.py`, `orchestration/scripts/iceberg_maintenance.py` | compaction is serialized, as described above |
| `streaming/thrift_server.py` | exits when its SparkContext dies |

Airflow was stopped during the load and recovery tests to free about 1.6 GB of host RAM. Airflow isn't on the ingest → bronze path.

## Reproduce

```bash
# load ramp (writes scripts/results/<label>_steps.jsonl); stops at the first broken step
bash scripts/load_ramp.sh baseline "500 1000 2000 5000 10000 20000 40000" 180
# a single replay step
docker compose run --rm --no-deps -v ./scripts:/scripts poller python /scripts/replay.py --rate 10000 --seconds 180 --run-id test
# remove replayed rows afterwards
docker compose exec spark python sql.py "DELETE FROM bronze.trip_updates WHERE source LIKE 'replay:%'" "DELETE FROM bronze.vehicle_positions WHERE source LIKE 'replay:%'"
# recovery
python scripts/recovery_test.py bronze --down 300
python scripts/recovery_test.py silver --down 300
# partitioning benchmark (~2.5 min)
docker compose run --rm --no-deps -v ./scripts:/scripts spark python /scripts/partition_bench.py
```
