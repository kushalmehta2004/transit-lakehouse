# Which Boston bus is late, and how would you even know?

*Building a streaming lakehouse on live MBTA data, and the bugs that only showed up once real data hit it.*

Every 15 seconds, Boston's transit agency publishes where its ~540 vehicles are and when it expects each of ~1,340 trips to reach each upcoming stop. The question I wanted to answer sounds simple: **which routes are chronically late, by how much, and at what time of day?** Answering it means ingesting that feed continuously, matching every prediction against the published timetable, and modeling the result. That's a real-time data engineering problem from end to end, so I built the whole thing: Kafka (Redpanda), Spark Structured Streaming, Apache Iceberg on MinIO, dbt, Great Expectations, Airflow, Prometheus and Grafana. It all comes up with one `docker compose up`, and I deployed it once to Kubernetes.

This post covers what was harder than expected.

## "Actual arrival" doesn't exist in the feed

The first surprise was that the feed contains no arrivals at all, only **predictions**. A trip update says "the bus will reach stop 5 at 2:03:25". When the bus passes stop 5, the stop simply disappears from the next update.

So "when did the bus actually arrive" has to be inferred: it's the **last prediction made before the stop vanished from the feed**. By that point the vehicle is seconds away, and the median gap between that prediction and the arrival turned out to be 0 s. To check it, I compared against an independent signal: GPS vehicle positions that report `STOPPED_AT` a stop. **99.6% of the inferred arrivals were within 60 seconds of the GPS.** That one validation query gave me more confidence than any unit test.

## The stream-static join, and the 15-minute question

Computing delay means joining each observed arrival to its scheduled time in the static GTFS timetable. Two details matter here:

- **Times past midnight.** About 5% of scheduled stop times are written as "25:10:00", meaning 1:10 AM the *next* day, still on the previous service day. Done naively, those produce the classic "10-hour delay" bug. The fix is the GTFS rule that a service day starts at "noon minus 12 hours" local time, which also handles daylight-saving days.
- **Late data.** Events don't arrive in order; in the real feed some showed up 12 minutes late. Suppose the stream finalizes stop 5 the moment it disappears. A straggling update from 30 seconds earlier then arrives and can't change the answer anymore.

I used a per-trip state machine (`applyInPandasWithState`) behind a **15-minute event-time watermark**. A passed stop is held until the watermark proves no older update can still arrive, and only then is it written. I tested it by replaying real data with 2-minute chunks reversed inside 10-minute windows. The output was **identical row for row**, and deliberately injected 20-minute-late events were dropped and counted.

The cost is visible on the dashboard: silver lands about **16 minutes** after the bus passes. That's the right trade for a historical question like "which routes are chronically late", and the wrong one for a "where's my bus" app. I sized the alerts around it: bronze must be fresh within 2 minutes, while silver only alerts well beyond its designed 16.

## The quality gates earned their keep

I added 24 dbt tests and a 15-expectation Great Expectations suite, wired into an Airflow DAG that turns red on any failure. I expected them to catch things I injected on purpose. They caught real bugs instead.

**Bug 1: a stop counted twice.** The uniqueness test failed on its first run. MBTA had announced that a trip would *skip* a stop, four hours ahead of time, and the trip then vanished from the feed for two hours. My tracker forgot finished trips after one hour, so when the trip reappeared, it emitted that stop again. I made two fixes:
- Remember trips until their service day ends.
- More importantly, write silver with an **insert-only MERGE** on `(service_date, trip_id, stop_sequence)`. "One row per stop visit" is now enforced by the table, not just by my state machine. Spark plans that MERGE as an anti-join plus append, so it costs almost nothing.

**Bug 2: a bad row that wouldn't leave.** In a drill I injected a 10-hour delay. The gates fired as designed, and I deleted the row, but the next run *still* failed. My incremental gold model used MERGE, and a MERGE only updates groups present in the new aggregate. The bad row had created its own group, so once the row was deleted, nothing ever recomputed that group. Switching to `insert_overwrite` on day partitions fixed it: each run replaces whole day partitions, so stale groups disappear.

## Finding the ceiling

The load test replays historical messages at increasing rates. At **2,000 events/s** everything was calm. At **5,000**, batches took 14 s against a 10 s trigger and Kafka lag grew by 900 messages per second.

The Spark UI showed the cause immediately: **one task per batch**. Back in phase 2, I had added `coalesce(1)` so each commit wrote a single file, a tidy-looking choice. But coalesce also collapses the *upstream* stages, so the whole job (Kafka read, JSON parse, Iceberg write) ran on one core out of twelve. Restoring one task per Kafka partition took throughput to **10,000 events/s sustained** at the same p99 of 9.9 s. The extra small files are absorbed by an hourly Iceberg compaction job. The next limit is the 6-partition Kafka topic.

Two more incidents showed up only under that load:
- **The Airflow UI died at boot,** because it couldn't start within gunicorn's 120-second timeout on a busy laptop. The scheduler kept running throughout.
- **The SQL server ran out of memory** while three Iceberg compactions ran at once in one 1.5 GB JVM. Worse, the dead JVM left the container "running", so Docker never restarted it. The fixes were to run compactions one at a time and have the wrapper process exit when Spark dies, so the restart policy works.

## Partitioning: faster, until it isn't

On a 30-day copy of silver, partitioning by `(service_date, bucket(8, route_id))` made "one route, last 7 days" **1.9× faster**, reading 4 MB instead of 51 MB. All routes over 7 days improved 1.44×. But a full-history query for one route got **11% slower**: when only the route bucket can prune, many small files cost more than they save. Partitioning is a bet on query shape, not a free win.

## Kubernetes, once

I deployed the core pipeline to a local `kind` cluster: Kafka, MinIO, Postgres, the poller, and three Spark jobs, each with readiness and liveness probes. It came up clean on the first apply. The real lessons were about what *not* to do:

- **No autoscaler on the poller or the streaming jobs.** The PRD suggested one, but a second poller re-reads the same feed and produces duplicates, and a second streaming writer corrupts its checkpoint. The autoscaler went on the stateless SQL server instead, and the streaming jobs use `Recreate` rollouts so two copies never overlap.
- **The break-it drill had a twist.** A wrong readiness port quietly removed the poller from its Service while it kept running, which is the most dangerous kind of failure because nothing looks broken. A wrong liveness *path* did nothing at all, because the metrics server answers 200 on every URL. Only a wrong liveness *port* produced the expected CrashLoopBackOff.

A probe only proves what it actually checks.

## What I'd do differently

- **Push uniqueness into the sink from day one.** State machines have edge cases; table constraints don't care.
- **Load-test before "tidying".** My one-file-per-commit choice looked like good hygiene and cost 4× throughput.
- **Measure correctness against an independent signal.** The GPS comparison was the most convincing test in the project.

The code, dashboards, and every number in this post are in the repo: <https://github.com/kushalmehta2004/transit-lakehouse>.
