# Break it on purpose: probe misconfiguration drills (kind, 2026-10-21 UTC)

Cluster: kind v0.33.0, one node, namespace `transit-lakehouse`. Target: the `poller` Deployment (1 replica, `Recreate`). Raw command output is in `breakit-raw-1-readiness.txt`, `breakit-raw-2-liveness.txt` and `breakit-raw-3-liveness-port.txt`.

## Drill 1: bad readiness probe → pod runs but never receives traffic

| Time (UTC) | What happened |
|---|---|
| 02:09:36 | `kubectl patch` changed the readiness probe port **8000 → 8001** |
| 02:10:36 | `kubectl get pods` showed `poller-c77df6658-z44fb  0/1  Running  0` |
| 02:10:36 | `kubectl get endpoints poller` showed **no endpoints** |
| 02:10:37 | `kubectl describe pod` event: `Readiness probe failed: Get "http://10.244.0.18:8001/metrics": dial tcp ...:8001: connect: connection refused` (x5 over 48 s) |
| 02:10:37 | `kubectl logs` showed the app working normally: `vehicle_positions: 376 entities -> 271 produced` |
| 02:10:37 | `curl localhost:18000/metrics` (NodePort) failed: no ready endpoint behind the Service |
| 02:10:38 | **Fix:** `kubectl rollout undo deployment/poller` |
| 02:10:51 | `1/1 Running`, endpoint `10.244.0.19:8000`, NodePort returned HTTP 200 |

**Diagnosis.** The status `Running` with `0/1` READY means the container is alive but failing readiness. The `describe` events named the exact URL and port being probed. The logs proved the app was healthy, so the probe definition was wrong, not the app.

**Impact.** Because the Deployment uses `Recreate`, the old pod was killed before the new one became Ready, so the Service had **no** backends. Anything reaching the poller through its Service (Prometheus) went dark. The poller kept producing to Kafka the whole time, because readiness only gates *incoming* traffic.

## Drill 2a: bad liveness *path* → nothing happens (a trap)

| Time (UTC) | What happened |
|---|---|
| 02:11:03 | Patched the liveness path **`/metrics` → `/healthz`** |
| 02:11:03 – 02:18:46 | `1/1 Running`, **0 restarts** for 7.5 minutes |
| check | `GET poller:8000/healthz -> 200` |

**Finding.** The poller's metrics endpoint is `prometheus_client.start_http_server`, which serves the metrics page for **every** path. A mistyped probe path therefore still passes. The probe looked configured but proved nothing about the path. A real `/healthz` that checks the poll loop (for example, last successful poll less than 2 min ago) would make liveness meaningful.

## Drill 2b: bad liveness *port* → CrashLoopBackOff

| Time (UTC) | What happened |
|---|---|
| 02:20:20 | Patched the liveness port **8000 → 8001** |
| 02:21:52 | `RESTARTS 1`: the first kill, after `initialDelaySeconds 30 + 3 failures × 20 s` |
| 02:23:09 → 02:28:33 | Restarts 2 to 6, each followed by a longer back-off |
| 02:29:50 | **`0/1  CrashLoopBackOff  6`** |
| 02:29:51 | `describe`: `Liveness probe failed: Get "http://10.244.0.23:8001/metrics": ... connection refused` (x21); `Container poller failed liveness probe, will be restarted` (x5); `Back-off restarting failed container poller` |
| 02:29:51 | `Last State: Terminated, Reason: Completed, Exit Code: 0` |
| 02:29:51 | `logs --previous`: normal polling right up to `INFO MainThread: shutting down` |
| 02:29:51 | **Fix:** `kubectl rollout undo deployment/poller` |
| 02:31:35 | `1/1 Running`, 0 restarts after 90 s; spec back to liveness 8000 and readiness 8000 |

**Diagnosis.** "CrashLoopBackOff" sounds like the app crashed, but **exit code 0 / Completed** plus a clean `shutting down` in `--previous` logs means it didn't. The poller handles SIGTERM gracefully, so every kubelet kill looked like a normal exit. The `Killing ... failed liveness probe` events show who was killing it.

**Side effect worth knowing.** Each restart lost the poller's in-memory "unchanged entity" fingerprints, so the first poll after each restart re-produced the full snapshot (about 1,400 records). Bronze recorded those faithfully, and silver's dedup and its insert-only MERGE kept silver free of duplicates (0 duplicate stop visits after the drill).

## Takeaways
1. A readiness failure stops traffic; a liveness failure restarts the container. Diagnose by checking READY versus RESTARTS, then the `describe` events, then `logs --previous`.
2. With `Recreate` and a single replica, any bad rollout means a full outage. It's the right trade-off for a single-writer job, but use `kubectl rollout status` and undo quickly.
3. A probe against an endpoint that answers every path is not a health check.
4. `kubectl rollout undo` fixes the live object, but the manifest in git is the source of truth. Re-applying the file is the durable fix, and kubectl warns that `last-applied-configuration` is not updated by undo.
