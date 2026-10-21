# Kubernetes deployment (kind)

This deploys the streaming core of the pipeline to a local single-node **kind** cluster:

**poller → Redpanda → Spark bronze → Spark silver**, plus the Spark Thrift server, MinIO, and the Postgres Iceberg catalog.

Every service has a readiness probe; liveness and startup probes are added where they make sense. There's one HPA, and Prometheus and Grafana are an optional addon. Airflow, dbt, and Great Expectations stay in docker compose.

| File | What it creates |
|---|---|
| `kind-cluster.yaml` | 1-node cluster; host ports 18000 (poller metrics), 14040 (bronze Spark UI), 13000 (Grafana) |
| `00-namespace.yaml` | namespace `transit-lakehouse` |
| `01-config.yaml` | ConfigMap `pipeline-config` (the same values as `.env.example`) + Secret `lakehouse-creds` (local default credentials) |
| `10-redpanda.yaml` | StatefulSet + PVC; Service `redpanda:9092`, which is also the advertised address |
| `11-minio.yaml` | Deployment + PVC + Service `minio:9000` |
| `12-minio-init-job.yaml` | Job that creates the `lakehouse` bucket (idempotent) |
| `13-iceberg-catalog-db.yaml` | Postgres StatefulSet + PVC + Service `iceberg-catalog-db:5432` |
| `20-poller.yaml` | poller Deployment (1 replica, Recreate) + NodePort for `/metrics` |
| `21-spark-bronze.yaml` | bronze job (1 replica, Recreate) + Service `spark` (UI, NodePort) |
| `22-spark-silver.yaml` | silver job (1 replica, Recreate) + Service `spark-silver` |
| `23-spark-thrift.yaml` | Thrift JDBC server Deployment + Service `spark-thrift:10000` |
| `30-hpa-spark-thrift.yaml` | the HPA (CPU 70%, 1→2 replicas) |
| `optional/50-prometheus.yaml`, `optional/51-grafana.yaml` | observability addon, built from `observability/` configs |

Service names match docker compose, so every image and config works unchanged.

## Resources

All values are memory; CPU requests total about 2.6 cores and there are no CPU limits.

| Workload | Request | Limit | Measured in compose |
|---|---|---|---|
| redpanda | 512Mi | 1.5Gi | 0.3–0.45Gi |
| minio | 256Mi | 1Gi | 0.8Gi |
| iceberg-catalog-db | 128Mi | 512Mi | 0.1Gi |
| poller | 256Mi | 1Gi | 70Mi steady, ~600Mi during the daily schedule parse |
| spark-bronze | 1.25Gi | 2Gi | 1.7Gi (with a 2g heap; 1g here) |
| spark-silver | 2.5Gi | 5Gi | 4.3Gi (4.17Gi peak on kind) |
| spark-thrift | 1.5Gi | 3Gi | 2.7Gi |
| **Total** | **≈ 6.4Gi** | **≈ 14Gi** | |
| + 2nd thrift replica (HPA) | +1.5Gi | +3Gi | |
| + optional Prometheus and Grafana | +224Mi | +768Mi | |

## Why the HPA is on spark-thrift only

- **spark-thrift is stateless.** Each replica is an independent query engine over the same Iceberg catalog and MinIO, so more replicas simply mean more JDBC/query capacity.
- **Not the poller.** Every replica would poll the same MBTA feed and produce every event again, creating duplicates in Kafka. Ingest volume is set by the feed, not by replica count.
- **Not bronze or silver.** A Spark Structured Streaming checkpoint is **single-writer**. Two replicas on one checkpoint would race on Kafka offsets and the RocksDB state, and corrupt it. These jobs scale *vertically* (cores, memory, more Kafka partitions) and use `strategy: Recreate`, so even a rollout never runs two at once.

## Deploy

Prerequisites:
- Docker Desktop
- `kind` v0.33+ (`winget install Kubernetes.kind`)
- `kubectl` (ships with Docker Desktop)
- the locally built images (`docker compose build`)

Stop the compose stack first so the two don't compete for memory: `docker compose stop`.

```bash
# 1. cluster
kind create cluster --config k8s/kind-cluster.yaml

# 2. images: load the two LOCAL images one at a time. Don't kind-load pulled multi-arch images
#    (redpanda, minio, postgres): with Docker Desktop's containerd image store, kind's
#    `ctr import --all-platforms` fails ("content digest ... not found") and one bad image
#    aborts the whole batch. The node pulls those public images itself (IfNotPresent).
kind load docker-image transit-lakehouse/poller:latest --name transit-lakehouse
kind load docker-image transit-lakehouse/spark:latest  --name transit-lakehouse

# 3. metrics-server (needed by the HPA); kind's kubelet certs need --kubelet-insecure-tls
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.7.2/components.yaml
kubectl -n kube-system patch deployment metrics-server --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'

# 4. infra, then wait for it
kubectl apply -f k8s/00-namespace.yaml -f k8s/01-config.yaml
kubectl apply -f k8s/10-redpanda.yaml -f k8s/11-minio.yaml -f k8s/13-iceberg-catalog-db.yaml
kubectl -n transit-lakehouse rollout status statefulset/redpanda --timeout=180s
kubectl -n transit-lakehouse rollout status deployment/minio --timeout=180s
kubectl -n transit-lakehouse rollout status statefulset/iceberg-catalog-db --timeout=180s
kubectl apply -f k8s/12-minio-init-job.yaml
kubectl -n transit-lakehouse wait --for=condition=complete job/minio-init --timeout=180s

# 5. pipeline (init containers also wait for their dependencies)
kubectl apply -f k8s/20-poller.yaml -f k8s/21-spark-bronze.yaml -f k8s/22-spark-silver.yaml \
              -f k8s/23-spark-thrift.yaml -f k8s/30-hpa-spark-thrift.yaml
kubectl -n transit-lakehouse wait --for=condition=available deployment --all --timeout=600s

# 6. optional: Prometheus + Grafana from the repo's observability/ configs
kubectl -n transit-lakehouse create configmap prometheus-config \
  --from-file=observability/prometheus.yml --from-file=observability/alerts.yml
kubectl -n transit-lakehouse create configmap grafana-datasources \
  --from-file=observability/grafana/provisioning/datasources/
kubectl -n transit-lakehouse create configmap grafana-dashboard-provider \
  --from-file=observability/grafana/provisioning/dashboards/
kubectl -n transit-lakehouse create configmap grafana-dashboards \
  --from-file=observability/grafana/dashboards/
kubectl apply -f k8s/optional/          # the node pulls prom/prometheus and grafana/grafana itself
```

## Check it

Query from inside the cluster through the Thrift server, not `python sql.py` in the bronze pod. sql.py starts a second JVM inside the bronze container and can push it past its 2Gi limit.

```bash
kubectl -n transit-lakehouse exec deploy/spark-thrift -c thrift -- \
  /usr/local/lib/python3.11/site-packages/pyspark/bin/beeline -u jdbc:hive2://localhost:10000 -n me \
  -e "SELECT count(*) FROM silver.stop_delays"
kubectl -n transit-lakehouse get pods -o wide          # everything Running and READY 1/1
kubectl -n transit-lakehouse get hpa spark-thrift      # TARGETS shows a CPU %, not <unknown>
kubectl -n transit-lakehouse logs deploy/poller --tail=5          # "... produced, ... unchanged"
kubectl -n transit-lakehouse logs deploy/spark-bronze --tail=5    # bronze batches
kubectl -n transit-lakehouse logs deploy/spark-silver --tail=5    # silver batches + watermark
curl -s localhost:18000/metrics | grep gtfs_events_produced_total
```

The bronze Spark UI is at http://localhost:14040, and Grafana (if installed) at http://localhost:13000.

## Break it on purpose: probe misconfiguration drill

**1. Bad readiness probe.** The pod runs, but it never becomes Ready, so its Service has no endpoints:

```bash
kubectl -n transit-lakehouse patch deployment poller --type=json \
  -p '[{"op":"replace","path":"/spec/template/spec/containers/0/readinessProbe/httpGet/port","value":8001}]'
kubectl -n transit-lakehouse get pods -l app.kubernetes.io/name=poller -w   # Running, READY 0/1
kubectl -n transit-lakehouse describe pod -l app.kubernetes.io/name=poller   # Events: Readiness probe failed: ... connection refused
kubectl -n transit-lakehouse get endpoints poller                            # <none>
```

The poller still produces; logs show batches. Only the Service sees it as unready. With `Recreate`, the old pod was killed first, so there's no ready poller at all.

**2. Bad liveness probe.** The kubelet kills the container repeatedly, which ends in **CrashLoopBackOff** after about 9 minutes and 6 restarts.

Break the **port**, not the path. The poller's `prometheus_client` server answers *every* path with 200, so a wrong path never fails. See `results/breakit.md`.

```bash
kubectl -n transit-lakehouse patch deployment poller --type=json \
  -p '[{"op":"replace","path":"/spec/template/spec/containers/0/livenessProbe/httpGet/port","value":8001}]'
kubectl -n transit-lakehouse get pods -l app.kubernetes.io/name=poller -w   # RESTARTS climbing -> CrashLoopBackOff
kubectl -n transit-lakehouse describe pod -l app.kubernetes.io/name=poller   # Liveness probe failed ... :8001 connection refused
kubectl -n transit-lakehouse logs deploy/poller --previous                   # the app was healthy, it was just killed
```

Expect `Last State: Terminated, Reason: Completed, Exit Code: 0`. The poller shuts down cleanly on SIGTERM, so the "crash loop" is the kubelet killing a healthy app.

**Fix: roll back to the last good spec.**

```bash
kubectl -n transit-lakehouse rollout undo deployment/poller          # repeat until the probes are back to /metrics:8000
kubectl -n transit-lakehouse rollout status deployment/poller
```

You can also re-run `kubectl apply -f k8s/20-poller.yaml`.

## Teardown

```bash
kind delete cluster --name transit-lakehouse
docker compose start        # bring the compose stack back
```

## Measured on kind (2026-10-21)

These are recorded in `results/deploy-initial.txt`, `results/deploy-final.txt`, `results/hpa-scale.txt` and `results/breakit.md`.

- **First apply:** all pods Running/Ready with 0 restarts. Infra took about 1 min and the pipeline about 1 more minute. The poller produced and bronze committed within about 1 min; silver's first rows came about 15 min later (the watermark).
- **After ~25 min:** bronze had 33.5k vehicle rows and 104k trip-update rows; silver had 2,596 stop visits with 0 duplicates and wrote its last row 11 s before the query.
- **Peak memory:** the node used about 10.7Gi (72%). spark-silver reached **4.17Gi of its 4.5Gi limit**, too close for comfort; the manifest now sets 5Gi.
- **The HPA scales,** but CPU is a noisy signal for Spark. A *single* analytical query pushes CPU far past the 500m request (574–917%), so one ad-hoc query triggered 1 → 2. For a real deployment, use a higher CPU request (2 cores), a scale-up stabilization window, or a custom metric such as open JDBC sessions.

## Things to watch on kind

- **Image size.** `transit-lakehouse/spark:latest` is about 3 GB (1.2 GB compressed in the node), so `kind load` takes a few minutes and uses node disk.
- **Memory.** The limits add up to about 13.5Gi on one node. If the node is short, silver gets OOMKilled first; lower `SPARK_DRIVER_MEMORY` and its limit.
- **Ivy cache.** The Spark images resolve their jar packages from the Ivy cache baked into the image. If a pod logs Maven downloads, the node needs internet (kind has it by default).
- **Startup order.** If bronze hasn't created `bronze.trip_updates` yet when silver starts, silver exits once and restarts.
