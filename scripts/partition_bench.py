"""Partitioning benchmark: the same lateness queries on an unpartitioned vs a partitioned copy.

Silver only holds a day or two, so the benchmark tables replicate silver.stop_delays across
DAYS service days (service_date and timestamps shifted by N days):
    bench.stop_delays_flat   unpartitioned
    bench.stop_delays_part   PARTITIONED BY (service_date, bucket(8, route_id))   (= silver's spec)
Both are compacted (rewrite_data_files) so file sizes are comparable, then each query runs
RUNS times per table (first run discarded as warm-up). Wall time comes from the driver; files
and bytes scanned come from the Iceberg scan metrics in Spark's SQL REST API.

    docker compose run --rm --no-deps -v ./scripts:/scripts spark python /scripts/partition_bench.py
Writes scripts/results/partition_bench.json. Drops the bench namespace at the end (--keep to keep).
"""
import json
import statistics
import sys
import time
import urllib.request

sys.path.insert(0, "/app")
from spark_session import build_spark  # noqa: E402

DAYS, RUNS = 30, 6
COLS = """service_date, route_id, direction_id, trip_id, vehicle_id, stop_sequence, stop_id, scheduled_stop_id,
          time_kind, scheduled_time, observed_time, delay_seconds, scheduled_hour_local,
          prediction_horizon_seconds, n_predictions, first_event_time, final_event_time, passed_at,
          finalized_by, trip_schedule_relationship, stop_schedule_relationship, schedule_matched,
          ingest_time, processed_time"""
SHIFTED = """date_sub(service_date, n) AS service_date, route_id, direction_id, trip_id, vehicle_id, stop_sequence,
          stop_id, scheduled_stop_id, time_kind,
          scheduled_time - make_interval(0,0,0,n) AS scheduled_time, observed_time - make_interval(0,0,0,n) AS observed_time,
          delay_seconds, scheduled_hour_local, prediction_horizon_seconds, n_predictions,
          first_event_time - make_interval(0,0,0,n) AS first_event_time, final_event_time - make_interval(0,0,0,n) AS final_event_time,
          passed_at - make_interval(0,0,0,n) AS passed_at, finalized_by, trip_schedule_relationship, stop_schedule_relationship,
          schedule_matched, ingest_time - make_interval(0,0,0,n) AS ingest_time,
          processed_time - make_interval(0,0,0,n) AS processed_time"""


def ui_get(spark, path: str):
    base = spark.sparkContext.uiWebUrl.rstrip("/")
    app = spark.sparkContext.applicationId
    return json.load(urllib.request.urlopen(f"{base}/api/v1/applications/{app}{path}", timeout=30))


def scan_metrics(spark) -> dict:
    """Iceberg BatchScan metrics of the most recent SQL execution."""
    execs = ui_get(spark, "/sql?details=true&planDescription=false&length=200")
    last = max(execs, key=lambda e: e["id"])
    found = {}
    for node in last.get("nodes", []):
        if "BatchScan" in node.get("nodeName", "") or "Scan" in node.get("nodeName", ""):
            for m in node.get("metrics", []):
                found[m["name"]] = m["value"]
    return found


def timed(spark, sql: str) -> tuple[float, dict]:
    t0 = time.perf_counter()
    spark.sql(sql).collect()
    wall = time.perf_counter() - t0
    time.sleep(0.5)  # let the listener publish the execution
    return wall, scan_metrics(spark)


def main() -> None:
    keep = "--keep" in sys.argv
    spark = build_spark("partition_bench", {"spark.driver.memory": "2g", "spark.sql.adaptive.enabled": "true"})
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bench")
    for t in ("stop_delays_flat", "stop_delays_part"):
        spark.sql(f"DROP TABLE IF EXISTS bench.{t} PURGE")

    src = f"SELECT {SHIFTED} FROM silver.stop_delays CROSS JOIN (SELECT CAST(id AS INT) AS n FROM range({DAYS}))"
    t0 = time.perf_counter()
    spark.sql(f"CREATE TABLE bench.stop_delays_flat USING iceberg "
              f"TBLPROPERTIES ('write.parquet.compression-codec'='zstd') AS {src}")
    spark.sql(f"CREATE TABLE bench.stop_delays_part USING iceberg PARTITIONED BY (service_date, bucket(8, route_id)) "
              f"TBLPROPERTIES ('write.parquet.compression-codec'='zstd', 'write.distribution-mode'='hash') AS {src}")
    for t in ("stop_delays_flat", "stop_delays_part"):
        spark.sql(f"CALL lakehouse.system.rewrite_data_files(table => 'bench.{t}', "
                  "options => map('target-file-size-bytes', '134217728', 'min-input-files', '2'))").collect()
    build_s = time.perf_counter() - t0

    info = {}
    for t in ("stop_delays_flat", "stop_delays_part"):
        r = spark.sql(f"SELECT count(*) AS files, sum(file_size_in_bytes) AS bytes, sum(record_count) AS rows "
                      f"FROM bench.{t}.files").first()
        info[t] = {"files": r.files, "bytes": r.bytes, "rows": r.rows}
    max_date = spark.sql("SELECT max(service_date) AS d FROM bench.stop_delays_part").first().d
    busiest = spark.sql("SELECT route_id FROM silver.stop_delays WHERE finalized_by='passed' "
                        "GROUP BY 1 ORDER BY count(*) DESC LIMIT 1").first().route_id
    queries = {
        "one_route_7d_by_hour": (
            "SELECT scheduled_hour_local, count(*) n, avg(delay_seconds) avg_d, percentile_approx(delay_seconds, 0.9) p90 "
            "FROM {t} WHERE route_id = '" + busiest + "' AND service_date >= date_sub(DATE'" + str(max_date) + "', 6) "
            "AND finalized_by = 'passed' GROUP BY 1"),
        "all_routes_7d": (
            "SELECT route_id, count(*) n, avg(delay_seconds) avg_d, percentile_approx(delay_seconds, 0.9) p90 "
            "FROM {t} WHERE service_date >= date_sub(DATE'" + str(max_date) + "', 6) AND finalized_by = 'passed' "
            "GROUP BY 1"),
        "one_route_full_history": (
            "SELECT service_date, avg(delay_seconds) FROM {t} WHERE route_id = '" + busiest + "' "
            "AND finalized_by = 'passed' GROUP BY 1"),
    }
    results = {}
    for qname, template in queries.items():
        results[qname] = {}
        for t in ("stop_delays_flat", "stop_delays_part"):
            walls, metrics = [], {}
            for i in range(RUNS):
                wall, metrics = timed(spark, template.format(t=f"bench.{t}"))
                if i > 0:
                    walls.append(wall)
            results[qname][t] = {"median_s": round(statistics.median(walls), 3), "runs_s": [round(w, 3) for w in walls],
                                 "scan_metrics": metrics}
        flat, part = results[qname]["stop_delays_flat"]["median_s"], results[qname]["stop_delays_part"]["median_s"]
        results[qname]["speedup_x"] = round(flat / part, 2)
        results[qname]["latency_reduction_pct"] = round(100 * (1 - part / flat), 1)
        print(qname, results[qname]["speedup_x"], "x", flush=True)

    out = {"days": DAYS, "runs_per_query": RUNS - 1, "build_seconds": round(build_s, 1), "busiest_route": busiest,
           "max_service_date": str(max_date), "tables": info, "queries": results}
    print(json.dumps(out, indent=2, default=str))
    with open("/scripts/results/partition_bench.json", "w") as f:
        json.dump(out, f, indent=2, default=str)
    if not keep:
        for t in ("stop_delays_flat", "stop_delays_part"):
            spark.sql(f"DROP TABLE IF EXISTS bench.{t} PURGE")
        spark.sql("DROP NAMESPACE IF EXISTS bench")
    spark.stop()


if __name__ == "__main__":
    main()
