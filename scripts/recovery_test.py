"""Recovery test: stop a streaming job, restart it, measure how long it takes to catch up.

    python scripts/recovery_test.py bronze [--down 300]   # docker compose service "spark"
    python scripts/recovery_test.py silver [--down 300]   # docker compose service "spark-silver"

bronze: watches Kafka consumer lag (sum of kafka_consumer_lag_messages).
silver: watches silver freshness (now - newest processed_time).
"Caught up" = the metric is back at or below the max it reached during the 3 min before the stop
(its normal sawtooth). Runs on the host; stdlib only; Prometheus at localhost:9090.
Writes scripts/results/recovery_<job>.json.
"""
import argparse
import json
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

PROM = "http://localhost:9090/api/v1"
JOBS = {
    "bronze": {"service": "spark", "metric": "sum(kafka_consumer_lag_messages)",
               "batch": 'max(spark_query_last_batch_timestamp_seconds{query=~"bronze_.*"})'},
    "silver": {"service": "spark-silver", "metric": 'max(pipeline_freshness_seconds{table="silver.stop_delays"})',
               "batch": 'max(spark_query_last_batch_timestamp_seconds{query="silver_stop_delays"})'},
}


def instant(expr: str) -> float | None:
    q = urllib.parse.urlencode({"query": expr})
    res = json.load(urllib.request.urlopen(f"{PROM}/query?{q}", timeout=30))["data"]["result"]
    return float(res[0]["value"][1]) if res else None


def range_max(expr: str, start: float, end: float) -> float:
    q = urllib.parse.urlencode({"query": expr, "start": start, "end": end, "step": 15})
    res = json.load(urllib.request.urlopen(f"{PROM}/query_range?{q}", timeout=30))["data"]["result"]
    return max(float(v) for _, v in res[0]["values"]) if res else 0.0


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("job", choices=JOBS)
    ap.add_argument("--down", type=int, default=300)
    ap.add_argument("--timeout", type=int, default=1800)
    args = ap.parse_args()
    job = JOBS[args.job]

    now = time.time()
    baseline_max = range_max(job["metric"], now - 180, now)
    print(f"baseline (max over last 3 min): {baseline_max:.0f}", flush=True)

    t_stop = time.time()
    compose("stop", job["service"])
    print(f"stopped {job['service']}; down for {args.down}s", flush=True)
    time.sleep(args.down)
    metric_at_restart = instant(job["metric"])
    t_start = time.time()
    compose("start", job["service"])
    print(f"started {job['service']}; metric at restart = {metric_at_restart}", flush=True)

    peak, first_batch_s, caught_up_s, trace = metric_at_restart or 0, None, None, []
    while time.time() - t_start < args.timeout:
        time.sleep(10)
        elapsed = time.time() - t_start
        value, last_batch = instant(job["metric"]), instant(job["batch"])
        trace.append({"t": round(elapsed), "metric": value})
        if value is not None:
            peak = max(peak, value)
        if first_batch_s is None and last_batch and last_batch > t_start:
            first_batch_s = round(last_batch - t_start, 1)
        if first_batch_s is not None and value is not None and value <= baseline_max:
            caught_up_s = round(elapsed)
            break
        print(f"  +{elapsed:4.0f}s metric={value} first_batch={first_batch_s}", flush=True)

    result = {"job": args.job, "service": job["service"], "down_seconds": args.down,
              "baseline_max": baseline_max, "metric_at_restart": metric_at_restart, "peak": peak,
              "restart_to_first_batch_s": first_batch_s, "restart_to_caught_up_s": caught_up_s,
              "outage_total_s": round(time.time() - t_stop), "trace": trace}
    out = Path(__file__).parent / "results" / f"recovery_{args.job}.json"
    out.write_text(json.dumps(result, indent=2))
    print(json.dumps({k: v for k, v in result.items() if k != "trace"}))


if __name__ == "__main__":
    main()
