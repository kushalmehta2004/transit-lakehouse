"""Summarize a load-test step window from Prometheus (+ an optional docker-stats CSV).

    python scripts/measure.py <start_epoch> <end_epoch> [stats.csv]
Prints JSON: consumer-lag slope (msg/s, least squares) and max, bronze micro-batch duration
avg/max vs the 10 s trigger, bronze processing rate, and spark container CPU / memory.
Runs on the host (stdlib only) against http://localhost:9090.
"""
import csv
import json
import sys
import urllib.parse
import urllib.request

PROM = "http://localhost:9090/api/v1/query_range"


def series(expr: str, start: float, end: float, step: int = 15) -> list[tuple[float, float]]:
    q = urllib.parse.urlencode({"query": expr, "start": start, "end": end, "step": step})
    data = json.load(urllib.request.urlopen(f"{PROM}?{q}", timeout=30))["data"]["result"]
    return [(float(t), float(v)) for t, v in data[0]["values"]] if data else []


def slope(points: list[tuple[float, float]]) -> float:
    if len(points) < 2:
        return 0.0
    n = len(points)
    mx = sum(t for t, _ in points) / n
    my = sum(v for _, v in points) / n
    den = sum((t - mx) ** 2 for t, _ in points)
    return sum((t - mx) * (v - my) for t, v in points) / den if den else 0.0


def avg(points):
    return sum(v for _, v in points) / len(points) if points else None


def main() -> None:
    start, end = float(sys.argv[1]), float(sys.argv[2])
    settle = start + 30  # skip the first 30 s of ramp-up
    lag = series("sum(kafka_consumer_lag_messages)", settle, end)
    batch = series('max(spark_streaming_latency{app="bronze_job"})', settle, end)
    rate = series('sum(spark_streaming_processingRate_total{app="bronze_job"})', settle, end)
    inrate = series('sum(rate(kafka_topic_high_watermark{topic=~".*\\\\.raw"}[1m]))', settle, end)
    out = {
        "lag_slope_msg_per_s": round(slope(lag), 1),
        "lag_max": max((v for _, v in lag), default=None),
        "lag_end": lag[-1][1] if lag else None,
        "batch_ms_avg": round(avg(batch) or 0),
        "batch_ms_max": max((v for _, v in batch), default=None),
        "bronze_processing_rate_avg": round(avg(rate) or 0),
        "kafka_ingest_rate_avg": round(avg(inrate) or 0),
    }
    if len(sys.argv) > 3:
        rows = [r for r in csv.DictReader(open(sys.argv[3])) if start <= float(r["ts"]) <= end and r["name"] == "spark"]
        if rows:
            cpu = [float(r["cpu"].rstrip("%")) for r in rows]
            mem = [r["mem"].split("/")[0].strip() for r in rows]
            out["spark_cpu_pct_avg"] = round(sum(cpu) / len(cpu))
            out["spark_cpu_pct_max"] = round(max(cpu))
            out["spark_mem_last"] = mem[-1]
    print(json.dumps(out))


if __name__ == "__main__":
    main()
