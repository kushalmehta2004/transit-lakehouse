"""Pipeline exporter: the lag/freshness metrics no single service can report. Read-only.

    kafka_topic_high_watermark{topic,partition}         latest offset per partition (DLQ depth for *.dlq)
    kafka_consumer_lag_messages{query,topic,partition}  Kafka high watermark - offset committed in the
                                                        Spark checkpoint (Spark keeps Kafka offsets in
                                                        its checkpoint, not in a consumer group)
    spark_query_last_batch_timestamp_seconds{query}     trigger time of the last committed micro-batch
    spark_query_watermark_seconds{query}                event-time watermark of that batch
    pipeline_e2e_latency_seconds{stage,quantile}        ingest_time -> landed, for rows landed recently (silver: passed stops)
                                                        (stage=bronze: bronze_ingested_at, silver: processed_time)
    pipeline_freshness_seconds{table}                   now - newest ingest_time (bronze) / processed_time (silver)
    quality_gate_success / quality_gate_last_run_timestamp_seconds   from quality/results/latest.json
    exporter_errors_total{check}

Served on :9200/metrics.
"""
import json
import logging
import os
import re
import threading
import time
from pathlib import Path

import pyarrow.fs as pafs
from confluent_kafka import Consumer, TopicPartition
from prometheus_client import Counter, Gauge, start_http_server
from pyhive import hive

log = logging.getLogger("exporter")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
BUCKET = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")
KAFKA_QUERIES = ["bronze_vehicle_positions", "bronze_trip_updates"]  # checkpoints with Kafka offsets
ALL_QUERIES = KAFKA_QUERIES + ["silver_stop_delays"]
QUALITY_RESULT = Path(os.environ.get("QUALITY_RESULT_PATH", "/quality/results/latest.json"))

HIGH_WATERMARK = Gauge("kafka_topic_high_watermark", "Latest offset (end of log)", ["topic", "partition"])
CONSUMER_LAG = Gauge("kafka_consumer_lag_messages", "Messages not yet committed by the Spark query",
                     ["query", "topic", "partition"])
LAST_BATCH = Gauge("spark_query_last_batch_timestamp_seconds", "Trigger time of last committed batch", ["query"])
WATERMARK = Gauge("spark_query_watermark_seconds", "Event-time watermark of last committed batch", ["query"])
E2E = Gauge("pipeline_e2e_latency_seconds", "ingest_time -> written, rows written recently",
            ["stage", "quantile"])
FRESHNESS = Gauge("pipeline_freshness_seconds", "Seconds since the newest data in the table", ["table"])
QUALITY_OK = Gauge("quality_gate_success", "1 if the latest Great Expectations checkpoint passed")
QUALITY_TS = Gauge("quality_gate_last_run_timestamp_seconds", "When the latest GE checkpoint ran")
ERRORS = Counter("exporter_errors_total", "Failed checks", ["check"])


# ---------------------------------------------------------------- kafka + checkpoints

def s3() -> pafs.S3FileSystem:
    scheme, host = os.environ.get("S3_ENDPOINT", "http://minio:9000").split("://", 1)
    return pafs.S3FileSystem(access_key=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
                             secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
                             endpoint_override=host, scheme=scheme, region="us-east-1")


def last_committed_offsets(fs: pafs.S3FileSystem, query: str) -> tuple[dict, dict] | None:
    """(batch metadata, {topic: {partition: offset}}) of the newest batch in commits/."""
    base = f"{BUCKET}/checkpoints/{query}"
    try:
        commits = fs.get_file_info(pafs.FileSelector(f"{base}/commits"))
    except FileNotFoundError:
        return None
    batch_ids = [int(Path(f.path).name) for f in commits if re.fullmatch(r"\d+", Path(f.path).name)]
    if not batch_ids:
        return None
    with fs.open_input_stream(f"{base}/offsets/{max(batch_ids)}") as f:
        lines = f.read().decode().splitlines()
    # line 0: version, line 1: batch metadata, line 2+: one offset JSON per source
    return json.loads(lines[1]), json.loads(lines[2]) if len(lines) > 2 else {}


def check_kafka(consumer: Consumer, fs: pafs.S3FileSystem) -> None:
    topics = [t for t in consumer.list_topics(timeout=10).topics if not t.startswith("_")]
    end = {}
    for topic in topics:
        for p in consumer.list_topics(topic, timeout=10).topics[topic].partitions:
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            end[(topic, p)] = high
            HIGH_WATERMARK.labels(topic, str(p)).set(high)
    for query in ALL_QUERIES:
        state = last_committed_offsets(fs, query)
        if state is None:
            continue
        meta, offsets = state
        LAST_BATCH.labels(query).set(meta.get("batchTimestampMs", 0) / 1000)
        if meta.get("batchWatermarkMs", 0) > 0:  # bronze queries have no watermark (always 0)
            WATERMARK.labels(query).set(meta["batchWatermarkMs"] / 1000)
        if query in KAFKA_QUERIES:
            for topic, parts in offsets.items():
                for p, committed in parts.items():
                    high = end.get((topic, int(p)))
                    if high is not None:
                        CONSUMER_LAG.labels(query, topic, p).set(max(high - committed, 0))


# ---------------------------------------------------------------- freshness via Thrift

E2E_SQL = {
    "bronze": "SELECT percentile_approx(unix_millis(bronze_ingested_at) - unix_millis(ingest_time), array(0.5, 0.99)) "
              "FROM bronze.vehicle_positions WHERE bronze_ingested_at >= current_timestamp() - INTERVAL 2 MINUTES",
    # passed stops only: 'expired' stops (trips that vanished from the feed) are released
    # predicted time + 10 min after their last update by design, which would swamp p99
    "silver": "SELECT percentile_approx(unix_millis(processed_time) - unix_millis(ingest_time), array(0.5, 0.99)) "
              "FROM silver.stop_delays WHERE processed_time >= current_timestamp() - INTERVAL 5 MINUTES "
              "AND finalized_by = 'passed'",
}
FRESHNESS_SQL = {
    "bronze.vehicle_positions": "SELECT unix_timestamp() - unix_timestamp(max(ingest_time)) FROM bronze.vehicle_positions "
                                "WHERE bronze_ingested_at >= current_timestamp() - INTERVAL 3 HOURS",
    "silver.stop_delays": "SELECT unix_timestamp() - unix_timestamp(max(processed_time)) FROM silver.stop_delays "
                          "WHERE processed_time >= current_timestamp() - INTERVAL 3 HOURS",
}


class Thrift:
    def __init__(self):
        self.conn = None

    def _run(self, sql: str, fetch: bool):
        for attempt in (1, 2):
            try:
                if self.conn is None:
                    self.conn = hive.connect(host=os.environ.get("SPARK_THRIFT_HOST", "spark-thrift"),
                                             port=int(os.environ.get("SPARK_THRIFT_PORT", "10000")),
                                             username="exporter")
                cur = self.conn.cursor()
                cur.execute(sql)
                if not fetch:
                    return None
                row = cur.fetchone()
                return row[0] if row else None
            except Exception:
                self.conn = None  # reconnect once (thrift server restarted, idle timeout, ...)
                if attempt == 2:
                    raise

    def execute(self, sql: str) -> None:
        self._run(sql, fetch=False)

    def scalar(self, sql: str):
        return self._run(sql, fetch=True)


def check_freshness(thrift: Thrift) -> None:
    # The Thrift server's Iceberg catalog caches tables and only expires an entry after 30 s
    # WITHOUT access; tables polled constantly would otherwise be read at a stale snapshot.
    for table in ("bronze.vehicle_positions", "silver.stop_delays"):
        thrift.execute(f"REFRESH TABLE {table}")
    for stage, sql in E2E_SQL.items():
        value = thrift.scalar(sql)
        quantiles = json.loads(value) if isinstance(value, str) else value
        if quantiles and quantiles[0] is not None:
            E2E.labels(stage, "0.5").set(quantiles[0] / 1000)
            E2E.labels(stage, "0.99").set(quantiles[1] / 1000)
    for table, sql in FRESHNESS_SQL.items():
        age = thrift.scalar(sql)
        if age is not None:
            FRESHNESS.labels(table).set(float(age))
        else:  # nothing in 3 h: report the window as a floor rather than a stale value
            FRESHNESS.labels(table).set(3 * 3600)


def check_quality() -> None:
    if QUALITY_RESULT.exists():
        result = json.loads(QUALITY_RESULT.read_text())
        QUALITY_OK.set(1 if result.get("success") else 0)
        QUALITY_TS.set(QUALITY_RESULT.stat().st_mtime)


# ---------------------------------------------------------------- loop

def every(seconds: float, name: str, fn) -> None:
    def loop():
        while True:
            started = time.monotonic()
            try:
                fn()
            except Exception as e:
                ERRORS.labels(name).inc()
                log.warning("%s check failed: %s", name, e)
            time.sleep(max(1.0, seconds - (time.monotonic() - started)))
    threading.Thread(target=loop, name=name, daemon=True).start()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(threadName)s: %(message)s")
    start_http_server(int(os.environ.get("EXPORTER_PORT", "9200")))
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "pipeline-exporter",
                         "enable.auto.commit": False})
    fs, thrift = s3(), Thrift()
    every(15, "kafka", lambda: check_kafka(consumer, fs))
    every(30, "freshness", lambda: check_freshness(thrift))
    every(30, "quality", check_quality)
    log.info("exporter serving :9200/metrics")
    threading.Event().wait()


if __name__ == "__main__":
    main()
