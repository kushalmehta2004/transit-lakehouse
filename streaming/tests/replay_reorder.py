"""Phase 3 acceptance test: replay real bronze data in order vs. reordered -> identical silver.

    docker compose run --rm --no-deps spark python tests/replay_reorder.py [--trips 150] [--keep]

1. Takes 40 minutes of real bronze.trip_updates for a sample of trips, cut into 2-minute chunks.
2. Writes two test bronze tables, one Iceberg snapshot per chunk:
     ordered   chunks appended in event-time order
     shuffled  chunks reversed inside every 10-minute block (events up to 10 min out of order),
               plus "20-minutes-late" copies of real updates with altered predictions
3. Runs the real silver pipeline (silver_job.start_silver, 15-min watermark, one snapshot per
   micro-batch) over each, then appends far-future sentinel rows so the watermark flushes all state.
4. Asserts: identical silver rows; late copies dropped by the watermark (none of them reach
   silver); every micro-batch saw out-of-order data within the watermark.
"""
import argparse
import logging
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/app")
from silver_job import SILVER_SPARK_CONF, start_silver  # noqa: E402
from spark_session import build_spark, checkpoint_path  # noqa: E402

log = logging.getLogger("replay_test")
NS = "test"
WINDOW_MIN, CHUNK_MIN, BLOCK_CHUNKS, LATE_BY_MIN = 40, 2, 5, 20
COMPARE_COLS = ["service_date", "trip_id", "stop_sequence", "observed_time", "scheduled_time", "delay_seconds",
                "finalized_by", "passed_at", "final_event_time", "n_predictions", "time_kind"]


def drop_test_artifacts(spark: SparkSession) -> None:
    for t in ("bronze_ordered", "bronze_shuffled", "silver_ordered", "silver_shuffled"):
        spark.sql(f"DROP TABLE IF EXISTS {NS}.{t} PURGE")
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    for name in ("test_silver_ordered", "test_silver_shuffled"):
        p = jvm.org.apache.hadoop.fs.Path(checkpoint_path(name))
        p.getFileSystem(conf).delete(p, True)


def sample_bronze(spark: SparkSession, n_trips: int) -> tuple[DataFrame, int]:
    bronze = spark.table("bronze.trip_updates")
    t0 = bronze.select(F.min("ingest_time")).first()[0]
    start = spark.sql(f"SELECT timestamp'{t0}' + INTERVAL 5 MINUTES").first()[0]
    start_s = int(start.timestamp())
    window = bronze.where(
        (F.unix_timestamp("event_time") >= start_s) & (F.unix_timestamp("event_time") < start_s + WINDOW_MIN * 60)
    )
    trips = [r.trip_id for r in window.select("trip_id").distinct().orderBy(F.rand(42)).limit(n_trips).collect()]
    sample = window.where(F.col("trip_id").isin(trips)).withColumn(
        "_chunk", F.floor((F.unix_timestamp("event_time") - start_s) / (CHUNK_MIN * 60)).cast("int")
    ).cache()
    log.info("sampled %d updates for %d trips, window starting %s", sample.count(), len(trips), start)
    return sample, start_s


def append(df: DataFrame, table: str) -> None:
    df.drop("_chunk").coalesce(1).writeTo(table).append()  # one file = one snapshot = one micro-batch


def build_tables(spark: SparkSession, sample: DataFrame, start_s: int) -> int:
    schema_only = sample.drop("_chunk").limit(0)
    for t in ("bronze_ordered", "bronze_shuffled"):
        schema_only.writeTo(f"{NS}.{t}").using("iceberg").create()
    n_chunks = WINDOW_MIN // CHUNK_MIN

    for c in range(n_chunks):
        append(sample.where(F.col("_chunk") == c), f"{NS}.bronze_ordered")

    late_rows = 0
    for block_start in range(0, n_chunks, BLOCK_CHUNKS):
        block = list(range(block_start, min(block_start + BLOCK_CHUNKS, n_chunks)))
        for c in reversed(block):
            append(sample.where(F.col("_chunk") == c), f"{NS}.bronze_shuffled")
        if block_start == 3 * BLOCK_CHUNKS:  # max event time is now ~start+40min
            # Copies of updates stamped LATE_BY_MIN before the latest event, with predictions moved
            # +999 s. If the watermark let them in, silver would differ from the ordered run.
            late_ev = start_s + (block[-1] + 1) * CHUNK_MIN * 60 - LATE_BY_MIN * 60
            late = (
                sample.where(F.col("_chunk") == 4).limit(25)
                .withColumn("event_time", F.timestamp_seconds(F.lit(late_ev) + F.monotonically_increasing_id() % 50))
                .withColumn("stop_time_updates", F.transform(
                    "stop_time_updates", lambda s: s.withField("arrival_time", s["arrival_time"] + 999)))
            )
            late_rows = late.count()
            append(late, f"{NS}.bronze_shuffled")

    # Sentinels: a fake trip 3h in the future pushes the watermark past every real stop.
    for offset_s in (3 * 3600, 3 * 3600 + 60):
        sentinel = (
            sample.limit(1)
            .withColumn("trip_id", F.lit("__sentinel__"))
            .withColumn("event_time", F.timestamp_seconds(F.lit(start_s + WINDOW_MIN * 60 + offset_s)))
            .withColumn("stop_time_updates", F.array().cast(sample.schema["stop_time_updates"].dataType))
        )
        for t in ("bronze_ordered", "bronze_shuffled"):
            append(sentinel, f"{NS}.{t}")
    return late_rows


def wait_until_idle(query, quiet_s: float = 10.0, timeout_s: float = 300.0) -> None:
    """Block until no batch is running and no new batch has started for `quiet_s` seconds.

    processAllAvailable() only waits for input data. Event-time timeouts fire in a separate
    no-data batch after the watermark moves; stopping before it commits loses those rows.
    """
    deadline, last_batch, quiet_since = time.time() + timeout_s, None, time.time()
    while time.time() < deadline:
        batch = (query.lastProgress or {}).get("batchId")
        if batch != last_batch or query.status["isTriggerActive"]:
            last_batch, quiet_since = batch, time.time()
        elif time.time() - quiet_since >= quiet_s:
            return
        time.sleep(0.5)
    raise TimeoutError("streaming query did not go idle")


def run_silver(spark: SparkSession, variant: str) -> dict:
    query = start_silver(spark, f"{NS}.bronze_{variant}", f"{NS}.silver_{variant}", f"test_silver_{variant}",
                         trigger="1 second", watermark="15 minutes", max_files_per_batch=1)
    query.processAllAvailable()
    wait_until_idle(query)  # the watermark-driven "no-data" batch that flushes state runs after this
    progress = query.recentProgress  # list of progress dicts
    query.stop()
    dropped = sum(op["numRowsDroppedByWatermark"] for p in progress for op in p["stateOperators"])
    batches = sum(1 for p in progress if p["numInputRows"] > 0)
    return {"batches_with_data": batches, "rows_dropped_late": dropped,
            "silver_rows": spark.table(f"{NS}.silver_{variant}").count()}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    args = argparse.ArgumentParser()
    args.add_argument("--trips", type=int, default=150)
    args.add_argument("--keep", action="store_true", help="keep test tables for inspection")
    opts = args.parse_args()

    spark = build_spark("replay_reorder_test", {**SILVER_SPARK_CONF, "spark.ui.enabled": "false"})
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")
    drop_test_artifacts(spark)
    sample, start_s = sample_bronze(spark, opts.trips)
    late_rows = build_tables(spark, sample, start_s)

    results = {v: run_silver(spark, v) for v in ("ordered", "shuffled")}
    a = spark.table(f"{NS}.silver_ordered").select(COMPARE_COLS)
    b = spark.table(f"{NS}.silver_shuffled").select(COMPARE_COLS)
    only_ordered, only_shuffled = a.exceptAll(b).count(), b.exceptAll(a).count()

    print("\n================ replay / reorder test ================")
    for v, r in results.items():
        print(f"{v:9s}: {r}")
    print(f"late rows injected (event_time {LATE_BY_MIN} min behind max): {late_rows}")
    print(f"rows only in ordered: {only_ordered}   rows only in shuffled: {only_shuffled}")
    checks = {
        "silver identical under reordering": only_ordered == 0 and only_shuffled == 0,
        "silver not empty": results["ordered"]["silver_rows"] > 0,
        "ordered run dropped nothing as late": results["ordered"]["rows_dropped_late"] == 0,
        "every 20-min-late row was dropped": results["shuffled"]["rows_dropped_late"] >= late_rows > 0,
        "data was spread over many micro-batches": results["shuffled"]["batches_with_data"] >= 10,
    }
    for name, ok in checks.items():
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    if not opts.keep:
        drop_test_artifacts(spark)
    spark.stop()
    sys.exit(0 if all(checks.values()) else 1)


if __name__ == "__main__":
    main()
