"""Bronze: raw Kafka topics -> Iceberg tables, untransformed.

One streaming query per topic:
    vehicle_positions.raw -> bronze.vehicle_positions
    trip_updates.raw      -> bronze.trip_updates     (stop_time_updates kept as a nested array)

Bronze is a faithful copy: no dedup, no join, no filtering. Each row is the producer's JSON
parsed against an explicit schema (the record contract in ingestion/poller.py) plus Kafka
lineage columns (topic/partition/offset), which make replays and duplicate checks provable.
A value that fails to parse is not dropped: its raw text lands in `_raw_value`.

Exactly-once into Iceberg: Kafka offsets live in the checkpoint, and the Iceberg sink records
each micro-batch id in the snapshot summary so a replayed batch is not committed twice.
"""
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_session import build_spark, checkpoint_path

log = logging.getLogger("bronze")

_COMMON_FIELDS = [
    StructField("entity_id", StringType()),
    StructField("route_id", StringType()),
    StructField("trip_id", StringType()),
    StructField("direction_id", IntegerType()),
    StructField("start_date", StringType()),
    StructField("start_time", StringType()),
    StructField("trip_schedule_relationship", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("vehicle_label", StringType()),
    StructField("event_time", TimestampType()),
    StructField("event_time_source", StringType()),
    StructField("feed_timestamp", TimestampType()),
    StructField("ingest_time", TimestampType()),
    StructField("source", StringType()),
]

VEHICLE_POSITION_SCHEMA = StructType(_COMMON_FIELDS + [
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("bearing", DoubleType()),
    StructField("speed", DoubleType()),
    StructField("current_stop_sequence", IntegerType()),
    StructField("stop_id", StringType()),
    StructField("current_status", StringType()),
    StructField("occupancy_status", StringType()),
])

STOP_TIME_UPDATE_SCHEMA = StructType([
    StructField("stop_sequence", IntegerType()),
    StructField("stop_id", StringType()),
    StructField("arrival_time", LongType()),
    StructField("arrival_delay", IntegerType()),
    StructField("arrival_uncertainty", IntegerType()),
    StructField("departure_time", LongType()),
    StructField("departure_delay", IntegerType()),
    StructField("departure_uncertainty", IntegerType()),
    StructField("schedule_relationship", StringType()),
])

TRIP_UPDATE_SCHEMA = StructType(_COMMON_FIELDS + [
    StructField("trip_delay", IntegerType()),
    StructField("stop_time_updates", ArrayType(STOP_TIME_UPDATE_SCHEMA)),
])

# feed -> (kafka topic, value schema, iceberg table)
STREAMS = {
    "vehicle_positions": ("vehicle_positions.raw", VEHICLE_POSITION_SCHEMA, "bronze.vehicle_positions"),
    "trip_updates": ("trip_updates.raw", TRIP_UPDATE_SCHEMA, "bronze.trip_updates"),
}

TABLE_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    # a commit every trigger adds a metadata.json; keep only the recent ones
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}


def parse_kafka(raw: DataFrame, schema: StructType) -> DataFrame:
    """Kafka rows -> one column per contract field + lineage columns. No rows are dropped."""
    value = F.col("value").cast("string")
    parsed = raw.select(
        F.from_json(value, schema).alias("r"),
        value.alias("_value"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    unparsed = F.col("r").isNull() | F.col("r.event_time").isNull() | F.col("r.route_id").isNull()
    return parsed.select(
        *[F.col(f"r.{f.name}").alias(f.name) for f in schema.fields],
        "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        F.when(unparsed, F.col("_value")).alias("_raw_value"),
        F.current_timestamp().alias("bronze_ingested_at"),
    )


def ensure_table(spark: SparkSession, table: str, df: DataFrame) -> None:
    """Create the bronze table from the parsed stream's schema (explicit, never inferred)."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.split('.')[0]}")
    if spark.catalog.tableExists(table):
        return
    writer = spark.createDataFrame([], df.schema).writeTo(table).using("iceberg")
    for key, value in TABLE_PROPERTIES.items():
        writer = writer.tableProperty(key, value)
    # Hidden partitioning on event day; rows with an unparseable event_time land in the null partition.
    writer.partitionedBy(F.days("event_time")).create()
    log.info("created table %s", table)


class ProgressLogger(StreamingQueryListener):
    def onQueryStarted(self, event):
        log.info("query %s started (id %s)", event.name, event.id)

    def onQueryProgress(self, event):
        p = event.progress
        log.info("%s batch %d: %d rows, %.1f rows/s", p.name, p.batchId, p.numInputRows,
                 p.processedRowsPerSecond or 0.0)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log.warning("query %s terminated%s", event.id, f": {event.exception}" if event.exception else "")


def start_stream(spark: SparkSession, feed: str, trigger: str):
    topic, schema, table = STREAMS[feed]
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092"))
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")  # first run only; afterwards the checkpoint wins
        # Raw topics keep 48h. If the job is down longer, keep going rather than crash-loop.
        .option("failOnDataLoss", "false")
    )
    if os.environ.get("BRONZE_MAX_OFFSETS_PER_TRIGGER"):
        reader = reader.option("maxOffsetsPerTrigger", os.environ["BRONZE_MAX_OFFSETS_PER_TRIGGER"])
    df = parse_kafka(reader.load(), schema)
    ensure_table(spark, table, df)
    # One task per Kafka partition would write 6 tiny files per table every trigger;
    # coalescing keeps the file count sane (raise it under load — phase 6).
    df = df.coalesce(int(os.environ.get("BRONZE_WRITE_PARTITIONS", "1")))
    return (
        df.writeStream.format("iceberg")
        .queryName(f"bronze_{feed}")
        .outputMode("append")
        .trigger(processingTime=trigger)
        .option("checkpointLocation", checkpoint_path(f"bronze_{feed}"))
        .option("fanout-enabled", "true")  # a batch may span two event days around midnight
        .toTable(table)
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    spark = build_spark("bronze_job")
    spark.streams.addListener(ProgressLogger())
    trigger = f"{os.environ.get('BRONZE_TRIGGER_SECONDS', '10')} seconds"
    for feed in STREAMS:
        start_stream(spark, feed, trigger)
    spark.streams.awaitAnyTermination()  # if either query dies, exit so the container restarts


if __name__ == "__main__":
    main()
