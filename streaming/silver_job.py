"""Silver: bronze.trip_updates -> silver.stop_delays (one row per observed stop visit, with delay).

Pipeline (one streaming query):
    readStream bronze.trip_updates (Iceberg, append snapshots)
      -> withWatermark(event_time, 15 min)
      -> dropDuplicatesWithinWatermark(start_date, trip_id, vehicle_id, event_time)
         (a trip update at a given event_time fully determines its stop rows, so deduping the
         update dedups every (vehicle, trip, stop, event_time) row with ~25x less state)
      -> explode stop_time_updates
      -> groupBy(start_date, trip_id).applyInPandasWithState(TripTracker)   [trip_state.py]
         emits one row per stop once the watermark passes the moment the stop was passed
      -> foreachBatch: join the active static schedule (broadcast) on
         (service_date, trip_id, stop_sequence), compute delay_seconds, append to Iceberg
         idempotently (batch id stored in the snapshot summary)

Times past 24:00 (problem 2): scheduled_time = service-day start + arrival_secs, where the
service-day start is "noon minus 12h" local time on the trip's start_date (the GTFS
definition, correct on DST days), so 25:10:00 on 20261019 is 01:10 local on Oct 20.

Join key: (trip_id, stop_sequence) rather than (trip_id, stop_id). Realtime data reports
platform-level stop ids (e.g. "Alewife-02") where the schedule has the station ("70061"), so
joining on stop_id loses those rows. The scheduled stop id is kept as scheduled_stop_id.
"""
import json
import logging
import os
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from spark_session import bucket, build_spark, checkpoint_path
from trip_state import TripTracker, finals_frame, updates_from_frame

log = logging.getLogger("silver")

SCHEDULE_TZ = os.environ.get("SCHEDULE_TIMEZONE", "America/New_York")
SCHEDULE_BASE = f"s3a://{bucket()}/reference/gtfs_static"

FINALS_SCHEMA = StructType([
    StructField("start_date", StringType()),
    StructField("trip_id", StringType()),
    StructField("route_id", StringType()),
    StructField("direction_id", IntegerType()),
    StructField("vehicle_id", StringType()),
    StructField("trip_schedule_relationship", StringType()),
    StructField("stop_sequence", IntegerType()),
    StructField("stop_id", StringType()),
    StructField("stop_schedule_relationship", StringType()),
    StructField("time_kind", StringType()),
    StructField("observed_time_s", LongType()),
    StructField("final_event_ms", LongType()),
    StructField("final_ingest_ms", LongType()),
    StructField("first_event_ms", LongType()),
    StructField("passed_at_ms", LongType()),
    StructField("n_predictions", IntegerType()),
    StructField("finalized_by", StringType()),
])
STATE_SCHEMA = StructType([StructField("tracker_json", StringType())])

SILVER_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    service_date               date      COMMENT 'GTFS service day (trip start_date)',
    route_id                   string,
    direction_id               int,
    trip_id                    string,
    vehicle_id                 string,
    stop_sequence              int,
    stop_id                    string    COMMENT 'stop id from the realtime feed (may be a platform)',
    scheduled_stop_id          string    COMMENT 'stop id from the static schedule',
    time_kind                  string    COMMENT 'arrival, or departure for first stops',
    scheduled_time             timestamp,
    observed_time              timestamp COMMENT 'last prediction before the stop was passed',
    delay_seconds              int       COMMENT 'observed_time - scheduled_time',
    scheduled_hour_local       int       COMMENT 'hour of day of scheduled_time in agency timezone',
    prediction_horizon_seconds int       COMMENT 'observed_time - final_event_time: how far ahead the final prediction was',
    n_predictions              int,
    first_event_time           timestamp,
    final_event_time           timestamp,
    passed_at                  timestamp COMMENT 'event time of the first update proving the stop was passed',
    finalized_by               string    COMMENT 'passed | expired',
    trip_schedule_relationship string,
    stop_schedule_relationship string,
    schedule_matched           boolean,
    ingest_time                timestamp COMMENT 'poller fetch time of the final prediction',
    processed_time             timestamp COMMENT 'when this row was written to silver'
)
USING iceberg
PARTITIONED BY (service_date, bucket(8, route_id))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.distribution-mode' = 'hash',
    -- the stream appends every 30 s; snapshot isolation lets maintenance DELETE/MERGE commit
    -- alongside those appends instead of failing on "conflicting files"
    'delete.isolation-level' = 'snapshot',
    'update.isolation-level' = 'snapshot',
    'merge.isolation-level' = 'snapshot',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '100'
)
"""


# ---------------------------------------------------------------- stream: bronze -> final stop visits

def track_trip_group(key, pdfs, state):
    """applyInPandasWithState callback for one (start_date, trip_id)."""
    watermark_ms = max(state.getCurrentWatermarkMs(), 0)
    tracker = TripTracker.from_json(state.get[0]) if state.exists else TripTracker(key[0], key[1])
    if not state.hasTimedOut:
        for pdf in pdfs:
            for update in updates_from_frame(pdf):
                tracker.apply(update, watermark_ms)
    finals = tracker.release(watermark_ms)
    if tracker.can_drop(watermark_ms):
        state.remove()
    else:
        state.update((tracker.to_json(),))
        state.setTimeoutTimestamp(max(tracker.next_deadline_ms(), watermark_ms + 1))
    if finals:
        yield finals_frame(finals)


def build_finals_stream(spark: SparkSession, source_table: str, watermark: str,
                        max_files_per_batch: int) -> DataFrame:
    bronze = (
        spark.readStream.format("iceberg")
        .option("streaming-max-files-per-micro-batch", str(max_files_per_batch))
        # compaction / expiry (phase 5) rewrite or delete files; those snapshots add no new events
        .option("streaming-skip-overwrite-snapshots", "true")
        .option("streaming-skip-delete-snapshots", "true")
        .load(source_table)
    )
    updates = (
        bronze.where(F.col("event_time").isNotNull() & F.col("trip_id").isNotNull() & F.col("start_date").isNotNull())
        .withWatermark("event_time", watermark)
        .dropDuplicatesWithinWatermark(["start_date", "trip_id", "vehicle_id", "event_time"])
    )
    rows = updates.select(
        "start_date", "trip_id", "route_id", "direction_id", "vehicle_id", "trip_schedule_relationship",
        "event_time", "ingest_time", F.explode_outer("stop_time_updates").alias("stu"),
    ).select(
        "start_date", "trip_id", "route_id", "direction_id", "vehicle_id", "trip_schedule_relationship",
        "event_time", "ingest_time",
        F.col("stu.stop_sequence").alias("stop_sequence"), F.col("stu.stop_id").alias("stop_id"),
        F.col("stu.arrival_time").alias("arrival_time"), F.col("stu.departure_time").alias("departure_time"),
        F.col("stu.schedule_relationship").alias("stop_schedule_relationship"),
    )
    return rows.groupBy("start_date", "trip_id").applyInPandasWithState(
        track_trip_group,
        outputStructType=FINALS_SCHEMA,
        stateStructType=STATE_SCHEMA,
        outputMode="append",
        timeoutConf=GroupStateTimeout.EventTimeTimeout,
    )


# ---------------------------------------------------------------- static schedule (broadcast side)

def read_small_file(spark: SparkSession, path: str) -> str:
    """Read a small text file via the Hadoop FS API. (spark.read skips names starting with '_'.)"""
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    stream = fs.open(hpath)
    try:
        return jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
    finally:
        stream.close()


class ScheduleCache:
    """Scheduled stop times for the trips that actually run on the needed service dates.

    The full stop_times table is 3.1M rows; filtered by calendar to a few service days it is a
    few hundred thousand, small enough to broadcast. Rebuilt when a new service date shows up
    or the poller publishes a new schedule version (checked every `check_interval_s`).
    """

    def __init__(self, spark: SparkSession, check_interval_s: float = 600):
        self.spark = spark
        self.check_interval_s = check_interval_s
        self._version, self._version_checked_at = None, 0.0
        self.dates: set[str] = set()
        self.loaded_version = None
        self.df: DataFrame | None = None

    def _read(self, table: str) -> DataFrame:
        return self.spark.read.parquet(f"{SCHEDULE_BASE}/{table}/")

    def _current_version(self) -> str:
        now = datetime.now().timestamp()
        if self._version is None or now - self._version_checked_at > self.check_interval_s:
            manifest = json.loads(read_small_file(self.spark, f"{SCHEDULE_BASE}/_manifest.json"))
            self._version = f"{manifest.get('feed_version')}|{manifest.get('loaded_at')}"
            self._version_checked_at = now
        return self._version

    def get(self, needed: set[str]) -> DataFrame:
        version = self._current_version()
        if self.df is None or version != self.loaded_version or not needed <= self.dates:
            dates = set(needed)  # ~400k stop times per service day; rebuilt about once a day
            fresh = self._build(sorted(dates)).persist()
            rows = fresh.count()
            if self.df is not None:
                self.df.unpersist()
            self.df, self.dates, self.loaded_version = fresh, dates, version
            log.info("schedule cache rebuilt: %d stop times for service dates %s (version %s)",
                     rows, sorted(dates), version)
        return self.df

    def _build(self, dates: list[str]) -> DataFrame:
        dates_df = self.spark.createDataFrame([(d,) for d in dates], "service_date string")
        calendar, calendar_dates = self._read("calendar"), self._read("calendar_dates")
        weekday_flag = F.element_at(  # dayofweek: 1 = Sunday
            F.array("sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"),
            F.dayofweek(F.to_date("service_date", "yyyyMMdd")),
        )
        regular = (
            dates_df.crossJoin(calendar)
            .where((F.col("service_date") >= F.col("start_date")) & (F.col("service_date") <= F.col("end_date"))
                   & (weekday_flag == 1))
            .select("service_date", "service_id")
        )
        exceptions = calendar_dates.withColumnRenamed("date", "service_date").join(dates_df, "service_date")
        added = exceptions.where("exception_type = 1").select("service_date", "service_id")
        removed = exceptions.where("exception_type = 2").select("service_date", "service_id")
        active_services = regular.union(added).distinct().join(removed, ["service_date", "service_id"], "left_anti")
        active_trips = active_services.join(self._read("trips").select("trip_id", "service_id"), "service_id")
        return (
            active_trips.join(self._read("stop_times"), "trip_id")
            .select("service_date", "trip_id", "stop_sequence",
                    F.col("stop_id").alias("scheduled_stop_id"), "arrival_secs", "departure_secs")
        )


# ---------------------------------------------------------------- sink

def enrich(finals: DataFrame, schedule: DataFrame) -> DataFrame:
    f, s = finals.alias("f"), F.broadcast(schedule).alias("s")
    joined = f.join(
        s,
        (F.col("f.start_date") == F.col("s.service_date")) & (F.col("f.trip_id") == F.col("s.trip_id"))
        & (F.col("f.stop_sequence") == F.col("s.stop_sequence")),
        "left",
    )
    scheduled_secs = F.when(F.col("f.time_kind") == "departure", F.coalesce("s.departure_secs", "s.arrival_secs")) \
        .otherwise(F.coalesce("s.arrival_secs", "s.departure_secs"))
    service_day_start = F.unix_timestamp(
        F.to_utc_timestamp(F.to_timestamp(F.concat(F.col("f.start_date"), F.lit(" 12:00:00")), "yyyyMMdd HH:mm:ss"),
                           SCHEDULE_TZ)
    ) - 12 * 3600
    scheduled_time = F.timestamp_seconds(service_day_start + scheduled_secs)
    observed_time = F.timestamp_seconds(F.col("f.observed_time_s"))
    return joined.select(
        F.to_date(F.col("f.start_date"), "yyyyMMdd").alias("service_date"),
        F.col("f.route_id").alias("route_id"),
        F.col("f.direction_id").alias("direction_id"),
        F.col("f.trip_id").alias("trip_id"),
        F.col("f.vehicle_id").alias("vehicle_id"),
        F.col("f.stop_sequence").alias("stop_sequence"),
        F.col("f.stop_id").alias("stop_id"),
        F.col("s.scheduled_stop_id").alias("scheduled_stop_id"),
        F.col("f.time_kind").alias("time_kind"),
        scheduled_time.alias("scheduled_time"),
        observed_time.alias("observed_time"),
        (F.col("f.observed_time_s") - (service_day_start + scheduled_secs)).cast("int").alias("delay_seconds"),
        F.hour(F.from_utc_timestamp(scheduled_time, SCHEDULE_TZ)).alias("scheduled_hour_local"),
        (F.col("f.observed_time_s") - F.col("f.final_event_ms") / 1000).cast("int").alias("prediction_horizon_seconds"),
        F.col("f.n_predictions").alias("n_predictions"),
        F.timestamp_millis(F.col("f.first_event_ms")).alias("first_event_time"),
        F.timestamp_millis(F.col("f.final_event_ms")).alias("final_event_time"),
        F.timestamp_millis(F.col("f.passed_at_ms")).alias("passed_at"),
        F.col("f.finalized_by").alias("finalized_by"),
        F.col("f.trip_schedule_relationship").alias("trip_schedule_relationship"),
        F.col("f.stop_schedule_relationship").alias("stop_schedule_relationship"),
        F.col("s.trip_id").isNotNull().alias("schedule_matched"),
        F.timestamp_millis(F.col("f.final_ingest_ms")).alias("ingest_time"),
        F.current_timestamp().alias("processed_time"),
    )


class SilverSink:
    """foreachBatch sink: enrich + append, idempotent per (query run, batch id).

    A micro-batch can be re-executed after a crash. Each append stores the streaming query id
    and batch id in the Iceberg snapshot summary; a batch already present is skipped, which
    makes the foreachBatch sink exactly-once.
    """

    def __init__(self, spark: SparkSession, table: str, checkpoint: str, schedule: ScheduleCache):
        self.spark, self.table, self.checkpoint, self.schedule = spark, table, checkpoint, schedule
        self._run_id = None

    def run_id(self) -> str:
        if self._run_id is None:  # the query id Spark persists in the checkpoint; stable across restarts
            self._run_id = json.loads(read_small_file(self.spark, f"{self.checkpoint}/metadata"))["id"]
        return self._run_id

    def already_committed(self, batch_id: int) -> bool:
        return self.spark.sql(
            f"SELECT 1 FROM {self.table}.snapshots "
            f"WHERE summary['silver.query-id'] = '{self.run_id()}' AND summary['silver.batch-id'] = '{batch_id}' LIMIT 1"
        ).count() > 0

    def __call__(self, finals: DataFrame, batch_id: int) -> None:
        finals = finals.persist()
        try:
            if finals.isEmpty():
                return
            if self.already_committed(batch_id):
                log.warning("batch %d already committed to %s; skipping replay", batch_id, self.table)
                return
            dates = {r.start_date for r in finals.select("start_date").distinct().collect()}
            out = enrich(finals, self.schedule.get(dates))
            (out.writeTo(self.table)
                .option("snapshot-property.silver.query-id", self.run_id())
                .option("snapshot-property.silver.batch-id", str(batch_id))
                .append())
        finally:
            finals.unpersist()


# ---------------------------------------------------------------- wiring

class ProgressLogger(StreamingQueryListener):
    def onQueryStarted(self, event):
        log.info("query %s started (id %s)", event.name, event.id)

    def onQueryProgress(self, event):
        p = event.progress
        ops = " | ".join(
            f"{o.operatorName}: {o.numRowsTotal} keys, {o.memoryUsedBytes / 1e6:.1f} MB, "
            f"dropped_late={o.numRowsDroppedByWatermark}"
            for o in p.stateOperators
        )
        log.info("%s batch %d: in=%d rows, watermark=%s | %s", p.name, p.batchId, p.numInputRows,
                 p.eventTime.get("watermark"), ops)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log.warning("query %s terminated%s", event.id, f": {event.exception}" if event.exception else "")


SILVER_SPARK_CONF = {
    # RocksDB keeps state off the JVM heap, so memory stays flat as state grows
    "spark.sql.streaming.stateStore.providerClass":
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
    "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled": "true",
}


def ensure_silver_table(spark: SparkSession, table: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.split('.')[0]}")
    spark.sql(SILVER_DDL.format(table=table))


def start_silver(spark: SparkSession, source_table: str, target_table: str, query_name: str,
                 trigger: str, watermark: str, max_files_per_batch: int):
    ensure_silver_table(spark, target_table)
    checkpoint = checkpoint_path(query_name)
    sink = SilverSink(spark, target_table, checkpoint, ScheduleCache(spark))
    return (
        build_finals_stream(spark, source_table, watermark, max_files_per_batch)
        .writeStream.queryName(query_name)
        .foreachBatch(sink)
        .trigger(processingTime=trigger)
        .option("checkpointLocation", checkpoint)
        .start()
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    spark = build_spark("silver_job", SILVER_SPARK_CONF)
    spark.streams.addListener(ProgressLogger())
    query = start_silver(
        spark,
        source_table=os.environ.get("SILVER_SOURCE_TABLE", "bronze.trip_updates"),
        target_table=os.environ.get("SILVER_TABLE", "silver.stop_delays"),
        query_name=os.environ.get("SILVER_QUERY_NAME", "silver_stop_delays"),
        trigger=f"{os.environ.get('SILVER_TRIGGER_SECONDS', '30')} seconds",
        watermark=os.environ.get("SILVER_WATERMARK", "15 minutes"),
        max_files_per_batch=int(os.environ.get("SILVER_MAX_FILES_PER_BATCH", "60")),
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
