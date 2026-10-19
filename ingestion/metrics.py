"""Prometheus metrics for the ingestion service, served on /metrics (port $METRICS_PORT)."""
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# --- realtime polling ---
POLLS = Counter("gtfs_polls_total", "Feed polls by outcome", ["feed", "outcome"])
POLL_ERRORS = Counter("gtfs_poll_errors_total", "Poll failures by stage (fetch/decode/validate)", ["feed", "stage"])
POLL_LATENCY = Histogram(
    "gtfs_poll_latency_seconds",
    "Wall time of one poll: fetch + decode + produce",
    ["feed"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)
FEED_ENTITIES = Gauge("gtfs_feed_entities", "Entities in the latest decoded snapshot", ["feed"])
FEED_AGE = Gauge("gtfs_feed_age_seconds", "Now minus the feed header timestamp at poll time", ["feed"])
LAST_SUCCESS = Gauge("gtfs_last_successful_poll_timestamp_seconds", "Unix time of the last successful poll", ["feed"])
ENTITIES_UNCHANGED = Counter(
    "gtfs_entities_unchanged_total", "Entities skipped because they are byte-identical to the previous poll", ["feed"]
)

# --- kafka ---
EVENTS_PRODUCED = Counter("gtfs_events_produced_total", "Records acknowledged by Kafka", ["topic"])
DLQ_RECORDS = Counter("gtfs_dlq_records_total", "Records sent to a dead-letter topic", ["feed", "stage"])
DELIVERY_FAILURES = Counter("gtfs_kafka_delivery_failures_total", "Records Kafka failed to acknowledge", ["topic"])

# --- static schedule ---
SCHEDULE_LOADS = Counter("gtfs_schedule_loads_total", "Static schedule load attempts", ["outcome"])
SCHEDULE_LAST_LOAD = Gauge("gtfs_schedule_last_load_timestamp_seconds", "Unix time the static schedule was last loaded")
SCHEDULE_ROWS = Gauge("gtfs_schedule_rows", "Rows per static schedule table", ["table"])


def start_metrics_server(port: int) -> None:
    start_http_server(port)
