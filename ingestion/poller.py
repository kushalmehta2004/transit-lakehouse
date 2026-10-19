"""GTFS-realtime poller: fetch protobuf feeds on an interval -> JSON records -> Kafka.

One thread per feed. Each poll:
    fetch (HTTP) -> decode (protobuf) -> per entity: build + validate a JSON record
    -> produce to <feed>.raw, keyed by route_id (so one route's events stay ordered).
Any fetch / decode / validation failure goes to <feed>.dlq with the error; the loop keeps going.
Entities byte-identical to the previous poll are skipped (re-fetching an unchanged snapshot is
a polling artifact, not a new event). Event-level dedup still happens in Spark (phase 3).

Record contract (streaming/bronze_job.py parses exactly this — keep them in sync):
  common:            entity_id, route_id, trip_id, direction_id, start_date (YYYYMMDD), start_time,
                     trip_schedule_relationship, vehicle_id, vehicle_label,
                     event_time      ISO-8601 UTC; entity timestamp, else the feed header timestamp
                     event_time_source  "entity" | "header"
                     feed_timestamp  ISO-8601 UTC feed header timestamp
                     ingest_time     ISO-8601 UTC, when this poller fetched the snapshot
                     source          feed URL
  vehicle_positions: latitude, longitude, bearing, speed, current_stop_sequence, stop_id,
                     current_status, occupancy_status
  trip_updates:      trip_delay, stop_time_updates[] = {stop_sequence, stop_id,
                     arrival_time, arrival_delay, arrival_uncertainty,
                     departure_time, departure_delay, departure_uncertainty, schedule_relationship}
                     (arrival_time / departure_time are epoch seconds, as the feed publishes them;
                     MBTA publishes predicted times, not delays)
DLQ records:         feed, error_stage (fetch|decode|validate), error, source, ingest_time, payload
"""
import base64
import hashlib
import json
import logging
import os
import signal
import threading
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

import metrics
from schedule_loader import ScheduleLoader

log = logging.getLogger("poller")

FEEDS = {
    "vehicle_positions": ("GTFS_RT_VEHICLE_POSITIONS_URL", "https://cdn.mbta.com/realtime/VehiclePositions.pb"),
    "trip_updates": ("GTFS_RT_TRIP_UPDATES_URL", "https://cdn.mbta.com/realtime/TripUpdates.pb"),
}


def raw_topic(feed: str) -> str:
    return f"{feed}.raw"


def dlq_topic(feed: str) -> str:
    return f"{feed}.dlq"


class ValidationError(ValueError):
    """An entity that decoded fine but can't be used (missing key, bad position, ...)."""


class FeedDecodeError(ValueError):
    """The HTTP body is not a GTFS-realtime FeedMessage."""


# ---------------------------------------------------------------- record building (pure)

def to_iso(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _opt(msg, field):
    return getattr(msg, field) if msg is not None and msg.HasField(field) else None


def _enum_name(msg, field, effective_default: bool = False):
    """Enum value name; None when unset unless the spec defines a default (effective_default)."""
    if not msg.HasField(field) and not effective_default:
        return None
    value = getattr(msg, field)
    enum_value = msg.DESCRIPTOR.fields_by_name[field].enum_type.values_by_number.get(value)
    return enum_value.name if enum_value else str(value)


def decode_feed(body: bytes) -> gtfs_realtime_pb2.FeedMessage:
    if not body:
        raise FeedDecodeError("empty response body")
    msg = gtfs_realtime_pb2.FeedMessage()
    try:
        msg.ParseFromString(body)
    except DecodeError as e:
        raise FeedDecodeError(f"protobuf decode failed: {e}") from e
    if not msg.header.gtfs_realtime_version:
        raise FeedDecodeError("missing header.gtfs_realtime_version: not a GTFS-realtime feed")
    return msg


def _common_fields(entity, trip, vehicle_desc, ts_msg, header_ts, ingest_time, source) -> dict:
    if ts_msg.HasField("timestamp") and ts_msg.timestamp > 0:
        event_ts, event_src = ts_msg.timestamp, "entity"
    elif header_ts:
        event_ts, event_src = header_ts, "header"
    else:
        raise ValidationError("no entity or header timestamp")
    record = {
        "entity_id": entity.id,
        "route_id": _opt(trip, "route_id"),
        "trip_id": _opt(trip, "trip_id"),
        "direction_id": _opt(trip, "direction_id"),
        "start_date": _opt(trip, "start_date"),
        "start_time": _opt(trip, "start_time"),
        "trip_schedule_relationship": _enum_name(trip, "schedule_relationship", effective_default=True),
        "vehicle_id": _opt(vehicle_desc, "id"),
        "vehicle_label": _opt(vehicle_desc, "label"),
        "event_time": to_iso(event_ts),
        "event_time_source": event_src,
        "feed_timestamp": to_iso(header_ts) if header_ts else None,
        "ingest_time": ingest_time,
        "source": source,
    }
    if not record["route_id"]:
        raise ValidationError("missing route_id (Kafka partition key)")
    return record


def vehicle_position_record(entity, header_ts, ingest_time, source) -> dict:
    if not entity.HasField("vehicle"):
        raise ValidationError("entity has no vehicle")
    v = entity.vehicle
    record = _common_fields(entity, v.trip, v.vehicle, v, header_ts, ingest_time, source)
    if not record["vehicle_id"]:
        raise ValidationError("missing vehicle.vehicle.id")
    if not v.HasField("position"):
        raise ValidationError("missing position")
    pos = v.position
    lat, lon = round(pos.latitude, 7), round(pos.longitude, 7)
    if not (-90 <= lat <= 90 and -180 <= lon <= 180) or (lat == 0 and lon == 0):
        raise ValidationError(f"position out of range: {lat},{lon}")
    record.update(
        latitude=lat,
        longitude=lon,
        bearing=_opt(pos, "bearing"),
        speed=_opt(pos, "speed"),
        current_stop_sequence=_opt(v, "current_stop_sequence"),
        stop_id=_opt(v, "stop_id"),
        current_status=_enum_name(v, "current_status"),
        occupancy_status=_enum_name(v, "occupancy_status"),
    )
    return record


def _stop_time_update(stu) -> dict:
    arr = stu.arrival if stu.HasField("arrival") else None
    dep = stu.departure if stu.HasField("departure") else None
    return {
        "stop_sequence": _opt(stu, "stop_sequence"),
        "stop_id": _opt(stu, "stop_id"),
        "arrival_time": _opt(arr, "time"),
        "arrival_delay": _opt(arr, "delay"),
        "arrival_uncertainty": _opt(arr, "uncertainty"),
        "departure_time": _opt(dep, "time"),
        "departure_delay": _opt(dep, "delay"),
        "departure_uncertainty": _opt(dep, "uncertainty"),
        "schedule_relationship": _enum_name(stu, "schedule_relationship", effective_default=True),
    }


def trip_update_record(entity, header_ts, ingest_time, source) -> dict:
    if not entity.HasField("trip_update"):
        raise ValidationError("entity has no trip_update")
    tu = entity.trip_update
    record = _common_fields(entity, tu.trip, tu.vehicle, tu, header_ts, ingest_time, source)
    if not record["trip_id"]:
        raise ValidationError("missing trip_id")
    record["trip_delay"] = _opt(tu, "delay")
    record["stop_time_updates"] = [_stop_time_update(s) for s in tu.stop_time_update]
    return record


RECORD_BUILDERS = {"vehicle_positions": vehicle_position_record, "trip_updates": trip_update_record}


def dlq_record(feed, stage, error, source, ingest_time, payload=None) -> dict:
    return {
        "feed": feed,
        "error_stage": stage,
        "error": str(error)[:2000],
        "source": source,
        "ingest_time": ingest_time,
        "payload": payload,
    }


# ---------------------------------------------------------------- kafka

def _on_delivery(err, msg):
    if err is not None:
        metrics.DELIVERY_FAILURES.labels(msg.topic()).inc()
        log.error("delivery to %s failed: %s", msg.topic(), err)
    else:
        metrics.EVENTS_PRODUCED.labels(msg.topic()).inc()


def produce_json(producer: Producer, topic: str, key: str, value: dict) -> None:
    data = json.dumps(value, separators=(",", ":")).encode()
    while True:
        try:
            producer.produce(topic, key=key.encode(), value=data, on_delivery=_on_delivery)
            break
        except BufferError:
            producer.poll(0.5)  # local queue full: serve delivery reports until there is room
    producer.poll(0)


def ensure_topics(bootstrap: str, partitions: int, raw_retention_ms: int, dlq_retention_ms: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    while True:
        try:
            existing = admin.list_topics(timeout=10).topics
            break
        except KafkaException as e:
            log.warning("waiting for Kafka at %s: %s", bootstrap, e)
            time.sleep(3)
    wanted = []
    for feed in FEEDS:
        wanted.append(NewTopic(raw_topic(feed), num_partitions=partitions, replication_factor=1,
                               config={"retention.ms": str(raw_retention_ms)}))
        wanted.append(NewTopic(dlq_topic(feed), num_partitions=1, replication_factor=1,
                               config={"retention.ms": str(dlq_retention_ms)}))
    missing = [t for t in wanted if t.topic not in existing]
    for topic, future in (admin.create_topics(missing).items() if missing else []):
        try:
            future.result()
            log.info("created topic %s", topic)
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


# ---------------------------------------------------------------- poll loop

class FeedPoller(threading.Thread):
    def __init__(self, feed: str, url: str, producer: Producer, interval_s: float, timeout_s: float,
                 stop_event: threading.Event):
        super().__init__(name=f"poll-{feed}", daemon=True)
        self.feed, self.url, self.producer = feed, url, producer
        self.interval_s, self.timeout_s, self.stop_event = interval_s, timeout_s, stop_event
        self.build_record = RECORD_BUILDERS[feed]
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "transit-lakehouse-poller/1.0"
        self._last_fingerprints: dict[str, bytes] = {}

    def run(self) -> None:
        log.info("polling %s every %ss from %s", self.feed, self.interval_s, self.url)
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                self.poll_once()
            except Exception:  # never let one bad poll kill the thread
                log.exception("unexpected error polling %s", self.feed)
                metrics.POLL_ERRORS.labels(self.feed, "unexpected").inc()
            self.stop_event.wait(max(0.0, self.interval_s - (time.monotonic() - started)))

    def _fail(self, stage: str, error, ingest_time: str, payload=None) -> None:
        log.warning("%s %s failed: %s", self.feed, stage, error)
        metrics.POLL_ERRORS.labels(self.feed, stage).inc()
        metrics.POLLS.labels(self.feed, "error").inc()
        metrics.DLQ_RECORDS.labels(self.feed, stage).inc()
        produce_json(self.producer, dlq_topic(self.feed), self.feed,
                     dlq_record(self.feed, stage, error, self.url, ingest_time, payload))
        self.producer.flush(10)

    def poll_once(self) -> None:
        ingest_time = to_iso(time.time())
        with metrics.POLL_LATENCY.labels(self.feed).time():
            try:
                resp = self.session.get(self.url, timeout=self.timeout_s)
                resp.raise_for_status()
            except requests.RequestException as e:
                return self._fail("fetch", e, ingest_time)
            try:
                feed_msg = decode_feed(resp.content)
            except FeedDecodeError as e:
                return self._fail("decode", e, ingest_time,
                                  {"body_base64_prefix": base64.b64encode(resp.content[:2048]).decode()})

            header_ts = feed_msg.header.timestamp if feed_msg.header.HasField("timestamp") else None
            metrics.FEED_ENTITIES.labels(self.feed).set(len(feed_msg.entity))
            if header_ts:
                metrics.FEED_AGE.labels(self.feed).set(time.time() - header_ts)

            produced = unchanged = invalid = 0
            fingerprints = {}
            for entity in feed_msg.entity:
                fingerprint = hashlib.blake2b(entity.SerializeToString(deterministic=True), digest_size=16).digest()
                fingerprints[entity.id] = fingerprint
                if self._last_fingerprints.get(entity.id) == fingerprint:
                    unchanged += 1
                    continue
                try:
                    record = self.build_record(entity, header_ts, ingest_time, self.url)
                except ValidationError as e:
                    invalid += 1
                    metrics.POLL_ERRORS.labels(self.feed, "validate").inc()
                    metrics.DLQ_RECORDS.labels(self.feed, "validate").inc()
                    produce_json(self.producer, dlq_topic(self.feed), entity.id or self.feed,
                                 dlq_record(self.feed, "validate", e, self.url, ingest_time, MessageToDict(entity)))
                    continue
                produce_json(self.producer, raw_topic(self.feed), record["route_id"], record)
                produced += 1
            self._last_fingerprints = fingerprints
            remaining = self.producer.flush(30)

        metrics.ENTITIES_UNCHANGED.labels(self.feed).inc(unchanged)
        metrics.POLLS.labels(self.feed, "ok").inc()
        metrics.LAST_SUCCESS.labels(self.feed).set(time.time())
        log.info("%s: %d entities -> %d produced, %d unchanged, %d invalid%s", self.feed, len(feed_msg.entity),
                 produced, unchanged, invalid, f", {remaining} NOT FLUSHED" if remaining else "")


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(threadName)s: %(message)s")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
    interval_s = float(os.environ.get("POLL_INTERVAL_SECONDS", "15"))
    timeout_s = float(os.environ.get("HTTP_TIMEOUT_SECONDS", "10"))

    metrics.start_metrics_server(int(os.environ.get("METRICS_PORT", "8000")))
    ensure_topics(
        bootstrap,
        partitions=int(os.environ.get("KAFKA_RAW_PARTITIONS", "6")),
        raw_retention_ms=int(float(os.environ.get("KAFKA_RAW_RETENTION_HOURS", "48")) * 3600_000),
        dlq_retention_ms=int(float(os.environ.get("KAFKA_DLQ_RETENTION_HOURS", "168")) * 3600_000),
    )
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "gtfs-poller",
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "zstd",
        "linger.ms": 50,
        "partitioner": "murmur2_random",  # same key->partition hashing as Java clients
    })

    stop_event = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop_event.set())

    workers = []
    if os.environ.get("SCHEDULE_LOADER_ENABLED", "true").lower() == "true":
        loader = ScheduleLoader()
        workers.append(threading.Thread(target=loader.run_forever, args=(stop_event,), name="schedule", daemon=True))
    for feed, (url_env, default_url) in FEEDS.items():
        workers.append(FeedPoller(feed, os.environ.get(url_env, default_url), producer, interval_s, timeout_s,
                                  stop_event))
    for w in workers:
        w.start()

    while not stop_event.wait(1):
        pass
    log.info("shutting down")
    for w in workers:
        w.join(timeout=15)
    producer.flush(10)


if __name__ == "__main__":
    main()
