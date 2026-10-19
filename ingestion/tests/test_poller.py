import pytest
from google.transit import gtfs_realtime_pb2

from poller import (
    FeedDecodeError,
    ValidationError,
    decode_feed,
    to_iso,
    trip_update_record,
    vehicle_position_record,
)

HEADER_TS = 1792426035
INGEST = "2026-10-19T16:07:20.000Z"
SRC = "https://example/feed.pb"


def _vehicle_entity(**overrides):
    e = gtfs_realtime_pb2.FeedEntity(id="y3212")
    v = e.vehicle
    v.trip.trip_id, v.trip.route_id, v.trip.start_date, v.trip.direction_id = "78174219", "85", "20261019", 0
    v.vehicle.id, v.vehicle.label = "y3212", "3212"
    v.position.latitude, v.position.longitude, v.position.bearing = 42.3364143, -71.0897903, 45
    v.current_stop_sequence, v.stop_id = 1, "17871"
    v.current_status = gtfs_realtime_pb2.VehiclePosition.STOPPED_AT
    v.timestamp = 1792426018
    for path, value in overrides.items():  # e.g. {"trip.route_id": None} clears v.trip.route_id
        *parents, leaf = path.split(".")
        target = v
        for p in parents:
            target = getattr(target, p)
        if value is None:
            target.ClearField(leaf)
        else:
            setattr(target, leaf, value)
    return e


def test_to_iso_is_utc_millis():
    assert to_iso(1792426018) == "2026-10-19T16:06:58.000Z"


def test_vehicle_position_record():
    r = vehicle_position_record(_vehicle_entity(), HEADER_TS, INGEST, SRC)
    assert r["route_id"] == "85" and r["vehicle_id"] == "y3212" and r["trip_id"] == "78174219"
    assert r["event_time"] == "2026-10-19T16:06:58.000Z" and r["event_time_source"] == "entity"
    assert r["ingest_time"] == INGEST and r["feed_timestamp"] == to_iso(HEADER_TS)
    assert r["current_status"] == "STOPPED_AT" and r["occupancy_status"] is None
    assert r["trip_schedule_relationship"] == "SCHEDULED"
    assert r["latitude"] == pytest.approx(42.3364143)


def test_event_time_falls_back_to_header():
    r = vehicle_position_record(_vehicle_entity(timestamp=None), HEADER_TS, INGEST, SRC)
    assert r["event_time"] == to_iso(HEADER_TS) and r["event_time_source"] == "header"


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"trip.route_id": None}, "route_id"),
        ({"vehicle.id": None}, "vehicle"),
        ({"position.latitude": 0.0, "position.longitude": 0.0}, "position"),
    ],
)
def test_vehicle_position_validation(overrides, message):
    with pytest.raises(ValidationError, match=message):
        vehicle_position_record(_vehicle_entity(**overrides), HEADER_TS, INGEST, SRC)


def test_no_timestamp_anywhere_is_invalid():
    with pytest.raises(ValidationError, match="timestamp"):
        vehicle_position_record(_vehicle_entity(timestamp=None), None, INGEST, SRC)


def test_trip_update_record():
    e = gtfs_realtime_pb2.FeedEntity(id="78174219")
    tu = e.trip_update
    tu.trip.trip_id, tu.trip.route_id, tu.trip.start_date = "78174219", "85", "20261019"
    tu.timestamp = 1792426030
    s1 = tu.stop_time_update.add(stop_sequence=1, stop_id="17871")
    s1.departure.time = 1792426500
    s2 = tu.stop_time_update.add(stop_sequence=2, stop_id="1798")
    s2.arrival.time = 1792426622
    s2.schedule_relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED

    r = trip_update_record(e, HEADER_TS, INGEST, SRC)
    assert r["vehicle_id"] is None and r["trip_delay"] is None
    first, second = r["stop_time_updates"]
    assert first == {
        "stop_sequence": 1, "stop_id": "17871", "arrival_time": None, "arrival_delay": None,
        "arrival_uncertainty": None, "departure_time": 1792426500, "departure_delay": None,
        "departure_uncertainty": None, "schedule_relationship": "SCHEDULED",
    }
    assert second["arrival_time"] == 1792426622 and second["schedule_relationship"] == "SKIPPED"


def test_decode_feed_rejects_garbage():
    with pytest.raises(FeedDecodeError):
        decode_feed(b"<html>not a feed</html>")
    with pytest.raises(FeedDecodeError):
        decode_feed(b"")


def test_decode_feed_roundtrip():
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version, msg.header.timestamp = "2.0", HEADER_TS
    msg.entity.append(_vehicle_entity())
    assert len(decode_feed(msg.SerializeToString()).entity) == 1
