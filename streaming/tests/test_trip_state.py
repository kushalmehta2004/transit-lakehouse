import random

import pandas as pd
import pytest

from trip_state import (
    EXPIRE_GRACE_S,
    TOMBSTONE_TTL_S,
    StopUpdate,
    TripTracker,
    TripUpdate,
    finals_frame,
    updates_from_frame,
)

T0 = 1_792_426_000_000  # ms
MIN = 60_000
WATERMARK_DELAY = 15 * MIN


def upd(minute, stops, sr="SCHEDULED"):
    """An update at T0+minute whose stops are (seq, predicted arrival in epoch seconds)."""
    ev = T0 + minute * MIN
    return TripUpdate(ev, ev + 5_000, "85", 0, "y3212", sr,
                      [StopUpdate(seq, f"s{seq}", t, None, "SCHEDULED") for seq, t in stops])


# a vehicle working through stops 1..4; predictions drift a little each update
TRIP = [
    upd(0, [(1, 1_792_426_300), (2, 1_792_426_600), (3, 1_792_426_900), (4, 1_792_427_200)]),
    upd(2, [(1, 1_792_426_320), (2, 1_792_426_630), (3, 1_792_426_930), (4, 1_792_427_230)]),
    upd(5, [(2, 1_792_426_650), (3, 1_792_426_950), (4, 1_792_427_250)]),   # stop 1 passed
    upd(8, [(2, 1_792_426_660), (3, 1_792_426_960), (4, 1_792_427_260)]),
    upd(11, [(3, 1_792_426_990), (4, 1_792_427_290)]),                      # stop 2 passed
    upd(16, [(4, 1_792_427_300)]),                                          # stop 3 passed
]


def run(updates, watermark_delay=WATERMARK_DELAY):
    """Feed updates one per 'batch' like Spark would, with watermark = max seen - delay."""
    tracker, out, max_ev = TripTracker("20261019", "t1"), [], 0
    for u in updates:
        wm = max(max_ev - watermark_delay, 0)
        tracker.apply(u, wm)
        out += tracker.release(wm)
        max_ev = max(max_ev, u.event_ms)
    out += tracker.release(10**15)  # end of stream: flush everything
    return tracker, out


def key(rows):
    return sorted((r["stop_sequence"], r["observed_time_s"], r["final_event_ms"], r["finalized_by"],
                   r["passed_at_ms"]) for r in rows)


def test_in_order_final_prediction_is_last_before_passing():
    _, rows = run(TRIP)
    by_seq = {r["stop_sequence"]: r for r in rows}
    assert by_seq[1]["observed_time_s"] == 1_792_426_320 and by_seq[1]["passed_at_ms"] == T0 + 5 * MIN
    assert by_seq[2]["observed_time_s"] == 1_792_426_660 and by_seq[2]["finalized_by"] == "passed"
    assert by_seq[4]["finalized_by"] == "expired"  # last stop is never "passed" by a later update
    assert by_seq[1]["n_predictions"] == 2 and by_seq[1]["first_event_ms"] == T0


def test_stop_is_held_until_watermark_reaches_passed_at():
    tracker = TripTracker("20261019", "t1")
    for u in TRIP[:3]:
        tracker.apply(u, 0)
    passed_at = T0 + 5 * MIN
    assert tracker.release(passed_at - 1) == []
    assert [r["stop_sequence"] for r in tracker.release(passed_at)] == [1]


@pytest.mark.parametrize("seed", range(200))
def test_reordering_within_watermark_gives_identical_results(seed):
    shuffled = TRIP[:]
    random.Random(seed).shuffle(shuffled)  # whole trip spans 16 min; delay 20 min covers any order
    assert key(run(shuffled, watermark_delay=20 * MIN)[1]) == key(run(TRIP, watermark_delay=20 * MIN)[1])


def test_reordering_with_15_min_watermark_matches_in_order():
    reordered = [TRIP[0], TRIP[2], TRIP[1], TRIP[4], TRIP[3], TRIP[5]]  # each swap is 3 min late
    assert key(run(reordered)[1]) == key(run(TRIP)[1])


def test_event_later_than_watermark_is_ignored():
    tracker = TripTracker("20261019", "t1")
    tracker.apply(TRIP[0], watermark_ms=0)
    late = upd(-20, [(1, 1_792_426_999)])
    tracker.apply(late, watermark_ms=T0 - 5 * MIN)  # 20 min old vs a watermark 5 min behind T0
    assert tracker.ignored == 1
    assert tracker.pending[1]["time_s"] == 1_792_426_300


def test_update_for_already_finalized_stop_is_ignored():
    tracker, _ = run(TRIP)
    before = tracker.ignored
    tracker.apply(upd(30, [(1, 1_792_499_999)]), watermark_ms=T0 + 15 * MIN)
    assert 1 not in tracker.pending and tracker.ignored == before + 1


def test_canceled_trip_emits_nothing():
    tracker = TripTracker("20261019", "t1")
    tracker.apply(TRIP[0], 0)
    tracker.apply(upd(3, [], sr="CANCELED"), 0)
    assert tracker.release(10**15) == [] and tracker.canceled


def test_expiry_and_tombstone_lifecycle():
    tracker = TripTracker("20261019", "t1")
    tracker.apply(upd(0, [(9, 1_792_426_300)]), 0)
    expire_at = 1_792_426_300_000 + EXPIRE_GRACE_S * 1000
    assert tracker.next_deadline_ms() == expire_at
    assert tracker.release(expire_at)[0]["finalized_by"] == "expired"
    assert not tracker.can_drop(expire_at)
    assert tracker.next_deadline_ms() == T0 + TOMBSTONE_TTL_S * 1000
    assert tracker.can_drop(T0 + TOMBSTONE_TTL_S * 1000)


def test_json_roundtrip_preserves_state():
    tracker = TripTracker("20261019", "t1")
    for u in TRIP[:4]:
        tracker.apply(u, 0)
    restored = TripTracker.from_json(tracker.to_json())
    assert restored.__dict__ == tracker.__dict__


def test_pandas_adapters():
    ts = pd.Timestamp("2026-10-19T16:06:40")
    pdf = pd.DataFrame({
        "event_time": [ts, ts, ts + pd.Timedelta(seconds=30)],
        "ingest_time": [ts + pd.Timedelta(seconds=5)] * 3,
        "route_id": ["85"] * 3, "direction_id": [0, 0, 0], "vehicle_id": ["y1", "y1", None],
        "trip_schedule_relationship": ["SCHEDULED"] * 3,
        "stop_sequence": [1, 2, None], "stop_id": ["a", "b", None],
        "arrival_time": [None, 1_792_426_700.0, None], "departure_time": [1_792_426_650.0, None, None],
        "stop_schedule_relationship": ["SCHEDULED", "SCHEDULED", None],
    })
    updates = updates_from_frame(pdf)
    assert [len(u.stops) for u in updates] == [2, 0]
    assert updates[0].stops[0].departure_time == 1_792_426_650 and updates[0].event_ms == int(ts.value // 1e6)
    tracker = TripTracker("20261019", "t1")
    for u in updates:
        tracker.apply(u, 0)
    frame = finals_frame(tracker.release(10**15))
    assert list(frame["time_kind"]) == ["departure", "arrival"]
    assert str(frame["passed_at_ms"].dtype) == "Int64"


@pytest.mark.parametrize("unit", ["ns", "us", "ms"])
def test_epoch_ms_independent_of_datetime_unit(unit):
    # Regression: Spark handed us datetime64[us]; `astype(int64) // 1e6` then yielded seconds.
    ts = pd.Series(pd.to_datetime(["2026-10-19T16:06:40.123"])).astype(f"datetime64[{unit}]")
    pdf = pd.DataFrame({
        "event_time": ts, "ingest_time": ts, "route_id": ["85"], "direction_id": [0], "vehicle_id": ["y1"],
        "trip_schedule_relationship": ["SCHEDULED"], "stop_sequence": [1], "stop_id": ["a"],
        "arrival_time": [1_792_426_700], "departure_time": [None], "stop_schedule_relationship": ["SCHEDULED"],
    })
    assert updates_from_frame(pdf)[0].event_ms == 1_792_426_000_123
