"""Per-trip state machine: a stream of GTFS-rt predictions -> one final row per stop visit.

MBTA publishes *predicted* arrival times only, and drops a stop from a trip's update once the
vehicle has passed it. So for each (service date, trip, stop):

    observed time = the latest prediction (by event_time) made before the stop dropped out
    passed_at     = event_time of the earliest update proving the stop was passed
                    (an update, newer than the stop's last prediction, whose lowest
                    stop_sequence is beyond this stop)

A stop is *released* (emitted) only once the watermark reaches passed_at. The watermark means
"no event older than this will be accepted any more", so after that point no late update can
still change the stop's latest prediction. That is what makes the result independent of
arrival order for anything inside the watermark. The cost is latency: rows appear about one
watermark delay after the vehicle passes.

Stops that are never proven passed (usually the last stop of a trip, or a vehicle that stops
reporting) are released EXPIRE_GRACE_S after their predicted time, as finalized_by='expired'.
After a trip has nothing pending, its state is kept as a tombstone, remembering which stops
were already emitted, until its service day is over (start_date + SERVICE_DAY_SPAN_S, at
least TOMBSTONE_TTL_S after the last update). Trips can vanish from the feed for hours and come
back: MBTA announced a skipped stop for trip 78854570 four hours ahead, the trip disappeared,
and it reappeared two hours later. A shorter tombstone re-emitted that stop.

This module is pure Python/pandas so it can be unit-tested without Spark.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd

EXPIRE_GRACE_S = 600
TOMBSTONE_TTL_S = 3600
# A GTFS service day can run past midnight (times like 27:30:00). Measured from start_date
# 00:00 UTC, 36 h covers that in any US timezone.
SERVICE_DAY_SPAN_S = 36 * 3600


@dataclass
class StopUpdate:
    stop_sequence: int
    stop_id: str | None
    arrival_time: int | None    # epoch seconds
    departure_time: int | None  # epoch seconds
    schedule_relationship: str | None


@dataclass
class TripUpdate:
    event_ms: int
    ingest_ms: int
    route_id: str | None
    direction_id: int | None
    vehicle_id: str | None
    trip_schedule_relationship: str | None
    stops: list[StopUpdate]


OUTPUT_COLUMNS = [
    "start_date", "trip_id", "route_id", "direction_id", "vehicle_id", "trip_schedule_relationship",
    "stop_sequence", "stop_id", "stop_schedule_relationship", "time_kind", "observed_time_s",
    "final_event_ms", "final_ingest_ms", "first_event_ms", "passed_at_ms", "n_predictions", "finalized_by",
]
INT_COLUMNS = ["direction_id", "stop_sequence", "observed_time_s", "final_event_ms", "final_ingest_ms",
               "first_event_ms", "passed_at_ms", "n_predictions"]


class TripTracker:
    def __init__(self, start_date: str, trip_id: str):
        self.start_date = start_date
        self.trip_id = trip_id
        self.route_id: str | None = None
        self.direction_id: int | None = None
        self.vehicle_id: str | None = None
        self.trip_schedule_relationship: str | None = None
        self.last_event_ms: int | None = None
        self.canceled = False
        self.pending: dict[int, dict] = {}
        self.finalized: set[int] = set()
        self.proofs: list[list[int]] = []  # [event_ms, lowest stop_sequence in that update]
        self.ignored = 0  # updates/stops dropped as late or for already-finalized stops

    # ------------------------------------------------------------ updates
    def apply(self, u: TripUpdate, watermark_ms: int) -> None:
        if u.event_ms < watermark_ms:  # older than the watermark: too late to be counted
            self.ignored += 1
            return
        newest = self.last_event_ms is None or u.event_ms >= self.last_event_ms
        if newest:
            self.last_event_ms = u.event_ms
            self.route_id = u.route_id or self.route_id
            self.direction_id = u.direction_id if u.direction_id is not None else self.direction_id
            self.vehicle_id = u.vehicle_id or self.vehicle_id
            self.trip_schedule_relationship = u.trip_schedule_relationship
            self.canceled = u.trip_schedule_relationship == "CANCELED"
            if self.canceled:  # a canceled trip has no stop visits to report
                self.pending.clear()
                return
        elif self.canceled:
            return

        for s in u.stops:
            if s.stop_sequence in self.finalized:
                self.ignored += 1
                continue
            p = self.pending.get(s.stop_sequence)
            if p is None:
                p = self.pending[s.stop_sequence] = {"n": 0, "first_event_ms": u.event_ms, "passed_at_ms": None,
                                                     "event_ms": -1}
            p["n"] += 1
            p["first_event_ms"] = min(p["first_event_ms"], u.event_ms)
            if u.event_ms >= p["event_ms"]:
                p.update(self._prediction(s, u))

        # Every update is a "proof" that stops below its lowest sequence were passed by its event
        # time. Proofs are kept (until the watermark passes them) and passed_at is recomputed from
        # all of them, so the result doesn't depend on which update happened to arrive first.
        if u.stops:
            self.proofs.append([u.event_ms, min(s.stop_sequence for s in u.stops)])
        for seq, p in self.pending.items():
            p["passed_at_ms"] = min(
                (ev for ev, min_seq in self.proofs if min_seq > seq and ev > p["event_ms"]), default=None
            )

    @staticmethod
    def _prediction(s: StopUpdate, u: TripUpdate) -> dict:
        if s.arrival_time is not None:
            kind, t = "arrival", s.arrival_time
        elif s.departure_time is not None:  # first stop of a trip only has a departure
            kind, t = "departure", s.departure_time
        else:
            kind, t = None, None
        return {"stop_id": s.stop_id, "time_kind": kind, "time_s": t, "sr": s.schedule_relationship,
                "event_ms": u.event_ms, "ingest_ms": u.ingest_ms}

    # ------------------------------------------------------------ release
    @staticmethod
    def _close_at_ms(p: dict) -> int:
        if p["passed_at_ms"] is not None:
            return p["passed_at_ms"]
        anchor_ms = p["time_s"] * 1000 if p["time_s"] is not None else p["event_ms"]
        return anchor_ms + EXPIRE_GRACE_S * 1000

    def release(self, watermark_ms: int) -> list[dict]:
        out = []
        for seq in sorted(self.pending):
            p = self.pending[seq]
            if self._close_at_ms(p) <= watermark_ms:
                out.append(self._final_row(seq, p))
        for row in out:
            del self.pending[row["stop_sequence"]]
            self.finalized.add(row["stop_sequence"])
        # Proofs older than the watermark can't matter any more: any stop they could prove passed
        # has a passed_at <= watermark and was released just above.
        self.proofs = [pr for pr in self.proofs if pr[0] >= watermark_ms]
        return out

    def _final_row(self, seq: int, p: dict) -> dict:
        return {
            "start_date": self.start_date, "trip_id": self.trip_id, "route_id": self.route_id,
            "direction_id": self.direction_id, "vehicle_id": self.vehicle_id,
            "trip_schedule_relationship": self.trip_schedule_relationship,
            "stop_sequence": seq, "stop_id": p["stop_id"], "stop_schedule_relationship": p["sr"],
            "time_kind": p["time_kind"], "observed_time_s": p["time_s"],
            "final_event_ms": p["event_ms"], "final_ingest_ms": p["ingest_ms"],
            "first_event_ms": p["first_event_ms"], "passed_at_ms": p["passed_at_ms"], "n_predictions": p["n"],
            "finalized_by": "passed" if p["passed_at_ms"] is not None else "expired",
        }

    def _tombstone_until_ms(self) -> int:
        try:
            day_start_ms = int(datetime.strptime(self.start_date, "%Y%m%d").replace(tzinfo=timezone.utc).timestamp()) * 1000
        except (TypeError, ValueError):
            day_start_ms = 0
        return max(day_start_ms + SERVICE_DAY_SPAN_S * 1000, (self.last_event_ms or 0) + TOMBSTONE_TTL_S * 1000)

    def next_deadline_ms(self) -> int:
        if self.pending:
            return min(self._close_at_ms(p) for p in self.pending.values())
        return self._tombstone_until_ms()

    def can_drop(self, watermark_ms: int) -> bool:
        return not self.pending and watermark_ms >= self._tombstone_until_ms()

    # ------------------------------------------------------------ (de)serialization for GroupState
    def to_json(self) -> str:
        d = {k: v for k, v in self.__dict__.items() if k not in ("pending", "finalized")}
        d["pending"] = {str(k): v for k, v in self.pending.items()}
        d["finalized"] = sorted(self.finalized)
        return json.dumps(d, separators=(",", ":"))

    @classmethod
    def from_json(cls, raw: str) -> TripTracker:
        d = json.loads(raw)
        t = cls(d["start_date"], d["trip_id"])
        for k, v in d.items():
            if k not in ("pending", "finalized"):
                setattr(t, k, v)
        t.pending = {int(k): v for k, v in d["pending"].items()}
        t.finalized = set(d["finalized"])
        return t


# ---------------------------------------------------------------- pandas adapters

def _none(v):
    return None if v is None or pd.isna(v) else v


def _epoch_ms(col: pd.Series) -> pd.Series:
    # Unit-agnostic: Spark may hand pandas datetime64[us] or [ns] depending on versions.
    return (pd.to_datetime(col) - pd.Timestamp(0)) // pd.Timedelta(milliseconds=1)


def updates_from_frame(pdf: pd.DataFrame) -> list[TripUpdate]:
    """Flat rows (one per stop prediction; event_time/ingest_time as datetime64) -> TripUpdates."""
    pdf = pdf.assign(_event_ms=_epoch_ms(pdf["event_time"]), _ingest_ms=_epoch_ms(pdf["ingest_time"]))
    updates = []
    for event_ms, g in pdf.groupby("_event_ms", sort=True):
        first = g.iloc[0]
        stops = [
            StopUpdate(int(r.stop_sequence), _none(r.stop_id),
                       None if pd.isna(r.arrival_time) else int(r.arrival_time),
                       None if pd.isna(r.departure_time) else int(r.departure_time),
                       _none(r.stop_schedule_relationship))
            for r in g.itertuples(index=False) if not pd.isna(r.stop_sequence)
        ]
        updates.append(TripUpdate(
            event_ms=int(event_ms), ingest_ms=int(g["_ingest_ms"].max()),
            route_id=_none(first.route_id),
            direction_id=None if pd.isna(first.direction_id) else int(first.direction_id),
            vehicle_id=_none(first.vehicle_id),
            trip_schedule_relationship=_none(first.trip_schedule_relationship),
            stops=stops,
        ))
    return updates


def finals_frame(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    return df.astype({c: "Int64" for c in INT_COLUMNS})
