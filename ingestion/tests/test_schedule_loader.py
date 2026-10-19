import zipfile

import pyarrow as pa
import pytest

from schedule_loader import gtfs_time_to_seconds, parse_gtfs_zip


def test_gtfs_time_to_seconds_keeps_after_midnight_hours():
    out = gtfs_time_to_seconds(pa.array(["05:19:00", "25:10:00", " 7:05:30", None]))
    assert out.to_pylist() == [19140, 90600, 25530, None]


def _write_zip(path, files):
    with zipfile.ZipFile(path, "w") as zf:
        for name, text in files.items():
            zf.writestr(name, text)


MINIMAL = {
    # BOM on the first header must not rename stop_id
    "stops.txt": "﻿stop_id,stop_name,stop_lat,stop_lon\n1,A,42.1,-71.1\n",
    "routes.txt": "route_id,route_type\nRed,1\n",
    "trips.txt": "route_id,service_id,trip_id,direction_id\nRed,WK,t1,0\n",
    "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                      "t1,23:59:00,23:59:30,1,1\nt1,24:20:00,24:20:00,1,2\n",
    "calendar_dates.txt": "service_id,date,exception_type\nWK,20261019,1\n",
}


def test_parse_gtfs_zip(tmp_path):
    p = tmp_path / "gtfs.zip"
    _write_zip(p, MINIMAL)
    tables = parse_gtfs_zip(p)
    assert tables["stops"]["stop_id"].to_pylist() == ["1"]
    st = tables["stop_times"]
    assert st["arrival_secs"].to_pylist() == [86340, 87600]
    assert st["stop_sequence"].type == pa.int32()
    assert "calendar" not in tables  # optional table absent -> skipped
    assert tables["trips"]["route_id"].to_pylist() == ["Red"]


def test_parse_gtfs_zip_missing_required(tmp_path):
    p = tmp_path / "gtfs.zip"
    _write_zip(p, {k: v for k, v in MINIMAL.items() if k != "trips.txt"})
    with pytest.raises(ValueError, match="trips.txt"):
        parse_gtfs_zip(p)
