"""Static GTFS schedule -> reference parquet (local cache + MinIO).

Downloads the agency's static GTFS zip, keeps only the columns the pipeline needs and writes
one parquet file per table to:

    local:  $SCHEDULE_CACHE_DIR/<table>.parquet
    MinIO:  s3://$LAKEHOUSE_BUCKET/reference/gtfs_static/<table>/<table>.parquet

plus a _manifest.json (etag, feed_version, row counts, loaded_at). The MinIO copy is what
Spark reads in phase 3, so the poller and Spark share the schedule without a shared disk.

GTFS times can exceed 24:00:00: a trip on service day D that runs past midnight is written as
e.g. "24:20:00". We keep the raw string and add <col>_secs = seconds since the start of the
service day, which may be >= 86400. Turning that into a timestamp needs the service date
(the realtime trip's start_date), which happens in phase 3.

Run standalone with `python schedule_loader.py [--force]`.
"""
import argparse
import io
import json
import logging
import os
import threading
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import requests

import metrics

log = logging.getLogger("schedule_loader")

DEFAULT_STATIC_URL = "https://cdn.mbta.com/MBTA_GTFS.zip"
S3_PREFIX = "reference/gtfs_static"

# table -> (columns to keep, {column: cast type}, required?)
TABLES = {
    "stops": (
        ["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station"],
        {"stop_lat": pa.float64(), "stop_lon": pa.float64(), "location_type": pa.int8()},
        True,
    ),
    "routes": (
        ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"],
        {"route_type": pa.int16()},
        True,
    ),
    "trips": (
        ["trip_id", "route_id", "service_id", "direction_id", "trip_headsign"],
        {"direction_id": pa.int8()},
        True,
    ),
    "stop_times": (
        ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"],
        {"stop_sequence": pa.int32()},
        True,
    ),
    "calendar": (
        ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
         "start_date", "end_date"],
        {d: pa.int8() for d in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]},
        False,
    ),
    "calendar_dates": (
        ["service_id", "date", "exception_type"],
        {"exception_type": pa.int8()},
        False,
    ),
}
# Columns that must be fully populated, or the parse is treated as broken.
KEY_COLUMNS = {"stops": "stop_id", "routes": "route_id", "trips": "trip_id", "stop_times": "trip_id"}


def gtfs_time_to_seconds(arr: pa.Array) -> pa.Array:
    """'HH:MM:SS' -> seconds since service-day start. Keeps hours >= 24; nulls stay null."""
    parts = pc.split_pattern(pc.utf8_trim_whitespace(arr), ":")
    h, m, s = (pc.cast(pc.list_element(parts, i), pa.int32()) for i in range(3))
    return pc.add(pc.add(pc.multiply(h, 3600), pc.multiply(m, 60)), s)


def _read_csv(raw: bytes, columns: list[str]) -> pa.Table:
    # Read the header ourselves so a UTF-8 BOM can't silently rename the first column.
    text_header, _, _ = raw.partition(b"\n")
    header = [c.strip().strip('"') for c in text_header.decode("utf-8-sig").strip().split(",")]
    return pacsv.read_csv(
        io.BytesIO(raw),
        read_options=pacsv.ReadOptions(column_names=header, skip_rows=1, block_size=1 << 26),
        convert_options=pacsv.ConvertOptions(
            include_columns=columns,
            include_missing_columns=True,
            column_types={c: pa.string() for c in columns},
            strings_can_be_null=True,
        ),
    )


def parse_gtfs_zip(path: Path) -> dict[str, pa.Table]:
    tables = {}
    with zipfile.ZipFile(path) as zf:
        names = set(zf.namelist())
        for table, (columns, casts, required) in TABLES.items():
            fname = f"{table}.txt"
            if fname not in names:
                if required:
                    raise ValueError(f"static GTFS is missing required file {fname}")
                log.warning("static GTFS has no %s; skipping", fname)
                continue
            tbl = _read_csv(zf.read(fname), columns)
            for col, typ in casts.items():
                tbl = tbl.set_column(tbl.schema.get_field_index(col), col, pc.cast(tbl[col], typ))
            if table == "stop_times":
                for col in ("arrival_time", "departure_time"):
                    tbl = tbl.append_column(f"{col.removesuffix('_time')}_secs", gtfs_time_to_seconds(tbl[col]))
            key = KEY_COLUMNS.get(table)
            if key and tbl[key].null_count == tbl.num_rows:
                raise ValueError(f"{fname}: key column {key} is entirely null — header mismatch?")
            tables[table] = tbl
        if "feed_info.txt" in names:
            info = _read_csv(zf.read("feed_info.txt"), ["feed_version", "feed_start_date", "feed_end_date"])
            tables["_feed_info"] = info
    return tables


def s3_filesystem() -> pafs.S3FileSystem:
    scheme, host = os.environ.get("S3_ENDPOINT", "http://minio:9000").split("://", 1)
    return pafs.S3FileSystem(
        access_key=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        endpoint_override=host,
        scheme=scheme,
        region=os.environ.get("AWS_REGION", "us-east-1"),
    )


class ScheduleLoader:
    def __init__(self):
        self.url = os.environ.get("GTFS_STATIC_URL", DEFAULT_STATIC_URL)
        self.cache_dir = Path(os.environ.get("SCHEDULE_CACHE_DIR", "/data/schedule"))
        self.bucket = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")
        self.refresh_s = float(os.environ.get("SCHEDULE_REFRESH_HOURS", "24")) * 3600
        self.s3 = s3_filesystem()

    @property
    def _manifest_path(self) -> str:
        return f"{self.bucket}/{S3_PREFIX}/_manifest.json"

    def read_manifest(self) -> dict | None:
        try:
            with self.s3.open_input_stream(self._manifest_path) as f:
                return json.loads(f.read())
        except (FileNotFoundError, OSError):
            return None

    def _remote_etag(self) -> str | None:
        try:
            resp = requests.head(self.url, timeout=30, allow_redirects=True)
            resp.raise_for_status()
            return resp.headers.get("ETag") or resp.headers.get("Last-Modified")
        except requests.RequestException as e:
            log.warning("HEAD %s failed: %s", self.url, e)
            return None

    def is_stale(self, manifest: dict | None, etag: str | None) -> bool:
        if manifest is None:
            return True
        age = time.time() - manifest.get("loaded_at_epoch", 0)
        if age >= self.refresh_s:
            return True
        return etag is not None and etag != manifest.get("etag")

    def _download(self) -> Path:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        dest, tmp = self.cache_dir / "gtfs.zip", self.cache_dir / "gtfs.zip.tmp"
        with requests.get(self.url, timeout=(10, 120), stream=True) as resp:
            resp.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in resp.iter_content(1 << 20):
                    f.write(chunk)
        tmp.replace(dest)
        return dest

    def load(self, etag: str | None) -> dict:
        t0 = time.perf_counter()
        log.info("downloading static GTFS from %s", self.url)
        tables = parse_gtfs_zip(self._download())
        feed_info = tables.pop("_feed_info", None)

        row_counts = {}
        for name, tbl in tables.items():
            pq.write_table(tbl, self.cache_dir / f"{name}.parquet", compression="zstd")
            pq.write_table(tbl, f"{self.bucket}/{S3_PREFIX}/{name}/{name}.parquet",
                           filesystem=self.s3, compression="zstd")
            row_counts[name] = tbl.num_rows
            metrics.SCHEDULE_ROWS.labels(name).set(tbl.num_rows)

        now = time.time()
        manifest = {
            "source_url": self.url,
            "etag": etag,
            "feed_version": feed_info["feed_version"][0].as_py() if feed_info is not None and feed_info.num_rows else None,
            "feed_start_date": feed_info["feed_start_date"][0].as_py() if feed_info is not None and feed_info.num_rows else None,
            "feed_end_date": feed_info["feed_end_date"][0].as_py() if feed_info is not None and feed_info.num_rows else None,
            "row_counts": row_counts,
            "loaded_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
            "loaded_at_epoch": now,
        }
        with self.s3.open_output_stream(self._manifest_path) as f:
            f.write(json.dumps(manifest, indent=2).encode())
        (self.cache_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2))

        metrics.SCHEDULE_LOADS.labels("loaded").inc()
        metrics.SCHEDULE_LAST_LOAD.set(now)
        log.info("static GTFS loaded in %.1fs: %s", time.perf_counter() - t0, row_counts)
        return manifest

    def load_if_stale(self, force: bool = False) -> bool:
        manifest = self.read_manifest()
        etag = self._remote_etag()
        if not force and not self.is_stale(manifest, etag):
            log.info("static GTFS unchanged (etag %s, version %s); skipping", etag, manifest.get("feed_version"))
            metrics.SCHEDULE_LOADS.labels("unchanged").inc()
            metrics.SCHEDULE_LAST_LOAD.set(manifest.get("loaded_at_epoch", 0))
            return False
        self.load(etag)
        return True

    def run_forever(self, stop_event: threading.Event, check_interval_s: float = 3600) -> None:
        """Check hourly (cheap HEAD); reload when the etag changes or the copy is > refresh age."""
        while not stop_event.is_set():
            try:
                self.load_if_stale()
            except Exception:
                log.exception("static GTFS load failed; keeping the previous copy")
                metrics.SCHEDULE_LOADS.labels("error").inc()
            stop_event.wait(check_interval_s)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Load the static GTFS schedule into parquet.")
    parser.add_argument("--force", action="store_true", help="reload even if the etag is unchanged")
    ScheduleLoader().load_if_stale(force=parser.parse_args().force)
