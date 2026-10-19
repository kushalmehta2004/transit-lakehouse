"""Iceberg table maintenance over the Thrift server.

    iceberg_maintenance.py compact <namespace.table>   bin-pack small files (~128 MB targets)
    iceberg_maintenance.py expire  <namespace.table>   expire snapshots older than 24 h (keep >= 50)
"""
import sys
from datetime import datetime, timedelta, timezone

import lakehouse_sql

CATALOG = "lakehouse"
EXPIRE_OLDER_THAN = timedelta(hours=24)
RETAIN_LAST = 50


def files(table: str) -> int:
    return lakehouse_sql.run(f"SELECT count(*) AS n FROM {table}.files", fetch=True)[0]["n"]


def compact(table: str) -> None:
    before = files(table)
    result = lakehouse_sql.run(
        f"CALL {CATALOG}.system.rewrite_data_files(table => '{table}', "
        "options => map('min-input-files', '5', 'target-file-size-bytes', '134217728', "
        "'partial-progress.enabled', 'true'))",
        fetch=True,
    )
    print(f"{table}: {before} data files -> {files(table)}; {result}")


def expire(table: str) -> None:
    cutoff = (datetime.now(timezone.utc) - EXPIRE_OLDER_THAN).strftime("%Y-%m-%d %H:%M:%S")
    result = lakehouse_sql.run(
        f"CALL {CATALOG}.system.expire_snapshots(table => '{table}', "
        f"older_than => TIMESTAMP '{cutoff}', retain_last => {RETAIN_LAST})",
        fetch=True,
    )
    snapshots = lakehouse_sql.run(f"SELECT count(*) AS n FROM {table}.snapshots", fetch=True)[0]["n"]
    print(f"{table}: expired snapshots older than {cutoff} UTC; {snapshots} remain; {result}")


if __name__ == "__main__":
    action, table = sys.argv[1], sys.argv[2]
    {"compact": compact, "expire": expire}[action](table)
