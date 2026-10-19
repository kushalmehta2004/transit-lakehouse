"""lakehouse_maintenance: keep the streaming-written Iceberg tables healthy, hourly.

Streaming appends commit every 10-30 s, so each table gains thousands of small files and
snapshots a day. Per table:
    compact   rewrite_data_files (bin-pack small files into ~128 MB files)
    expire    expire_snapshots older than 24 h (keep >= 50): bounds metadata + deletes replaced files
Silver reads bronze.trip_updates as a stream and skips compaction ("replace") snapshots. Expiring
only snapshots older than 24 h means silver can be down for up to a day and still resume.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

TABLES = ["bronze.vehicle_positions", "bronze.trip_updates", "silver.stop_delays"]

with DAG(
    dag_id="lakehouse_maintenance",
    description="Iceberg compaction + snapshot expiry",
    schedule="@hourly",
    start_date=datetime(2026, 10, 19),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=2),
                  "execution_timeout": timedelta(minutes=30)},
    doc_md=__doc__,
    tags=["maintenance", "iceberg"],
) as dag:
    for table in TABLES:
        safe = table.replace(".", "__")
        compact = BashOperator(
            task_id=f"compact__{safe}",
            bash_command=f"/opt/dbt-venv/bin/python /opt/airflow/scripts/iceberg_maintenance.py compact {table}",
        )
        expire = BashOperator(
            task_id=f"expire__{safe}",
            bash_command=f"/opt/dbt-venv/bin/python /opt/airflow/scripts/iceberg_maintenance.py expire {table}",
        )
        compact >> expire
