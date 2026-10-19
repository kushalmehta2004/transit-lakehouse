"""Quality-gate drill: prove the gates catch bad data, then clean up.

1. INSERT a silver row with an impossible delay (+10 h, finalized_by='passed').
2. Run `dbt test --select source:silver` and the GE checkpoint -> both must FAIL.
3. DELETE the row; run both again -> both must PASS.
Exit 0 only if all four outcomes are as expected.

    docker compose run --rm dbt python /work/quality/bad_row_drill.py
"""
import os
import subprocess
import sys

from pyhive import hive

DRILL_TRIP = "__quality_drill__"
INSERT_BAD_ROW = f"""
INSERT INTO silver.stop_delays
SELECT
    current_date()                                   AS service_date,
    '1'                                              AS route_id,
    0                                                AS direction_id,
    '{DRILL_TRIP}'                                   AS trip_id,
    'drill-vehicle'                                  AS vehicle_id,
    1                                                AS stop_sequence,
    '64'                                             AS stop_id,
    '64'                                             AS scheduled_stop_id,
    'arrival'                                        AS time_kind,
    current_timestamp() - INTERVAL 10 HOURS          AS scheduled_time,
    current_timestamp()                              AS observed_time,
    36000                                            AS delay_seconds,
    hour(current_timestamp())                        AS scheduled_hour_local,
    0                                                AS prediction_horizon_seconds,
    5                                                AS n_predictions,
    current_timestamp() - INTERVAL 5 MINUTES         AS first_event_time,
    current_timestamp() - INTERVAL 1 MINUTE          AS final_event_time,
    current_timestamp()                              AS passed_at,
    'passed'                                         AS finalized_by,
    'SCHEDULED'                                      AS trip_schedule_relationship,
    'SCHEDULED'                                      AS stop_schedule_relationship,
    true                                             AS schedule_matched,
    current_timestamp() - INTERVAL 1 MINUTE          AS ingest_time,
    current_timestamp()                              AS processed_time
"""


def sql(statement: str) -> None:
    conn = hive.connect(host=os.environ.get("SPARK_THRIFT_HOST", "localhost"),
                        port=int(os.environ.get("SPARK_THRIFT_PORT", "10000")), username="drill")
    try:
        conn.cursor().execute(statement)
    finally:
        conn.close()


def gates() -> dict[str, bool]:
    """Run both gates; return {gate: passed}."""
    dbt = subprocess.run(["dbt", "test", "--select", "source:silver"], cwd="/work/dbt",
                         capture_output=True, text=True)
    print("\n".join(l for l in dbt.stdout.splitlines() if " PASS " in l or " FAIL " in l or "Done." in l))
    ge = subprocess.run([sys.executable, "/work/quality/run_checks.py"], capture_output=True, text=True)
    print("\n".join(l for l in ge.stdout.splitlines() if "FAIL" in l or "RESULT" in l))
    return {"dbt test": dbt.returncode == 0, "great_expectations": ge.returncode == 0}


def main() -> int:
    sql(f"DELETE FROM silver.stop_delays WHERE trip_id = '{DRILL_TRIP}'")  # leftover from an aborted drill
    print(">>> injecting bad row: delay_seconds = 36000 (10 h)")
    sql(INSERT_BAD_ROW)
    try:
        with_bad = gates()
    finally:
        print(">>> removing bad row")
        sql(f"DELETE FROM silver.stop_delays WHERE trip_id = '{DRILL_TRIP}'")
    clean = gates()

    expected = {f"{g} fails on bad row": not ok for g, ok in with_bad.items()}
    expected |= {f"{g} passes after cleanup": ok for g, ok in clean.items()}
    print("\n================ quality drill ================")
    for name, ok in expected.items():
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    return 0 if all(expected.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
