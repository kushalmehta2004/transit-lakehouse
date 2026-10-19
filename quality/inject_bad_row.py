"""Insert or remove the drill's bad silver row by hand (e.g. to watch the Airflow DAG go red).

    python inject_bad_row.py inject    # a stop visit with delay_seconds = 36000 (10 h)
    python inject_bad_row.py remove
"""
import sys

from bad_row_drill import DRILL_TRIP, INSERT_BAD_ROW, sql

if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else ""
    if action == "inject":
        sql(f"DELETE FROM silver.stop_delays WHERE trip_id = '{DRILL_TRIP}'")
        sql(INSERT_BAD_ROW)
        print("bad row injected (delay_seconds = 36000)")
    elif action == "remove":
        sql(f"DELETE FROM silver.stop_delays WHERE trip_id = '{DRILL_TRIP}'")
        print("bad row removed")
    else:
        sys.exit(__doc__)
