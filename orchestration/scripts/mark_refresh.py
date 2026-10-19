"""Record a successful gold refresh: gold.pipeline_refreshes, idempotent MERGE on run_id.

    mark_refresh.py <airflow run_id> <logical ts>
"""
import sys

import lakehouse_sql

DDL = """
CREATE TABLE IF NOT EXISTS gold.pipeline_refreshes (
    run_id        string,
    logical_ts    string,
    refreshed_at  timestamp,
    gold_rows     bigint
) USING iceberg
"""


def main(run_id: str, logical_ts: str) -> None:
    lakehouse_sql.run(DDL)
    rid = run_id.replace("'", "''")
    lakehouse_sql.run(f"""
        MERGE INTO gold.pipeline_refreshes t
        USING (SELECT '{rid}' AS run_id, '{logical_ts}' AS logical_ts, current_timestamp() AS refreshed_at,
                      (SELECT count(*) FROM gold.route_lateness_by_hour) AS gold_rows) s
        ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET refreshed_at = s.refreshed_at, gold_rows = s.gold_rows
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"refresh recorded for {run_id}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
