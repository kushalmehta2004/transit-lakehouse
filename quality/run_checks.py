"""Great Expectations checkpoint on silver.stop_delays. Exit code 1 if any expectation fails.

Pulls the rows written to silver in the last $QUALITY_WINDOW_HOURS (default 6) over the Spark
Thrift server and validates them against quality/expectations/silver_stop_delays.json.

    docker compose run --rm dbt python /work/quality/run_checks.py
"""
import json
import os
import sys
from pathlib import Path

import great_expectations as gx
import pandas as pd
from great_expectations.core import ExpectationSuite
from pyhive import hive

HERE = Path(__file__).resolve().parent
SUITE_PATH = HERE / "expectations" / "silver_stop_delays.json"
RESULTS_DIR = Path(os.environ.get("QUALITY_RESULTS_DIR", HERE / "results"))
TIMESTAMP_COLS = ["scheduled_time", "observed_time", "first_event_time", "final_event_time", "passed_at",
                  "ingest_time", "processed_time"]
INT_COLS = ["direction_id", "stop_sequence", "delay_seconds", "scheduled_hour_local",
            "prediction_horizon_seconds", "n_predictions"]


def fetch_silver(window_hours: float) -> pd.DataFrame:
    conn = hive.connect(host=os.environ.get("SPARK_THRIFT_HOST", "localhost"),
                        port=int(os.environ.get("SPARK_THRIFT_PORT", "10000")), username="quality")
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM silver.stop_delays "
            f"WHERE processed_time >= current_timestamp() - INTERVAL {int(window_hours * 60)} MINUTES"
        )
        cols = [d[0].split(".")[-1] for d in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()
    for c in TIMESTAMP_COLS:
        df[c] = pd.to_datetime(df[c])
    for c in INT_COLS:
        df[c] = pd.to_numeric(df[c]).astype("Int64")
    df["service_date"] = pd.to_datetime(df["service_date"]).dt.date
    return df


def run_checkpoint(df: pd.DataFrame) -> dict:
    context = gx.get_context(mode="ephemeral")
    suite = ExpectationSuite(**json.loads(SUITE_PATH.read_text()))
    context.add_or_update_expectation_suite(expectation_suite=suite)
    asset = context.sources.add_pandas("silver").add_dataframe_asset("stop_delays")
    checkpoint = context.add_or_update_checkpoint(
        name="silver_stop_delays_checkpoint",
        validations=[{
            "batch_request": asset.build_batch_request(dataframe=df),
            "expectation_suite_name": suite.expectation_suite_name,
        }],
    )
    return checkpoint.run().to_json_dict()


def summarize(result: dict, rows: int) -> tuple[bool, list[dict]]:
    run = next(iter(result["run_results"].values()))["validation_result"]
    lines = []
    for r in run["results"]:
        cfg = r["expectation_config"]
        kw = cfg["kwargs"]
        target = kw.get("column") or kw.get("column_list") or (
            f"{kw.get('column_A')} vs {kw.get('column_B')}" if "column_A" in kw else "table")
        lines.append({
            "expectation": cfg["expectation_type"],
            "target": target,
            "success": r["success"],
            "unexpected_count": r["result"].get("unexpected_count"),
            "observed_value": r["result"].get("observed_value"),
        })
    return run["success"], lines


def main() -> int:
    window = float(os.environ.get("QUALITY_WINDOW_HOURS", "6"))
    df = fetch_silver(window)
    success, lines = summarize(run_checkpoint(df), len(df))

    print(f"Great Expectations: silver.stop_delays, {len(df)} rows written in the last {window:g} h")
    for line in lines:
        detail = f"unexpected={line['unexpected_count']}" if line["unexpected_count"] is not None \
            else f"observed={line['observed_value']}"
        print(f"  [{'PASS' if line['success'] else 'FAIL'}] {line['expectation']} ({line['target']}) {detail}")
    print(f"RESULT: {'PASSED' if success else 'FAILED'} "
          f"({sum(l['success'] for l in lines)}/{len(lines)} expectations)")

    RESULTS_DIR.mkdir(exist_ok=True)
    (RESULTS_DIR / "latest.json").write_text(json.dumps(
        {"success": success, "rows": len(df), "window_hours": window, "expectations": lines}, indent=2, default=str))
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
