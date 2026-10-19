"""Generates dashboards/pipeline.json (provisioned into Grafana). Edit here, then:

    python observability/grafana/build_dashboard.py

Conventions: Grafana's dark theme, so series use the dark steps of a validated categorical
palette in fixed order (an entity keeps its color on every panel); status colors only for
state (ok / warn / failed); 2px lines, no points, no fills; one unit per chart.
"""
import json
from pathlib import Path

SERIES = ["#3987e5", "#d95926", "#199e70", "#c98500"]  # blue, orange, aqua, yellow (validated, dark)
GOOD, WARNING, CRITICAL = "#0ca30c", "#fab219", "#d03b3b"
DS = {"type": "prometheus", "uid": "prometheus"}

panels: list[dict] = []
_next_id = 1


def _id() -> int:
    global _next_id
    _next_id += 1
    return _next_id


def row(title: str, y: int) -> None:
    panels.append({"type": "row", "title": title, "id": _id(), "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []})


def target(expr: str, legend: str = "", ref: str = "A", instant: bool = False) -> dict:
    return {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref,
            "instant": instant, "range": not instant}


def color_overrides(names_to_colors: dict[str, str]) -> list[dict]:
    return [{"matcher": {"id": "byName", "options": name},
             "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}]}
            for name, color in names_to_colors.items()]


def timeseries(title, targets, x, y, w, h, unit, colors, description="", thresholds=None, draw="line"):
    custom = {"lineWidth": 2, "fillOpacity": 0, "showPoints": "never", "spanNulls": True,
              "axisSoftMin": 0, "drawStyle": draw, "gradientMode": "none"}
    extra = {}
    if draw == "bars":
        # one bar per 15-min bucket: evaluating increase(...[15m]) every 15 s would stack
        # overlapping windows into towers that overcount runs
        custom.update({"fillOpacity": 80, "barAlignment": -1, "lineWidth": 1, "stacking": {"mode": "normal"},
                       "spanNulls": False})
        extra = {"interval": "15m"}
    defaults = {"unit": unit, "custom": custom, "color": {"mode": "fixed", "fixedColor": SERIES[0]}}
    if thresholds:
        defaults["thresholds"] = {"mode": "absolute", "steps": [
            {"color": "transparent", "value": None}, {"color": CRITICAL, "value": thresholds}]}
        custom["thresholdsStyle"] = {"mode": "dashed"}
    panels.append({
        "type": "timeseries", "title": title, "id": _id(), "datasource": DS, "description": description,
        "gridPos": {"h": h, "w": w, "x": x, "y": y}, "targets": targets,
        "fieldConfig": {"defaults": defaults, "overrides": color_overrides(colors)},
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
        **extra,
    })


def stat(title, expr, x, y, unit, steps, description="", decimals=None, w=3, mapping=None):
    defaults = {"unit": unit, "thresholds": {"mode": "absolute", "steps": steps},
                "color": {"mode": "thresholds"}}
    if decimals is not None:
        defaults["decimals"] = decimals
    if mapping:
        defaults["mappings"] = mapping
    panels.append({
        "type": "stat", "title": title, "id": _id(), "datasource": DS, "description": description,
        "gridPos": {"h": 4, "w": w, "x": x, "y": y}, "targets": [target(expr, instant=True)],
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"colorMode": "value", "graphMode": "none", "textMode": "value", "justifyMode": "center",
                    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
    })


def ok_warn_bad(warn, bad):
    return [{"color": GOOD, "value": None}, {"color": WARNING, "value": warn}, {"color": CRITICAL, "value": bad}]


# ---------------------------------------------------------------- headline tiles
y = 0
stat("Events / sec", 'sum(rate(gtfs_events_produced_total{topic=~".*\\\\.raw"}[2m]))', 0, y, "suffix: ev/s",
     [{"color": CRITICAL, "value": None}, {"color": GOOD, "value": 1}], "Records the poller got acked by Kafka")
stat("Bronze lag", 'max(pipeline_freshness_seconds{table="bronze.vehicle_positions"})', 3, y, "s",
     ok_warn_bad(60, 120), "Now minus newest ingest_time in bronze. Alert > 2 min for 5 min.")
stat("Silver latency p50", 'max(pipeline_e2e_latency_seconds{stage="silver",quantile="0.5"})', 6, y, "s",
     ok_warn_bad(1200, 1500), "ingest_time -> silver. ~16 min BY DESIGN (15-min watermark).")
stat("Kafka consumer lag", "sum(kafka_consumer_lag_messages)", 9, y, "short", ok_warn_bad(5000, 20000),
     "Messages in Kafka not yet committed by the bronze Spark queries")
stat("DLQ depth", 'sum(kafka_topic_high_watermark{topic=~".*\\\\.dlq"})', 12, y, "short",
     [{"color": GOOD, "value": None}, {"color": WARNING, "value": 1}],
     "Total records ever sent to *.dlq (retained 7 days)")
stat("DAG success (24h)",
     'sum(increase(airflow_dagrun_duration_seconds_count{dag_id="transit_lakehouse_pipeline",state="success"}[24h]))'
     ' / sum(increase(airflow_dagrun_duration_seconds_count{dag_id="transit_lakehouse_pipeline"}[24h]))',
     15, y, "percentunit", [{"color": CRITICAL, "value": None}, {"color": WARNING, "value": 0.9},
                            {"color": GOOD, "value": 0.99}], decimals=0)
stat("Quality gate", "quality_gate_success", 18, y, "none",
     [{"color": CRITICAL, "value": None}, {"color": GOOD, "value": 1}], "Latest Great Expectations checkpoint",
     mapping=[{"type": "value", "options": {"0": {"text": "FAILED"}, "1": {"text": "PASSED"}}}])
stat("Alerts firing", 'count(ALERTS{alertstate="firing"}) or vector(0)', 21, y, "none",
     [{"color": GOOD, "value": None}, {"color": CRITICAL, "value": 1}])

# ---------------------------------------------------------------- ingestion
y = 4
row("Ingestion: is data coming in?", y)
timeseries("Events / sec by topic", [target('sum by (topic) (rate(gtfs_events_produced_total{topic=~".*\\\\.raw"}[2m]))', "{{topic}}")],
           0, y + 1, 12, 8, "suffix: ev/s", {"vehicle_positions.raw": SERIES[0], "trip_updates.raw": SERIES[1]},
           "Drops to 0 when the poller or the feed stops")
timeseries("Poll latency p95", [target("histogram_quantile(0.95, sum by (le, feed) (rate(gtfs_poll_latency_seconds_bucket[5m])))", "{{feed}}")],
           12, y + 1, 6, 8, "s", {"vehicle_positions": SERIES[0], "trip_updates": SERIES[1]},
           "Fetch + decode + produce per poll")
timeseries("DLQ records / min by stage", [target("sum by (stage) (rate(gtfs_dlq_records_total[5m])) * 60", "{{stage}}")],
           18, y + 1, 6, 8, "short", {"fetch": SERIES[0], "decode": SERIES[1], "validate": SERIES[2]},
           "Fetch / decode / validation failures routed to *.dlq")

# ---------------------------------------------------------------- streaming
y = 13
row("Streaming: is Spark keeping up?", y)
timeseries("Kafka consumer lag (messages)", [target("sum by (query) (kafka_consumer_lag_messages)", "{{query}}")],
           0, y + 1, 8, 8, "short", {"bronze_vehicle_positions": SERIES[0], "bronze_trip_updates": SERIES[1]},
           "Climbs when the bronze job is slow or down while the poller keeps producing", thresholds=20000)
timeseries("Spark processing rate (rows/s)", [target("sum by (query) (spark_streaming_processingRate_total)", "{{query}}")],
           8, y + 1, 8, 8, "short", {"bronze_vehicle_positions": SERIES[0], "bronze_trip_updates": SERIES[1],
                                     "silver_stop_delays": SERIES[2]})
timeseries("Micro-batch duration", [target("max by (query) (spark_streaming_latency) / 1000", "{{query}}")],
           16, y + 1, 8, 8, "s", {"bronze_vehicle_positions": SERIES[0], "bronze_trip_updates": SERIES[1],
                                  "silver_stop_delays": SERIES[2]},
           "Above the trigger interval (bronze 10 s, silver 30 s) means falling behind")

# ---------------------------------------------------------------- latency
y = 22
row("Latency: how old is the data?", y)
timeseries("Bronze: ingest -> landed", [target('pipeline_e2e_latency_seconds{stage="bronze",quantile="0.5"}', "p50"),
                                        target('pipeline_e2e_latency_seconds{stage="bronze",quantile="0.99"}', "p99", "B")],
           0, y + 1, 8, 8, "s", {"p50": SERIES[0], "p99": SERIES[1]}, "Poller fetch -> row in bronze Iceberg")
timeseries("Silver: ingest -> landed", [target('pipeline_e2e_latency_seconds{stage="silver",quantile="0.5"}', "p50"),
                                        target('pipeline_e2e_latency_seconds{stage="silver",quantile="0.99"}', "p99", "B")],
           8, y + 1, 8, 8, "s", {"p50": SERIES[0], "p99": SERIES[1]},
           "~16 min by design: stops are held until the 15-min watermark passes", thresholds=1500)
timeseries("Freshness: now - newest row", [target("pipeline_freshness_seconds", "{{table}}")],
           16, y + 1, 8, 8, "s", {"bronze.vehicle_positions": SERIES[0], "silver.stop_delays": SERIES[2]},
           "Climbs steadily when the source stops (e.g. poller paused)", thresholds=120)

# ---------------------------------------------------------------- silver state
y = 31
row("Silver state: bounded?", y)
timeseries("State store rows", [target('sum by (query) (spark_streaming_states_rowsTotal{app="silver_job"})', "{{query}}")],
           0, y + 1, 8, 8, "short", {"silver_stop_delays": SERIES[2]}, "Dedup keys + per-trip trackers; flat = bounded")
timeseries("Watermark delay", [target('time() - spark_query_watermark_seconds{query="silver_stop_delays"}', "silver_stop_delays")],
           8, y + 1, 8, 8, "s", {"silver_stop_delays": SERIES[2]}, "Now minus the event-time watermark (~15 min)")
timeseries("Driver JVM heap", [target("max by (app) (spark_driver_jvm_heap_used_bytes)", "{{app}}")],
           16, y + 1, 8, 8, "bytes", {"bronze_job": SERIES[0], "silver_job": SERIES[2], "spark_thrift": SERIES[3]})

# ---------------------------------------------------------------- orchestration
y = 40
row("Orchestration & quality", y)
dag_runs = [target('sum(increase(airflow_dagrun_duration_seconds_count{dag_id="transit_lakehouse_pipeline",state="success"}[$__interval]))', "success"),
            target('sum(increase(airflow_dagrun_duration_seconds_count{dag_id="transit_lakehouse_pipeline",state="failed"}[$__interval]))', "failed", "B")]
timeseries("Pipeline DAG runs (per 15 min)", dag_runs, 0, y + 1, 8, 8, "short",
           {"success": GOOD, "failed": CRITICAL}, draw="bars")
timeseries("Failed tasks (per 15 min)",
           [target('sum by (task_id) (increase(airflow_task_finish_total{dag_id="transit_lakehouse_pipeline",state="failed"}[$__interval]))', "{{task_id}}")],
           8, y + 1, 8, 8, "short", {"dbt_run": SERIES[0], "dbt_test": SERIES[1], "ge_checkpoint": SERIES[2],
                                     "mark_refresh": SERIES[3]}, draw="bars")
timeseries("DAG run duration", [target('max by (state) (airflow_dagrun_duration_seconds{dag_id="transit_lakehouse_pipeline",quantile="0.5"})', "{{state}}")],
           16, y + 1, 8, 8, "s", {"success": GOOD, "failed": CRITICAL})

panels.append({
    "type": "table", "title": "Firing alerts", "id": _id(), "datasource": DS,
    "gridPos": {"h": 6, "w": 24, "x": 0, "y": y + 9},
    "targets": [target('ALERTS{alertstate="firing"}', instant=True)],
    "options": {"showHeader": True},
    "transformations": [{"id": "labelsToFields", "options": {}},
                        {"id": "organize", "options": {"excludeByName": {"Time": True, "Value": True, "__name__": True}}}],
    "fieldConfig": {"defaults": {"noValue": "No alerts firing"}, "overrides": []},
})

dashboard = {
    "uid": "transit-lakehouse",
    "title": "Transit lakehouse pipeline",
    "description": "Throughput, lag, latency, DLQ and DAG health for the MBTA streaming lakehouse",
    "tags": ["transit", "streaming"],
    "timezone": "browser",
    "refresh": "15s",
    "time": {"from": "now-3h", "to": "now"},
    "schemaVersion": 39,
    "editable": True,
    "panels": panels,
}
out = Path(__file__).parent / "dashboards" / "pipeline.json"
out.write_text(json.dumps(dashboard, indent=2) + "\n")
print(f"wrote {out} ({len(panels)} panels)")
