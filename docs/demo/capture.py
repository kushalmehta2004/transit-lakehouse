"""Capture demo screenshots of every UI in the running stack (headless Chromium on the compose network).

    docker run --rm --network transit-lakehouse_default -v "<repo>/docs/demo:/demo" \
        mcr.microsoft.com/playwright/python:v1.48.0-jammy python /demo/capture.py

Writes /demo/img/<name>.png. Each capture is independent: a UI that fails is reported and skipped.
"""
import os
import sys
import traceback
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path("/demo/img")
OUT.mkdir(parents=True, exist_ok=True)
W = 1600
GRAFANA_FROM = os.environ.get("GRAFANA_FROM", "now-1h")


def grafana(page):
    page.set_viewport_size({"width": W, "height": 2350})
    page.goto(f"http://grafana:3000/d/transit-lakehouse?orgId=1&kiosk&from={GRAFANA_FROM}&to=now",
              wait_until="networkidle")
    page.wait_for_timeout(15000)  # panels below the fold load lazily
    page.screenshot(path=OUT / "grafana-full.png")
    page.screenshot(path=OUT / "grafana-ingestion.png", clip={"x": 0, "y": 0, "width": W, "height": 640})
    page.screenshot(path=OUT / "grafana-streaming-latency.png", clip={"x": 0, "y": 600, "width": W, "height": 840})
    page.screenshot(path=OUT / "grafana-state-orchestration.png", clip={"x": 0, "y": 1400, "width": W, "height": 950})


def airflow(page):
    page.set_viewport_size({"width": W, "height": 1000})
    page.goto("http://airflow:8080/login/", wait_until="networkidle")
    page.fill("#username", "admin")
    page.fill("#password", "admin")
    page.click("input[type=submit], button[type=submit]")
    page.wait_for_load_state("networkidle")
    page.goto("http://airflow:8080/home", wait_until="networkidle")
    page.wait_for_timeout(2000)
    page.screenshot(path=OUT / "airflow-dags.png")
    page.goto("http://airflow:8080/dags/transit_lakehouse_pipeline/grid", wait_until="networkidle")
    page.wait_for_timeout(4000)
    page.screenshot(path=OUT / "airflow-pipeline-grid.png")
    page.goto("http://airflow:8080/dags/transit_lakehouse_pipeline/grid?tab=graph", wait_until="networkidle")
    page.wait_for_timeout(4000)
    page.screenshot(path=OUT / "airflow-pipeline-graph.png")
    page.goto("http://airflow:8080/dags/lakehouse_maintenance/grid", wait_until="networkidle")
    page.wait_for_timeout(4000)
    page.screenshot(path=OUT / "airflow-maintenance-grid.png")


def redpanda(page):
    page.set_viewport_size({"width": W, "height": 900})
    page.goto("http://redpanda-console:8080/topics", wait_until="networkidle")
    page.wait_for_timeout(3000)
    page.screenshot(path=OUT / "kafka-topics.png")
    page.goto("http://redpanda-console:8080/topics/vehicle_positions.raw", wait_until="networkidle")
    page.wait_for_timeout(5000)
    page.screenshot(path=OUT / "kafka-messages.png")


def minio(page):
    page.set_viewport_size({"width": W, "height": 900})
    page.goto("http://minio:9001/login", wait_until="networkidle")
    page.fill("#accessKey", "minioadmin")
    page.fill("#secretKey", "minioadmin")
    page.keyboard.press("Enter")
    page.wait_for_timeout(4000)
    page.goto("http://minio:9001/browser/lakehouse", wait_until="networkidle")
    page.wait_for_timeout(3000)
    page.screenshot(path=OUT / "minio-lakehouse.png")
    # deep links are encoded differently across console versions: click through folders instead
    for folder, shot in [("warehouse", "minio-warehouse"), ("silver", None), ("stop_delays", "minio-silver-table"),
                         ("data", "minio-silver-partitions")]:
        page.get_by_text(folder, exact=True).first.click()
        page.wait_for_timeout(2500)
        if shot:
            page.screenshot(path=OUT / f"{shot}.png")


def spark(page):
    page.set_viewport_size({"width": W, "height": 1000})
    for name, host in [("spark-bronze", "spark"), ("spark-silver", "spark-silver")]:
        page.goto(f"http://{host}:4040/StreamingQuery/", wait_until="networkidle")
        page.wait_for_timeout(2000)
        page.screenshot(path=OUT / f"{name}-queries.png")
        link = page.locator("a[href*='statistics']").first
        if link.count():
            link.click()
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(3000)
            page.set_viewport_size({"width": W, "height": 1700})
            page.screenshot(path=OUT / f"{name}-statistics.png")
            page.set_viewport_size({"width": W, "height": 1000})


def prometheus(page):
    page.set_viewport_size({"width": W, "height": 1100})
    page.goto("http://prometheus:9090/alerts?search=", wait_until="networkidle")
    page.wait_for_timeout(2000)
    page.screenshot(path=OUT / "prometheus-alerts.png")
    page.goto("http://prometheus:9090/targets", wait_until="networkidle")
    page.wait_for_timeout(2000)
    page.screenshot(path=OUT / "prometheus-targets.png")


def dbt_docs(page):
    page.set_viewport_size({"width": W, "height": 900})
    page.goto("http://dbt-docs:8085/#!/overview?g_v=1", wait_until="networkidle")
    page.wait_for_timeout(5000)
    page.screenshot(path=OUT / "dbt-lineage.png")
    page.goto("http://dbt-docs:8085/#!/model/model.transit_lakehouse.route_lateness_by_hour", wait_until="networkidle")
    page.wait_for_timeout(3000)
    page.screenshot(path=OUT / "dbt-model.png")


CAPTURES = {"grafana": grafana, "airflow": airflow, "redpanda": redpanda, "minio": minio,
            "spark": spark, "prometheus": prometheus, "dbt": dbt_docs}

if __name__ == "__main__":
    wanted = sys.argv[1:] or list(CAPTURES)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        for key in wanted:
            page = browser.new_page(device_scale_factor=1)
            try:
                CAPTURES[key](page)
                print(f"ok   {key}")
            except Exception:
                print(f"FAIL {key}\n{traceback.format_exc(limit=2)}")
            finally:
                page.close()
        browser.close()
    for f in sorted(OUT.glob("*.png")):
        print(f"{f.stat().st_size // 1024:6d} KB  {f.name}")
