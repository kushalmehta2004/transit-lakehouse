"""Tiny helper: run SQL on the Spark Thrift server (used by the Airflow task scripts).

Not named thrift.py: that would shadow the `thrift` package PyHive and dbt-spark import.
"""
import os

from pyhive import hive


def run(statement: str, fetch: bool = False):
    conn = hive.connect(host=os.environ.get("SPARK_THRIFT_HOST", "spark-thrift"),
                        port=int(os.environ.get("SPARK_THRIFT_PORT", "10000")), username="airflow")
    try:
        cur = conn.cursor()
        cur.execute(statement)
        if fetch:
            cols = [d[0].split(".")[-1] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        return None
    finally:
        conn.close()
