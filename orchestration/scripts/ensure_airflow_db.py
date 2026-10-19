"""Create the `airflow` metadata database in the existing Postgres (iceberg-catalog-db) if missing.

Reusing that Postgres instead of running a second one keeps the local stack lighter.
"""
import os
import time

import psycopg2

params = dict(host=os.environ.get("AIRFLOW_DB_HOST", "iceberg-catalog-db"), dbname="postgres",
              user=os.environ["AIRFLOW_DB_USER"], password=os.environ["AIRFLOW_DB_PASSWORD"])
for attempt in range(30):
    try:
        conn = psycopg2.connect(**params)
        break
    except psycopg2.OperationalError:
        time.sleep(2)
else:
    raise SystemExit("postgres not reachable")

conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'")
    if cur.fetchone() is None:
        cur.execute("CREATE DATABASE airflow")
        print("created database airflow")
conn.close()
