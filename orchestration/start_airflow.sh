#!/usr/bin/env bash
# Single-container Airflow: scheduler + webserver + triggerer (`airflow standalone`) on LocalExecutor,
# metadata in the existing Postgres. Lighter than the usual multi-container setup for a laptop.
set -euo pipefail

python /opt/airflow/scripts/ensure_airflow_db.py
airflow db migrate
airflow users create --role Admin \
    --username "${AIRFLOW_ADMIN_USER:-admin}" --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
    --firstname Admin --lastname User --email admin@example.com >/dev/null 2>&1 || true
exec airflow standalone
