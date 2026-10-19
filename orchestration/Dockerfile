# Airflow + an isolated venv with dbt and Great Expectations (their pins clash with Airflow's).
# Build context is the repo root: docker compose build airflow
FROM apache/airflow:2.9.3-python3.11

USER root
RUN python -m venv /opt/dbt-venv && chown -R airflow: /opt/dbt-venv
USER airflow

COPY dbt/requirements.txt /tmp/requirements-dbt.txt
COPY quality/requirements.txt /tmp/requirements-quality.txt
RUN /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt -r /tmp/requirements-quality.txt

# Owned by airflow so the named volume mounted here inherits writable ownership
RUN mkdir -p /opt/airflow/quality-results

COPY --chown=airflow:root orchestration/start_airflow.sh /opt/airflow/start_airflow.sh
CMD ["bash", "/opt/airflow/start_airflow.sh"]
