# dbt (gold models + tests) and Great Expectations (silver checkpoint), both talking to the
# Spark Thrift server. Build context is the repo root: docker compose build dbt
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 DBT_PROFILES_DIR=/work/dbt
WORKDIR /work/dbt

COPY dbt/requirements.txt /tmp/requirements-dbt.txt
COPY quality/requirements.txt /tmp/requirements-quality.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dbt.txt -r /tmp/requirements-quality.txt

COPY dbt/ /work/dbt/
COPY quality/ /work/quality/

CMD ["dbt", "build"]
