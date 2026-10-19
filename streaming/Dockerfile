FROM python:3.11-slim-bookworm

# Spark 3.5 runs on Java 17 (bookworm's default JRE)
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    SPARK_IVY_DIR=/opt/ivy
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Resolve the pinned Maven packages at build time so containers start without downloading jars.
COPY spark_session.py .
RUN python spark_session.py

COPY . .
EXPOSE 4040
CMD ["python", "bronze_job.py"]
