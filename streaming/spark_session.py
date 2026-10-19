"""The one SparkSession factory. Every Spark job imports this — don't configure Spark anywhere else.

Lakehouse layout:
    catalog   Iceberg JDBC catalog named `lakehouse`, metadata pointers in Postgres (iceberg-catalog-db).
              JDBC (not the Hadoop catalog) because S3/MinIO has no atomic rename, so concurrent
              writers (bronze, silver, dbt) need a real commit lock.
    data      s3a://$LAKEHOUSE_BUCKET/warehouse/<namespace>.db/<table>/   (MinIO)
    checkpoints  s3a://$LAKEHOUSE_BUCKET/checkpoints/<query>/

`lakehouse` is the default catalog, so jobs and dbt can write `bronze.vehicle_positions`.
Prometheus endpoints are enabled here for phase 5: /metrics/prometheus and /metrics/executors/prometheus
on the Spark UI port.
"""
import os

from pyspark.sql import SparkSession

SPARK_VERSION = "3.5.3"
ICEBERG_VERSION = "1.5.2"
HADOOP_AWS_VERSION = "3.3.4"  # must match the Hadoop version bundled with pyspark 3.5.x
PACKAGES = [
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
    f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
    "org.postgresql:postgresql:42.7.4",
]
CATALOG = "lakehouse"
IVY_DIR = os.environ.get("SPARK_IVY_DIR", "/opt/ivy")


def bucket() -> str:
    return os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")


def checkpoint_path(query_name: str) -> str:
    return f"s3a://{bucket()}/checkpoints/{query_name}"


def _base_conf() -> dict[str, str]:
    env = os.environ
    return {
        "spark.jars.packages": ",".join(PACKAGES),
        "spark.jars.ivy": IVY_DIR,
        "spark.driver.memory": env.get("SPARK_DRIVER_MEMORY", "2g"),
        "spark.sql.session.timeZone": "UTC",
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.shuffle.partitions": env.get("SPARK_SHUFFLE_PARTITIONS", "6"),
        # --- Iceberg ---
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
        f"spark.sql.catalog.{CATALOG}.uri": env.get("CATALOG_JDBC_URL", "jdbc:postgresql://iceberg-catalog-db:5432/iceberg"),
        f"spark.sql.catalog.{CATALOG}.jdbc.user": env.get("CATALOG_DB_USER", "iceberg"),
        f"spark.sql.catalog.{CATALOG}.jdbc.password": env.get("CATALOG_DB_PASSWORD", "iceberg"),
        f"spark.sql.catalog.{CATALOG}.jdbc.schema-version": "V1",
        f"spark.sql.catalog.{CATALOG}.warehouse": f"s3a://{bucket()}/warehouse",
        "spark.sql.defaultCatalog": CATALOG,
        # --- MinIO via s3a (Iceberg data files and streaming checkpoints) ---
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": env.get("S3_ENDPOINT", "http://minio:9000"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.access.key": env.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "spark.hadoop.fs.s3a.secret.key": env.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        # --- metrics for Prometheus (phase 5) ---
        "spark.ui.prometheus.enabled": "true",
        "spark.sql.streaming.metricsEnabled": "true",
        "spark.metrics.conf.*.sink.prometheusServlet.class": "org.apache.spark.metrics.sink.PrometheusServlet",
        "spark.metrics.conf.*.sink.prometheusServlet.path": "/metrics/prometheus",
        "spark.metrics.namespace": "${spark.app.name}",
    }


def build_spark(app_name: str, extra_conf: dict[str, str] | None = None) -> SparkSession:
    """Build (or get) the SparkSession. `extra_conf` lets a job add settings without editing this file."""
    builder = SparkSession.builder.appName(app_name).master(os.environ.get("SPARK_MASTER", "local[*]"))
    for key, value in {**_base_conf(), **(extra_conf or {})}.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


def warm_package_cache() -> None:
    """Resolve PACKAGES into the Ivy cache (run at image build so containers start offline-fast)."""
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.jars.packages", ",".join(PACKAGES))
        .config("spark.jars.ivy", IVY_DIR)
        .getOrCreate()
    )
    spark.stop()


if __name__ == "__main__":
    warm_package_cache()
