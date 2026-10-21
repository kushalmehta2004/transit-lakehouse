"""Spark Thrift (HiveServer2-compatible JDBC) server over the lakehouse catalog, for dbt and BI.

Started from a SparkSession built by spark_session.build_spark(), so it sees exactly the same
Iceberg catalog / MinIO config as the streaming jobs: no second place where Spark is configured.

    jdbc:hive2://spark-thrift:10000   (no auth; local only)
"""
import logging
import os
import signal
import threading

from spark_session import IVY_DIR, build_spark

log = logging.getLogger("thrift")

# JDBC sessions run on Thrift handler threads whose classloader can't see jars that
# spark.jars.packages adds at runtime, so put the catalog/storage jars on the JVM classpath
# at launch too (same pinned files the package cache already holds).
STARTUP_CLASSPATH_JARS = [
    "org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.5.2.jar",
    "org.apache.hadoop_hadoop-aws-3.3.4.jar",
    "com.amazonaws_aws-java-sdk-bundle-1.12.262.jar",
    "org.postgresql_postgresql-42.7.4.jar",
]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    spark = build_spark("spark_thrift", {
        "spark.driver.extraClassPath": ":".join(f"{IVY_DIR}/jars/{j}" for j in STARTUP_CLASSPATH_JARS),
        "spark.hadoop.hive.server2.thrift.port": os.environ.get("THRIFT_PORT", "10000"),
        # Long-lived server reading tables that streaming jobs commit to every 10-30 s: Iceberg's
        # catalog cache would keep serving an old snapshot to dbt / GE, so always load the latest.
        "spark.sql.catalog.lakehouse.cache-enabled": "false",
    })
    spark.sql("CREATE NAMESPACE IF NOT EXISTS default")  # every JDBC session starts with USE default
    jvm = spark.sparkContext._jvm
    jvm.org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.startWithContext(
        spark._jsparkSession.sqlContext()
    )
    log.info("thrift server listening on :%s", os.environ.get("THRIFT_PORT", "10000"))

    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    # If the JVM's SparkContext dies (e.g. heap OOM), exit non-zero so Docker's restart policy
    # brings the server back; otherwise this wrapper would keep a dead container "running".
    while not stop.wait(15):
        try:
            dead = spark.sparkContext._jsc.sc().isStopped()
        except Exception:
            dead = True
        if dead:
            log.error("SparkContext stopped; exiting so the container restarts")
            os._exit(1)
    spark.stop()


if __name__ == "__main__":
    main()
