"""Ad-hoc SQL against the lakehouse catalog.

    docker compose exec spark python sql.py "SELECT count(*) FROM bronze.vehicle_positions"
"""
import sys

from spark_session import build_spark

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    spark = build_spark("adhoc_sql", {"spark.ui.enabled": "false", "spark.driver.memory": "1g"})
    for statement in sys.argv[1:]:
        spark.sql(statement).show(50, truncate=False)
    spark.stop()
