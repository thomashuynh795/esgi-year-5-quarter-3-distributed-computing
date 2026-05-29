from __future__ import annotations

from pyspark.sql import SparkSession

from config.settings import SPARK_APP_NAME, SPARK_SHUFFLE_PARTITIONS


def create_spark_session() -> SparkSession:
    """Create and configure a local Spark session."""
    return (
        SparkSession.builder.appName(SPARK_APP_NAME)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .getOrCreate()
    )
