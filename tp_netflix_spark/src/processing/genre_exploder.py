from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def explode_genres(df: DataFrame) -> DataFrame:
    """Split comma-separated genres into individual rows."""
    return (
        df.withColumn("genre_name", F.explode(F.split("genre", ",")))
        .withColumn("genre_name", F.trim("genre_name"))
        .where(F.col("genre_name") != "")
    )
