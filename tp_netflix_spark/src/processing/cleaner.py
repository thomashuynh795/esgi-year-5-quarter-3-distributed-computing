from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.settings import REQUIRED_COLUMNS


def clean_dataset(typed_df: DataFrame) -> DataFrame:
    """Remove rows with null values in required columns and add temporal columns."""
    required_column_names: list[str] = list(REQUIRED_COLUMNS.keys())
    return (
        typed_df.dropna(subset=required_column_names)
        .withColumn("release_year", F.year("release_date"))
        .withColumn(
            "decade", (F.floor(F.col("release_year") / 10) * 10).cast("int")
        )
    )
