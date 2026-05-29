from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def load_raw_dataset(spark: SparkSession, input_path: str) -> DataFrame:
    """Load a raw CSV dataset with multiline and escape handling."""
    return (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(input_path)
    )
