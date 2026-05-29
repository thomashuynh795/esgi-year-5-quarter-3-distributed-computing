from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


def standardize_dataset(raw_df: DataFrame) -> DataFrame:
    """Cast raw columns to proper types and standardize column names."""
    return raw_df.select(
        F.to_date("Release_Date").alias("release_date"),
        F.trim(F.col("Title")).alias("title"),
        F.trim(F.col("Overview")).alias("overview"),
        F.col("Popularity").cast(DoubleType()).alias("popularity"),
        F.col("Vote_Count").cast(IntegerType()).alias("vote_count"),
        F.col("Vote_Average").cast(DoubleType()).alias("vote_average"),
        F.trim(F.col("Original_Language")).alias("original_language"),
        F.trim(F.col("Genre")).alias("genre"),
        F.col("Poster_Url").alias("poster_url"),
    )
