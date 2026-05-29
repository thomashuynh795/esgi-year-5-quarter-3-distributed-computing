from __future__ import annotations

from dataclasses import dataclass
from typing import Final, TypeAlias

from pyspark.sql import DataFrame


@dataclass(frozen=True)
class ProjectConfig:
    """Immutable configuration for the Netflix Spark analysis project."""

    input_path: str
    local_output_path: str


# Type aliases for report structures
MissingValueReportRow: TypeAlias = tuple[str, int, int, float, str]
OutlierThresholdRow: TypeAlias = tuple[str, float, float, float, float, float, int, str, str]
SparkDataFramePair: TypeAlias = tuple[DataFrame, DataFrame]
SparkScalar: TypeAlias = str | int | float | None
SummaryRow: TypeAlias = tuple[int, int, int, int, int, int, int, float, float]

# Required columns and their justification
REQUIRED_COLUMNS: Final[dict[str, str]] = {
    "release_date": "Required to analyze movies by year and decade.",
    "title": "Required to identify each movie clearly in the results.",
    "popularity": "Required to rank the most popular movies.",
    "vote_count": "Required to assess how reliable an average rating is.",
    "vote_average": "Required to compare movie and genre ratings.",
    "original_language": "Required to analyze the distribution by original language.",
    "genre": "Required to analyze the distribution by genre.",
}

# Text columns that should also be checked for empty strings
TEXT_COLUMNS_WITH_EMPTY_STRING_CHECK: Final[set[str]] = {
    "title",
    "original_language",
    "genre",
}

# Metrics to check for outliers
OUTLIER_METRICS: Final[dict[str, str]] = {
    "popularity": "A popularity value far from the rest can dominate rankings.",
    "vote_count": "A very high vote count can indicate a much more exposed movie.",
    "vote_average": "A very low or very high rating can influence genre averages.",
}

# Spark configuration
SPARK_APP_NAME: Final[str] = "NetflixSparkProject"
SPARK_SHUFFLE_PARTITIONS: Final[str] = "8"

# Statistical thresholds
IQR_MULTIPLIER: Final[float] = 1.5

# Chart and output limits
MIN_RELEASE_YEAR_FOR_YEAR_CHART: Final[int] = 1980
TOP_CHART_LIMIT: Final[int] = 10
OUTLIER_EXAMPLE_LIMIT: Final[int] = 20
