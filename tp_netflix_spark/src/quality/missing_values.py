from __future__ import annotations

from typing import cast

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import Row

from config.settings import (
    REQUIRED_COLUMNS,
    TEXT_COLUMNS_WITH_EMPTY_STRING_CHECK,
    MissingValueReportRow,
    SparkScalar,
)
from utils.converters import require_first_row, spark_scalar_as_int


def build_missing_values_report(spark: SparkSession, typed_df: DataFrame) -> DataFrame:
    """Build a report of missing values for required columns."""
    total_rows: int = int(typed_df.count())
    aggregations: list[Column] = []
    required_column_names: list[str] = list(REQUIRED_COLUMNS.keys())

    for column_name in required_column_names:
        condition: Column = F.col(column_name).isNull()
        if column_name in TEXT_COLUMNS_WITH_EMPTY_STRING_CHECK:
            condition = condition | (F.trim(F.col(column_name)) == "")
        aggregations.append(
            F.sum(F.when(condition, 1).otherwise(0)).alias(column_name)
        )

    counts_row: Row = require_first_row(
        typed_df.agg(*aggregations), "missing value counts"
    )
    counts_raw: dict[str, object] = counts_row.asDict()
    counts: dict[str, int] = {}
    for column_name in required_column_names:
        raw_missing_count: SparkScalar = cast(SparkScalar, counts_raw[column_name])
        counts[column_name] = spark_scalar_as_int(
            raw_missing_count, f"missing count for {column_name}"
        )

    rows: list[MissingValueReportRow] = []
    for column_name in required_column_names:
        missing_count: int = counts[column_name]
        missing_percent: float = (
            round((missing_count / total_rows) * 100, 2) if total_rows > 0 else 0.0
        )
        row: MissingValueReportRow = (
            column_name,
            total_rows,
            missing_count,
            missing_percent,
            REQUIRED_COLUMNS[column_name],
        )
        rows.append(row)

    return spark.createDataFrame(
        rows,
        ["column_name", "total_rows", "missing_count", "missing_percent", "why_problem"],
    )
