from __future__ import annotations

from typing import cast

from pyspark.sql import DataFrame
from pyspark.sql.types import Row

from config.settings import SparkScalar


def require_first_row(df: DataFrame, context: str) -> Row:
    """Extract the first row from a DataFrame, raising an error if empty."""
    row: Row | None = df.first()
    if row is None:
        raise ValueError(f"No row returned while reading {context}.")
    return row


def spark_scalar_as_int(value: SparkScalar, context: str) -> int:
    """Convert a Spark scalar value to int, raising an error if null."""
    if value is None:
        raise ValueError(f"Expected an integer for {context}, received null.")
    return int(value)


def spark_scalar_as_float(value: SparkScalar, context: str) -> float:
    """Convert a Spark scalar value to float, raising an error if null."""
    if value is None:
        raise ValueError(f"Expected a float for {context}, received null.")
    return float(value)


def first_column_as_int(df: DataFrame, context: str) -> int:
    """Extract the first column of the first row as an integer."""
    row: Row = require_first_row(df, context)
    value: SparkScalar = cast(SparkScalar, row[0])
    return spark_scalar_as_int(value, context)


def first_column_as_float(df: DataFrame, context: str) -> float:
    """Extract the first column of the first row as a float."""
    row: Row = require_first_row(df, context)
    value: SparkScalar = cast(SparkScalar, row[0])
    return spark_scalar_as_float(value, context)
