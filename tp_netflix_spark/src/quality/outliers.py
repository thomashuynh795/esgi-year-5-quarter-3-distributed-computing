from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import DoubleType

from config.settings import (
    IQR_MULTIPLIER,
    OUTLIER_EXAMPLE_LIMIT,
    OUTLIER_METRICS,
    OutlierThresholdRow,
    SparkDataFramePair,
)


def build_outlier_reports(spark: SparkSession, df: DataFrame) -> SparkDataFramePair:
    """Build outlier threshold report and sample outliers using IQR rule."""
    threshold_rows: list[OutlierThresholdRow] = []
    outlier_dfs: list[DataFrame] = []

    for metric, reason in OUTLIER_METRICS.items():
        quantiles: list[float] = df.approxQuantile(metric, [0.25, 0.75], 0.01)
        q1: float = float(quantiles[0])
        q3: float = float(quantiles[1])
        iqr: float = q3 - q1
        lower_bound: float = q1 - (IQR_MULTIPLIER * iqr)
        upper_bound: float = q3 + (IQR_MULTIPLIER * iqr)

        outlier_condition: Column = (F.col(metric) < lower_bound) | (
            F.col(metric) > upper_bound
        )
        outlier_count: int = int(df.where(outlier_condition).count())

        threshold_row: OutlierThresholdRow = (
            metric,
            round(q1, 2),
            round(q3, 2),
            round(iqr, 2),
            round(lower_bound, 2),
            round(upper_bound, 2),
            outlier_count,
            "IQR rule: value < Q1 - 1.5*IQR or value > Q3 + 1.5*IQR.",
            reason,
        )
        threshold_rows.append(threshold_row)

        metric_outliers: DataFrame = (
            df.where(outlier_condition)
            .withColumn("metric", F.lit(metric))
            .withColumn("metric_value", F.col(metric).cast(DoubleType()))
            .withColumn("lower_bound", F.lit(round(lower_bound, 2)))
            .withColumn("upper_bound", F.lit(round(upper_bound, 2)))
            .withColumn(
                "outlier_side",
                F.when(F.col(metric) < lower_bound, "low").otherwise("high"),
            )
            .withColumn(
                "distance_from_bound",
                F.when(
                    F.col(metric) < lower_bound,
                    lower_bound - F.col(metric),
                ).otherwise(F.col(metric) - upper_bound),
            )
            .withColumn("why_outlier", F.lit(reason))
            .select(
                "metric",
                "title",
                "release_year",
                "original_language",
                "genre",
                "metric_value",
                "lower_bound",
                "upper_bound",
                "outlier_side",
                "distance_from_bound",
                "why_outlier",
            )
            .orderBy(F.desc("distance_from_bound"))
            .limit(OUTLIER_EXAMPLE_LIMIT)
        )
        outlier_dfs.append(metric_outliers)

    outlier_thresholds: DataFrame = spark.createDataFrame(
        threshold_rows,
        [
            "metric",
            "q1",
            "q3",
            "iqr",
            "lower_bound",
            "upper_bound",
            "outlier_count",
            "rule",
            "why_check_this_metric",
        ],
    )

    sample_outliers: DataFrame = outlier_dfs[0]
    for metric_outliers in outlier_dfs[1:]:
        sample_outliers = sample_outliers.unionByName(metric_outliers)

    return outlier_thresholds, sample_outliers
