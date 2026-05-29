from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Final, TypeAlias, cast

import matplotlib.pyplot as pyplot_client
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as spark_client
from pyspark.sql.column import Column
from pyspark.sql.types import Row
from pyspark.sql.types import DoubleType, IntegerType


@dataclass(frozen=True)
class ProjectConfig:
    input_path: str
    local_output_path: str


MissingValueReportRow: TypeAlias = tuple[str, int, int, float, str]
OutlierThresholdRow: TypeAlias = tuple[str, float, float, float, float, float, int, str, str]
SparkDataFramePair: TypeAlias = tuple[DataFrame, DataFrame]
SparkScalar: TypeAlias = str | int | float | None
SummaryRow: TypeAlias = tuple[int, int, int, int, int, int, int, float, float]


REQUIRED_COLUMNS: Final[dict[str, str]] = {
    "release_date": "Required to analyze movies by year and decade.",
    "title": "Required to identify each movie clearly in the results.",
    "popularity": "Required to rank the most popular movies.",
    "vote_count": "Required to assess how reliable an average rating is.",
    "vote_average": "Required to compare movie and genre ratings.",
    "original_language": "Required to analyze the distribution by original language.",
    "genre": "Required to analyze the distribution by genre.",
}

TEXT_COLUMNS_WITH_EMPTY_STRING_CHECK: Final[set[str]] = {
    "title",
    "original_language",
    "genre",
}

OUTLIER_METRICS: Final[dict[str, str]] = {
    "popularity": "A popularity value far from the rest can dominate rankings.",
    "vote_count": "A very high vote count can indicate a much more exposed movie.",
    "vote_average": "A very low or very high rating can influence genre averages.",
}

IQR_MULTIPLIER: Final[float] = 1.5
SPARK_SHUFFLE_PARTITIONS: Final[str] = "8"
MIN_RELEASE_YEAR_FOR_YEAR_CHART: Final[int] = 1980
TOP_CHART_LIMIT: Final[int] = 10
OUTLIER_EXAMPLE_LIMIT: Final[int] = 20


def parse_args() -> ProjectConfig:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Spark mini-project: Netflix dataset analysis with PySpark."
    )
    parser.add_argument(
        "--input",
        default="netflix_dataset.csv",
        help="Local path to the input CSV file.",
    )
    parser.add_argument(
        "--local-output",
        default="tp_netflix_spark/results",
        help="Local folder for summary CSV files and charts.",
    )
    namespace: argparse.Namespace = parser.parse_args()
    return ProjectConfig(
        input_path=str(namespace.input),
        local_output_path=str(namespace.local_output),
    )


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("NetflixSparkProject")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .getOrCreate()
    )


def require_first_row(df: DataFrame, context: str) -> Row:
    row: Row | None = df.first()
    if row is None:
        raise ValueError(f"No row returned while reading {context}.")
    return row


def spark_scalar_as_int(value: SparkScalar, context: str) -> int:
    if value is None:
        raise ValueError(f"Expected an integer for {context}, received null.")
    integer_value: int = int(value)
    return integer_value


def spark_scalar_as_float(value: SparkScalar, context: str) -> float:
    if value is None:
        raise ValueError(f"Expected a float for {context}, received null.")
    float_value: float = float(value)
    return float_value


def first_column_as_int(df: DataFrame, context: str) -> int:
    row: Row = require_first_row(df, context)
    value: SparkScalar = cast(SparkScalar, row[0])
    return spark_scalar_as_int(value, context)


def first_column_as_float(df: DataFrame, context: str) -> float:
    row: Row = require_first_row(df, context)
    value: SparkScalar = cast(SparkScalar, row[0])
    return spark_scalar_as_float(value, context)


def load_raw_dataset(spark: SparkSession, input_path: str) -> DataFrame:
    raw_df: DataFrame = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(input_path)
    )
    return raw_df


def standardize_dataset(raw_df: DataFrame) -> DataFrame:
    typed_df: DataFrame = (
        raw_df.select(
            spark_client.to_date("Release_Date").alias("release_date"),
            spark_client.trim(spark_client.col("Title")).alias("title"),
            spark_client.trim(spark_client.col("Overview")).alias("overview"),
            spark_client.col("Popularity").cast(DoubleType()).alias("popularity"),
            spark_client.col("Vote_Count").cast(IntegerType()).alias("vote_count"),
            spark_client.col("Vote_Average").cast(DoubleType()).alias("vote_average"),
            spark_client.trim(spark_client.col("Original_Language")).alias("original_language"),
            spark_client.trim(spark_client.col("Genre")).alias("genre"),
            spark_client.col("Poster_Url").alias("poster_url"),
        )
    )
    return typed_df


def clean_dataset(typed_df: DataFrame) -> DataFrame:
    required_column_names: list[str] = list(REQUIRED_COLUMNS.keys())
    cleaned_df: DataFrame = (
        typed_df
        .dropna(
            subset=required_column_names
        )
        .withColumn("release_year", spark_client.year("release_date"))
        .withColumn("decade", (spark_client.floor(spark_client.col("release_year") / 10) * 10).cast("int"))
    )
    return cleaned_df


def build_missing_values_report(spark: SparkSession, typed_df: DataFrame) -> DataFrame:
    total_rows: int = int(typed_df.count())
    aggregations: list[Column] = []
    required_column_names: list[str] = list(REQUIRED_COLUMNS.keys())

    for column_name in required_column_names:
        condition: Column = spark_client.col(column_name).isNull()
        if column_name in TEXT_COLUMNS_WITH_EMPTY_STRING_CHECK:
            condition = condition | (spark_client.trim(spark_client.col(column_name)) == "")
        aggregations.append(
            spark_client.sum(spark_client.when(condition, 1).otherwise(0)).alias(column_name)
        )

    counts_row: Row = require_first_row(typed_df.agg(*aggregations), "missing value counts")
    counts_raw: dict[str, object] = counts_row.asDict()
    counts: dict[str, int] = {}
    for column_name in required_column_names:
        raw_missing_count: SparkScalar = cast(SparkScalar, counts_raw[column_name])
        counts[column_name] = spark_scalar_as_int(raw_missing_count, f"missing count for {column_name}")

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

    report_df: DataFrame = spark.createDataFrame(
        rows,
        ["column_name", "total_rows", "missing_count", "missing_percent", "why_problem"],
    )
    return report_df


def build_outlier_reports(spark: SparkSession, df: DataFrame) -> SparkDataFramePair:
    threshold_rows: list[OutlierThresholdRow] = []
    outlier_dfs: list[DataFrame] = []

    for metric, reason in OUTLIER_METRICS.items():
        quantiles: list[float] = df.approxQuantile(metric, [0.25, 0.75], 0.01)
        q1: float = float(quantiles[0])
        q3: float = float(quantiles[1])
        iqr: float = q3 - q1
        lower_bound: float = q1 - (IQR_MULTIPLIER * iqr)
        upper_bound: float = q3 + (IQR_MULTIPLIER * iqr)
        outlier_condition: Column = (spark_client.col(metric) < lower_bound) | (
            spark_client.col(metric) > upper_bound
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
            .withColumn("metric", spark_client.lit(metric))
            .withColumn("metric_value", spark_client.col(metric).cast(DoubleType()))
            .withColumn("lower_bound", spark_client.lit(round(lower_bound, 2)))
            .withColumn("upper_bound", spark_client.lit(round(upper_bound, 2)))
            .withColumn(
                "outlier_side",
                spark_client.when(spark_client.col(metric) < lower_bound, "low").otherwise("high"),
            )
            .withColumn(
                "distance_from_bound",
                spark_client.when(
                    spark_client.col(metric) < lower_bound,
                    lower_bound - spark_client.col(metric),
                ).otherwise(spark_client.col(metric) - upper_bound),
            )
            .withColumn("why_outlier", spark_client.lit(reason))
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
            .orderBy(spark_client.desc("distance_from_bound"))
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


def explode_genres(df: DataFrame) -> DataFrame:
    exploded_df: DataFrame = (
        df.withColumn("genre_name", spark_client.explode(spark_client.split("genre", ",")))
        .withColumn("genre_name", spark_client.trim("genre_name"))
        .where(spark_client.col("genre_name") != "")
    )
    return exploded_df


def save_result(df: DataFrame, local_path: Path, limit: int | None = None) -> None:
    output_df: DataFrame = df.limit(limit) if limit is not None else df
    temp_dir: Path = local_path.with_name(f".{local_path.stem}_spark_tmp")

    if local_path.exists():
        local_path.unlink()
    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    output_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(temp_dir))
    csv_part: Path = next(temp_dir.glob("part-*.csv"))
    shutil.move(str(csv_part), local_path)
    shutil.rmtree(temp_dir)


def configure_plot_style() -> None:
    plot_style: dict[str, bool | float | int] = {
        "axes.grid": True,
        "grid.alpha": 0.25,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "font.size": 10,
    }
    pyplot_client.rcParams.update(
        plot_style
    )


def save_horizontal_bar_chart(
    labels: list[str],
    values: list[float],
    title: str,
    xlabel: str,
    ylabel: str,
    output_path: Path,
    color: str,
) -> None:
    maximum_value: float = max(values) if len(values) > 0 else 0.0
    pyplot_client.figure(figsize=(11, 6))
    pyplot_client.barh(labels, values, color=color)
    pyplot_client.gca().invert_yaxis()
    pyplot_client.title(title)
    pyplot_client.xlabel(xlabel)
    pyplot_client.ylabel(ylabel)
    if len(values) > 0 and maximum_value == 0:
        pyplot_client.xlim(0, 1)
    pyplot_client.tight_layout()
    pyplot_client.savefig(output_path, dpi=180)
    pyplot_client.close()


def save_vertical_bar_chart(
    labels: list[str],
    values: list[float],
    title: str,
    xlabel: str,
    ylabel: str,
    output_path: Path,
    color: str,
) -> None:
    maximum_value: float = max(values) if len(values) > 0 else 0.0
    pyplot_client.figure(figsize=(10, 5))
    pyplot_client.bar(labels, values, color=color)
    pyplot_client.title(title)
    pyplot_client.xlabel(xlabel)
    pyplot_client.ylabel(ylabel)
    if len(values) > 0 and maximum_value == 0:
        pyplot_client.ylim(0, 1)
    pyplot_client.tight_layout()
    pyplot_client.savefig(output_path, dpi=180)
    pyplot_client.close()


def save_charts(
    local_output: Path,
    films_by_genre: DataFrame,
    films_by_language: DataFrame,
    films_by_year: DataFrame,
    avg_vote_by_genre: DataFrame,
    missing_values_report: DataFrame,
    outlier_thresholds: DataFrame,
) -> None:
    configure_plot_style()

    genre_rows: list[Row] = films_by_genre.limit(TOP_CHART_LIMIT).collect()
    genre_labels: list[str] = [str(row["genre_name"]) for row in genre_rows]
    genre_values: list[float] = [float(row["movie_count"]) for row in genre_rows]
    save_horizontal_bar_chart(
        genre_labels,
        genre_values,
        "Top 10 Most Common Genres",
        "Number of movies",
        "Genre",
        local_output / "top_genres.png",
        "#2a9d8f",
    )

    language_rows: list[Row] = films_by_language.limit(TOP_CHART_LIMIT).collect()
    language_labels: list[str] = [str(row["original_language"]) for row in language_rows]
    language_values: list[float] = [float(row["movie_count"]) for row in language_rows]
    save_vertical_bar_chart(
        language_labels,
        language_values,
        "Top 10 Original Languages",
        "Original language",
        "Number of movies",
        local_output / "top_languages.png",
        "#457b9d",
    )

    year_rows: list[Row] = (
        films_by_year.where(spark_client.col("release_year") >= MIN_RELEASE_YEAR_FOR_YEAR_CHART)
        .orderBy("release_year")
        .collect()
    )
    year_labels: list[int] = [int(row["release_year"]) for row in year_rows]
    year_values: list[float] = [float(row["movie_count"]) for row in year_rows]
    pyplot_client.figure(figsize=(12, 5))
    pyplot_client.plot(
        year_labels,
        year_values,
        marker="o",
        color="#2a6f97",
    )
    pyplot_client.title("Movie Count Evolution Since 1980")
    pyplot_client.xlabel("Release year")
    pyplot_client.ylabel("Number of movies")
    pyplot_client.tight_layout()
    pyplot_client.savefig(local_output / "films_by_year.png", dpi=180)
    pyplot_client.close()

    vote_rows: list[Row] = avg_vote_by_genre.limit(TOP_CHART_LIMIT).collect()
    vote_labels: list[str] = [str(row["genre_name"]) for row in vote_rows]
    vote_values: list[float] = [float(row["avg_vote"]) for row in vote_rows]
    save_horizontal_bar_chart(
        vote_labels,
        vote_values,
        "Highest-Rated Genres With At Least 50 Movies",
        "Average rating",
        "Genre",
        local_output / "avg_vote_by_genre.png",
        "#588157",
    )

    missing_rows: list[Row] = missing_values_report.collect()
    missing_labels: list[str] = [str(row["column_name"]) for row in missing_rows]
    missing_values: list[float] = [float(row["missing_count"]) for row in missing_rows]
    save_horizontal_bar_chart(
        missing_labels,
        missing_values,
        "Missing Values by Important Column",
        "Number of missing values",
        "Column",
        local_output / "missing_values.png",
        "#b56576",
    )

    outlier_rows: list[Row] = outlier_thresholds.collect()
    outlier_labels: list[str] = [str(row["metric"]) for row in outlier_rows]
    outlier_values: list[float] = [float(row["outlier_count"]) for row in outlier_rows]
    save_vertical_bar_chart(
        outlier_labels,
        outlier_values,
        "Outliers Detected With the IQR Rule",
        "Metric",
        "Number of outliers",
        local_output / "outliers_by_metric.png",
        "#e76f51",
    )


def main() -> None:
    config: ProjectConfig = parse_args()
    local_output: Path = Path(config.local_output_path)
    local_output.mkdir(parents=True, exist_ok=True)

    spark: SparkSession = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df: DataFrame = load_raw_dataset(spark, config.input_path)
    typed_df: DataFrame = standardize_dataset(raw_df)
    df: DataFrame = clean_dataset(typed_df)
    genres_df: DataFrame = explode_genres(df)
    missing_values_report: DataFrame = build_missing_values_report(spark, typed_df)
    outlier_reports: SparkDataFramePair = build_outlier_reports(spark, df)
    outlier_thresholds: DataFrame = outlier_reports[0]
    sample_outliers: DataFrame = outlier_reports[1]

    df.createOrReplaceTempView("movies")
    genres_df.createOrReplaceTempView("movie_genres")

    total_raw_columns: int = len(df.columns)
    total_movies: int = int(df.count())
    total_genre_rows: int = int(genres_df.count())
    distinct_languages: int = int(df.select("original_language").distinct().count())
    distinct_genres: int = int(genres_df.select("genre_name").distinct().count())
    min_release_year: int = first_column_as_int(
        df.agg(spark_client.min("release_year")),
        "minimum release year",
    )
    max_release_year: int = first_column_as_int(
        df.agg(spark_client.max("release_year")),
        "maximum release year",
    )
    average_vote_raw: float = first_column_as_float(
        df.agg(spark_client.avg("vote_average")),
        "average vote",
    )
    average_popularity_raw: float = first_column_as_float(
        df.agg(spark_client.avg("popularity")),
        "average popularity",
    )
    average_vote: float = round(average_vote_raw, 2)
    average_popularity: float = round(average_popularity_raw, 2)

    summary_rows: list[SummaryRow] = [
        (
            total_movies,
            total_genre_rows,
            total_raw_columns,
            distinct_languages,
            distinct_genres,
            min_release_year,
            max_release_year,
            average_vote,
            average_popularity,
        )
    ]
    summary_columns: list[str] = [
        "movies_after_cleaning",
        "movie_genre_rows",
        "typed_columns",
        "distinct_languages",
        "distinct_genres",
        "min_release_year",
        "max_release_year",
        "avg_vote",
        "avg_popularity",
    ]
    summary: DataFrame = spark.createDataFrame(
        summary_rows,
        summary_columns,
    )

    films_by_genre: DataFrame = spark.sql(
        """
        SELECT genre_name, COUNT(*) AS movie_count
        FROM movie_genres
        GROUP BY genre_name
        ORDER BY movie_count DESC, genre_name
        """
    )

    avg_vote_by_genre: DataFrame = spark.sql(
        """
        SELECT
            genre_name,
            ROUND(AVG(vote_average), 2) AS avg_vote,
            ROUND(AVG(popularity), 2) AS avg_popularity,
            COUNT(*) AS movie_count
        FROM movie_genres
        GROUP BY genre_name
        HAVING COUNT(*) >= 50
        ORDER BY avg_vote DESC, movie_count DESC
        """
    )

    films_by_language: DataFrame = spark.sql(
        """
        SELECT original_language, COUNT(*) AS movie_count
        FROM movies
        GROUP BY original_language
        ORDER BY movie_count DESC, original_language
        """
    )

    films_by_year: DataFrame = spark.sql(
        """
        SELECT release_year, COUNT(*) AS movie_count
        FROM movies
        GROUP BY release_year
        ORDER BY release_year DESC
        """
    )

    films_by_decade: DataFrame = spark.sql(
        """
        SELECT decade, COUNT(*) AS movie_count, ROUND(AVG(vote_average), 2) AS avg_vote
        FROM movies
        GROUP BY decade
        ORDER BY decade DESC
        """
    )

    top_popular_movies: DataFrame = spark.sql(
        """
        SELECT title, release_year, original_language, genre, popularity, vote_average, vote_count
        FROM movies
        ORDER BY popularity DESC
        LIMIT 20
        """
    )

    high_quality_popular_movies: DataFrame = spark.sql(
        """
        SELECT title, release_year, original_language, genre, popularity, vote_average, vote_count
        FROM movies
        WHERE vote_count >= 1000
        ORDER BY vote_average DESC, popularity DESC
        LIMIT 20
        """
    )

    save_result(summary, local_output / "summary.csv")
    save_result(films_by_genre, local_output / "films_by_genre.csv")
    save_result(avg_vote_by_genre, local_output / "avg_vote_by_genre.csv")
    save_result(films_by_language, local_output / "films_by_language.csv")
    save_result(films_by_year, local_output / "films_by_year.csv")
    save_result(films_by_decade, local_output / "films_by_decade.csv")
    save_result(top_popular_movies, local_output / "top_popular_movies.csv")
    save_result(high_quality_popular_movies, local_output / "high_quality_popular_movies.csv")
    save_result(missing_values_report, local_output / "data_quality_missing_values.csv")
    save_result(outlier_thresholds, local_output / "data_quality_outlier_thresholds.csv")
    save_result(sample_outliers, local_output / "data_quality_outlier_examples.csv")

    save_charts(
        local_output,
        films_by_genre,
        films_by_language,
        films_by_year,
        avg_vote_by_genre,
        missing_values_report,
        outlier_thresholds,
    )

    print("\nCleaned schema:")
    df.printSchema()
    print("\nSummary:")
    summary.show(truncate=False)
    print("\nTop 10 genres:")
    films_by_genre.show(TOP_CHART_LIMIT, truncate=False)
    print("\nTop 10 languages:")
    films_by_language.show(TOP_CHART_LIMIT, truncate=False)
    print("\nData quality - missing values:")
    missing_values_report.show(truncate=False)
    print("\nData quality - outlier thresholds:")
    outlier_thresholds.show(truncate=False)
    print(f"Local results: {local_output.resolve()}")

    spark.stop()


if __name__ == "__main__":
    main()
