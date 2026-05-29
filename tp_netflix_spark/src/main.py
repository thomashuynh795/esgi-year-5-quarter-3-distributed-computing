from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from analysis import (
    get_avg_vote_by_genre,
    get_films_by_decade,
    get_films_by_genre,
    get_films_by_language,
    get_films_by_year,
    get_high_quality_popular_movies,
    get_top_popular_movies,
)
from config.settings import (
    TOP_CHART_LIMIT,
    ProjectConfig,
    SparkDataFramePair,
    SummaryRow,
)
from loader import load_raw_dataset
from writers import save_charts, save_result
from processing import clean_dataset, explode_genres, standardize_dataset
from quality import build_missing_values_report, build_outlier_reports
from spark import create_spark_session
from utils import first_column_as_float, first_column_as_int


def parse_args() -> ProjectConfig:
    """Parse command-line arguments and return project configuration."""
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


def build_summary(
    spark: SparkSession, df: DataFrame, genres_df: DataFrame
) -> DataFrame:
    """Build a summary DataFrame with key statistics."""
    total_raw_columns: int = len(df.columns)
    total_movies: int = int(df.count())
    total_genre_rows: int = int(genres_df.count())
    distinct_languages: int = int(df.select("original_language").distinct().count())
    distinct_genres: int = int(genres_df.select("genre_name").distinct().count())

    min_release_year: int = first_column_as_int(
        df.agg(F.min("release_year")), "minimum release year"
    )
    max_release_year: int = first_column_as_int(
        df.agg(F.max("release_year")), "maximum release year"
    )
    average_vote_raw: float = first_column_as_float(
        df.agg(F.avg("vote_average")), "average vote"
    )
    average_popularity_raw: float = first_column_as_float(
        df.agg(F.avg("popularity")), "average popularity"
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
    return spark.createDataFrame(summary_rows, summary_columns)


def print_results(
    df: DataFrame,
    summary: DataFrame,
    films_by_genre: DataFrame,
    films_by_language: DataFrame,
    missing_values_report: DataFrame,
    outlier_thresholds: DataFrame,
    local_output: Path,
) -> None:
    """Print analysis results to the console."""
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


def main() -> None:
    """Main entry point for the Netflix Spark analysis."""
    config: ProjectConfig = parse_args()
    local_output: Path = Path(config.local_output_path)
    local_output.mkdir(parents=True, exist_ok=True)

    spark: SparkSession = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Data loading and processing
    raw_df: DataFrame = load_raw_dataset(spark, config.input_path)
    typed_df: DataFrame = standardize_dataset(raw_df)
    df: DataFrame = clean_dataset(typed_df)
    genres_df: DataFrame = explode_genres(df)

    # Data quality reports
    missing_values_report: DataFrame = build_missing_values_report(spark, typed_df)
    outlier_reports: SparkDataFramePair = build_outlier_reports(spark, df)
    outlier_thresholds: DataFrame = outlier_reports[0]
    sample_outliers: DataFrame = outlier_reports[1]

    # Register temp views for SQL queries
    df.createOrReplaceTempView("movies")
    genres_df.createOrReplaceTempView("movie_genres")

    # Build summary
    summary: DataFrame = build_summary(spark, df, genres_df)

    # Execute analysis queries
    films_by_genre: DataFrame = get_films_by_genre(spark)
    avg_vote_by_genre: DataFrame = get_avg_vote_by_genre(spark)
    films_by_language: DataFrame = get_films_by_language(spark)
    films_by_year: DataFrame = get_films_by_year(spark)
    films_by_decade: DataFrame = get_films_by_decade(spark)
    top_popular_movies: DataFrame = get_top_popular_movies(spark)
    high_quality_popular_movies: DataFrame = get_high_quality_popular_movies(spark)

    # Save results to CSV
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

    # Generate charts
    save_charts(
        local_output,
        films_by_genre,
        films_by_language,
        films_by_year,
        avg_vote_by_genre,
        missing_values_report,
        outlier_thresholds,
    )

    # Print results to console
    print_results(
        df,
        summary,
        films_by_genre,
        films_by_language,
        missing_values_report,
        outlier_thresholds,
        local_output,
    )

    spark.stop()


if __name__ == "__main__":
    main()
