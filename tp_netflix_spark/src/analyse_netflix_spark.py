from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mini-projet Spark: analyse du dataset Netflix avec PySpark."
    )
    parser.add_argument(
        "--input",
        default="netflix_dataset.csv",
        help="Chemin local du CSV d'entree.",
    )
    parser.add_argument(
        "--local-output",
        default="tp_netflix_spark/results",
        help="Dossier local pour les CSV de synthese et les graphiques.",
    )
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("NetflixSparkProject")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .getOrCreate()
    )


def load_and_clean_dataset(spark: SparkSession, input_path: str) -> DataFrame:
    raw_df = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(input_path)
    )

    return (
        raw_df.select(
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
        .dropna(
            subset=[
                "release_date",
                "title",
                "popularity",
                "vote_count",
                "vote_average",
                "original_language",
                "genre",
            ]
        )
        .withColumn("release_year", F.year("release_date"))
        .withColumn("decade", (F.floor(F.col("release_year") / 10) * 10).cast("int"))
    )


def explode_genres(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("genre_name", F.explode(F.split("genre", ",")))
        .withColumn("genre_name", F.trim("genre_name"))
        .where(F.col("genre_name") != "")
    )


def save_result(df: DataFrame, local_path: Path, limit: int | None = None) -> None:
    output_df = df.limit(limit) if limit else df
    pdf = output_df.toPandas()
    pdf.to_csv(local_path, index=False)


def save_charts(local_output: Path) -> None:
    sns.set_theme(style="whitegrid")

    genre_df = pd.read_csv(local_output / "films_by_genre.csv").head(10)
    plt.figure(figsize=(11, 6))
    sns.barplot(genre_df, x="movie_count", y="genre_name", hue="genre_name", palette="viridis", legend=False)
    plt.title("Top 10 des genres les plus presents")
    plt.xlabel("Nombre de films")
    plt.ylabel("Genre")
    plt.tight_layout()
    plt.savefig(local_output / "top_genres.png", dpi=180)
    plt.close()

    language_df = pd.read_csv(local_output / "films_by_language.csv").head(10)
    plt.figure(figsize=(10, 5))
    sns.barplot(
        language_df,
        x="original_language",
        y="movie_count",
        hue="original_language",
        palette="mako",
        legend=False,
    )
    plt.title("Top 10 des langues originales")
    plt.xlabel("Langue originale")
    plt.ylabel("Nombre de films")
    plt.tight_layout()
    plt.savefig(local_output / "top_languages.png", dpi=180)
    plt.close()

    year_df = pd.read_csv(local_output / "films_by_year.csv").sort_values("release_year")
    year_df = year_df[year_df["release_year"] >= 1980]
    plt.figure(figsize=(12, 5))
    sns.lineplot(year_df, x="release_year", y="movie_count", marker="o", color="#2a6f97")
    plt.title("Evolution du nombre de films depuis 1980")
    plt.xlabel("Annee de sortie")
    plt.ylabel("Nombre de films")
    plt.tight_layout()
    plt.savefig(local_output / "films_by_year.png", dpi=180)
    plt.close()

    vote_df = pd.read_csv(local_output / "avg_vote_by_genre.csv").head(10)
    plt.figure(figsize=(11, 6))
    sns.barplot(vote_df, x="avg_vote", y="genre_name", hue="genre_name", palette="crest", legend=False)
    plt.title("Genres les mieux notes, avec au moins 50 films")
    plt.xlabel("Note moyenne")
    plt.ylabel("Genre")
    plt.xlim(5.5, 7.5)
    plt.tight_layout()
    plt.savefig(local_output / "avg_vote_by_genre.png", dpi=180)
    plt.close()


def main() -> None:
    args = parse_args()
    local_output = Path(args.local_output)
    local_output.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = load_and_clean_dataset(spark, args.input)
    genres_df = explode_genres(df)

    df.createOrReplaceTempView("movies")
    genres_df.createOrReplaceTempView("movie_genres")

    total_raw_columns = len(df.columns)
    total_movies = df.count()
    total_genre_rows = genres_df.count()
    summary = spark.createDataFrame(
        [
            (
                total_movies,
                total_genre_rows,
                total_raw_columns,
                df.select("original_language").distinct().count(),
                genres_df.select("genre_name").distinct().count(),
                df.agg(F.min("release_year")).first()[0],
                df.agg(F.max("release_year")).first()[0],
                round(df.agg(F.avg("vote_average")).first()[0], 2),
                round(df.agg(F.avg("popularity")).first()[0], 2),
            )
        ],
        [
            "movies_after_cleaning",
            "movie_genre_rows",
            "typed_columns",
            "distinct_languages",
            "distinct_genres",
            "min_release_year",
            "max_release_year",
            "avg_vote",
            "avg_popularity",
        ],
    )

    films_by_genre = spark.sql(
        """
        SELECT genre_name, COUNT(*) AS movie_count
        FROM movie_genres
        GROUP BY genre_name
        ORDER BY movie_count DESC, genre_name
        """
    )

    avg_vote_by_genre = spark.sql(
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

    films_by_language = spark.sql(
        """
        SELECT original_language, COUNT(*) AS movie_count
        FROM movies
        GROUP BY original_language
        ORDER BY movie_count DESC, original_language
        """
    )

    films_by_year = spark.sql(
        """
        SELECT release_year, COUNT(*) AS movie_count
        FROM movies
        GROUP BY release_year
        ORDER BY release_year DESC
        """
    )

    films_by_decade = spark.sql(
        """
        SELECT decade, COUNT(*) AS movie_count, ROUND(AVG(vote_average), 2) AS avg_vote
        FROM movies
        GROUP BY decade
        ORDER BY decade DESC
        """
    )

    top_popular_movies = spark.sql(
        """
        SELECT title, release_year, original_language, genre, popularity, vote_average, vote_count
        FROM movies
        ORDER BY popularity DESC
        LIMIT 20
        """
    )

    high_quality_popular_movies = spark.sql(
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

    save_charts(local_output)

    print("\nSchema nettoye:")
    df.printSchema()
    print("\nSynthese:")
    summary.show(truncate=False)
    print("\nTop 10 genres:")
    films_by_genre.show(10, truncate=False)
    print("\nTop 10 langues:")
    films_by_language.show(10, truncate=False)
    print(f"Resultats locaux: {local_output.resolve()}")

    spark.stop()


if __name__ == "__main__":
    main()
