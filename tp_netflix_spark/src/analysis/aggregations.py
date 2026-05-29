from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def get_films_by_genre(spark: SparkSession) -> DataFrame:
    """Count movies per genre, ordered by count descending."""
    return spark.sql(
        """
        SELECT genre_name, COUNT(*) AS movie_count
        FROM movie_genres
        GROUP BY genre_name
        ORDER BY movie_count DESC, genre_name
        """
    )


def get_avg_vote_by_genre(spark: SparkSession) -> DataFrame:
    """Calculate average vote and popularity per genre (min 50 movies)."""
    return spark.sql(
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


def get_films_by_language(spark: SparkSession) -> DataFrame:
    """Count movies per original language, ordered by count descending."""
    return spark.sql(
        """
        SELECT original_language, COUNT(*) AS movie_count
        FROM movies
        GROUP BY original_language
        ORDER BY movie_count DESC, original_language
        """
    )


def get_films_by_year(spark: SparkSession) -> DataFrame:
    """Count movies per release year, ordered by year descending."""
    return spark.sql(
        """
        SELECT release_year, COUNT(*) AS movie_count
        FROM movies
        GROUP BY release_year
        ORDER BY release_year DESC
        """
    )


def get_films_by_decade(spark: SparkSession) -> DataFrame:
    """Count movies and average vote per decade, ordered by decade descending."""
    return spark.sql(
        """
        SELECT decade, COUNT(*) AS movie_count, ROUND(AVG(vote_average), 2) AS avg_vote
        FROM movies
        GROUP BY decade
        ORDER BY decade DESC
        """
    )
