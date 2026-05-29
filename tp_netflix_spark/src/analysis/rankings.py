from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def get_top_popular_movies(spark: SparkSession) -> DataFrame:
    """Get top 20 movies by popularity."""
    return spark.sql(
        """
        SELECT title, release_year, original_language, genre, popularity, vote_average, vote_count
        FROM movies
        ORDER BY popularity DESC
        LIMIT 20
        """
    )


def get_high_quality_popular_movies(spark: SparkSession) -> DataFrame:
    """Get top 20 highly-rated movies with at least 1000 votes."""
    return spark.sql(
        """
        SELECT title, release_year, original_language, genre, popularity, vote_average, vote_count
        FROM movies
        WHERE vote_count >= 1000
        ORDER BY vote_average DESC, popularity DESC
        LIMIT 20
        """
    )
