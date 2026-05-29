from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import TOP_GENRES_PER_CLUSTER


def compute_cluster_profiles(light_df: DataFrame) -> DataFrame:
    """
    Build a summary DataFrame with one row per cluster.

    Includes movie count, average and standard deviation of vote and popularity,
    average release year, and the top N genres by frequency within each cluster.
    """
    exploded: DataFrame = (
        light_df.withColumn("genre_name", F.explode("genre_array"))
        .withColumn("genre_name", F.trim("genre_name"))
        .filter(F.col("genre_name") != "")
    )

    genre_window: Window = Window.partitionBy("cluster").orderBy(F.col("genre_count").desc())
    genre_ranked: DataFrame = (
        exploded.groupBy("cluster", "genre_name")
        .count()
        .withColumnRenamed("count", "genre_count")
        .withColumn("rank", F.row_number().over(genre_window))
    )

    top_genre_dfs: list[DataFrame] = []
    for rank in range(1, TOP_GENRES_PER_CLUSTER + 1):
        alias: str = f"g{rank}"
        top_genre_dfs.append(
            genre_ranked.filter(F.col("rank") == rank)
            .select("cluster", F.col("genre_name").alias(alias))
        )

    top_genres: DataFrame = top_genre_dfs[0]
    for genre_df in top_genre_dfs[1:]:
        top_genres = top_genres.join(genre_df, "cluster", "left")

    genre_cols: list[str] = [f"g{rank}" for rank in range(1, TOP_GENRES_PER_CLUSTER + 1)]
    top_genres = top_genres.withColumn(
        "top_genres", F.concat_ws(" / ", *[F.col(c) for c in genre_cols])
    ).select("cluster", "top_genres")

    stats: DataFrame = light_df.groupBy("cluster").agg(
        F.count("*").alias("movie_count"),
        F.round(F.avg("vote_average"), 2).alias("avg_vote"),
        F.round(F.stddev("vote_average"), 2).alias("std_vote"),
        F.round(F.avg("popularity"), 2).alias("avg_popularity"),
        F.round(F.avg("release_year"), 0).cast("int").alias("avg_release_year"),
    )

    return stats.join(top_genres, "cluster").orderBy("cluster")
