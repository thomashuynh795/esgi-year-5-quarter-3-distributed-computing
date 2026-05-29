from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def recommend_similar(
    title: str, light_df: DataFrame, top_n: int = 10
) -> DataFrame | None:
    """
    Return the top_n most similar movies to the given title within the same cluster.

    Movies are ranked by vote_average descending, then popularity descending.
    Returns None if the title is not found in the dataset.
    """
    row = light_df.filter(F.lower(F.col("title")) == title.lower()).first()
    if row is None:
        print(f"  '{title}' not found in the dataset.")
        return None
    cluster_id: int = int(row["cluster"])
    return (
        light_df.filter(
            (F.col("cluster") == cluster_id) & (F.lower(F.col("title")) != title.lower())
        )
        .orderBy(F.col("vote_average").desc(), F.col("popularity").desc())
        .select("title", "release_year", "genre", "vote_average", "popularity", "cluster")
        .limit(top_n)
    )
