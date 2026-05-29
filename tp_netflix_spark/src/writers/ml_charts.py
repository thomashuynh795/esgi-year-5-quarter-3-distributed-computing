from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.types import Row

from config.settings import PCAProjection, SilhouetteScores
from writers.charts import (
    configure_plot_style,
    save_annotated_vertical_bar_chart,
    save_line_chart,
    save_scatter_chart,
)


def save_ml_charts(
    local_output: Path,
    silhouette_scores: SilhouetteScores,
    cluster_profiles: DataFrame,
    pca_projection: PCAProjection,
) -> None:
    """Generate and save all ML clustering charts to the output directory."""
    configure_plot_style()

    profile_rows: list[Row] = cluster_profiles.orderBy("cluster").collect()

    # Cluster size chart with top-genre annotations
    cluster_labels: list[str] = [str(row["cluster"]) for row in profile_rows]
    cluster_counts: list[float] = [float(row["movie_count"]) for row in profile_rows]
    cluster_annotations: list[str] = [str(row["top_genres"]) for row in profile_rows]
    save_annotated_vertical_bar_chart(
        cluster_labels,
        cluster_counts,
        cluster_annotations,
        "Number of Movies per Cluster (top 3 genres)",
        "Cluster",
        "Number of movies",
        local_output / "cluster_sizes.png",
        "#457b9d",
    )

    # PCA scatter chart
    x_values: list[float] = pca_projection[0]
    y_values: list[float] = pca_projection[1]
    ids: list[int] = pca_projection[2]
    save_scatter_chart(
        x_values,
        y_values,
        ids,
        "KMeans Clusters — PCA 2D Projection",
        "Principal component 1",
        "Principal component 2",
        local_output / "clusters_pca.png",
    )

    # Silhouette score chart (only when k search was performed)
    if silhouette_scores:
        k_values: list[int] = sorted(silhouette_scores.keys())
        score_values: list[float] = [silhouette_scores[k] for k in k_values]
        save_line_chart(
            k_values,
            score_values,
            "Silhouette Score by k (KMeans)",
            "k",
            "Silhouette score",
            local_output / "silhouette_scores.png",
            "#2a6f97",
        )
