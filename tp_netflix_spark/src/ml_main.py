from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import (
    ClusteringConfig,
    DEFAULT_EXAMPLE_MOVIES,
    K_MAX_DEFAULT,
    K_MIN_DEFAULT,
    PCAProjection,
    SilhouetteScores,
)
from loader import load_raw_dataset
from ml import (
    add_ml_features,
    build_feature_pipeline,
    compute_cluster_profiles,
    compute_pca_projections,
    find_best_k,
    recommend_similar,
    train_kmeans_model,
)
from processing import clean_dataset, standardize_dataset
from spark import create_spark_session
from writers import save_ml_charts, save_result


def parse_args() -> ClusteringConfig:
    """Parse command-line arguments and return clustering configuration."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Netflix KMeans clustering and content-based movie recommendation with PySpark MLlib."
    )
    parser.add_argument(
        "--input",
        default="netflix_dataset.csv",
        help="Local path to the input CSV file.",
    )
    parser.add_argument(
        "--local-output",
        default="tp_netflix_spark/results",
        help="Local folder for CSV results and charts.",
    )
    parser.add_argument(
        "--k-min",
        type=int,
        default=K_MIN_DEFAULT,
        help="Minimum k to evaluate during silhouette search.",
    )
    parser.add_argument(
        "--k-max",
        type=int,
        default=K_MAX_DEFAULT,
        help="Maximum k to evaluate during silhouette search.",
    )
    parser.add_argument(
        "--k",
        type=int,
        default=None,
        help="Fixed k — skips the silhouette search when provided.",
    )
    parser.add_argument(
        "--examples",
        nargs="+",
        default=list(DEFAULT_EXAMPLE_MOVIES),
        help="Movie titles for which to generate recommendations.",
    )
    namespace: argparse.Namespace = parser.parse_args()
    return ClusteringConfig(
        input_path=str(namespace.input),
        local_output_path=str(namespace.local_output),
        k_min=namespace.k_min,
        k_max=namespace.k_max,
        fixed_k=namespace.k,
        example_movies=list(namespace.examples),
    )


def print_results(cluster_profiles: DataFrame, local_output: Path) -> None:
    """Print cluster profiles and output location to the console."""
    print("\nCluster profiles:")
    cluster_profiles.show(truncate=False)
    print(f"Local results: {local_output.resolve()}")


def main() -> None:
    """Main entry point for the Netflix clustering and recommendation pipeline."""
    config: ClusteringConfig = parse_args()
    local_output: Path = Path(config.local_output_path)
    local_output.mkdir(parents=True, exist_ok=True)

    spark: SparkSession = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Load, type-cast, clean and enrich the dataset
    raw_df: DataFrame = load_raw_dataset(spark, config.input_path)
    typed_df: DataFrame = standardize_dataset(raw_df)
    df: DataFrame = clean_dataset(typed_df)
    df = add_ml_features(df)

    # Build feature vectors and cache to avoid recomputation across k iterations
    print("\nFitting the feature pipeline...")
    feature_df: DataFrame = build_feature_pipeline().fit(df).transform(df).cache()
    feature_df.count()

    # Determine k
    silhouette_scores: SilhouetteScores
    best_k: int
    if config.fixed_k is not None:
        best_k = config.fixed_k
        silhouette_scores = {}
        print(f"Using fixed k={best_k}.")
    else:
        print(f"\nSearching for best k ({config.k_min} → {config.k_max})...")
        best_k, silhouette_scores = find_best_k(feature_df, config.k_min, config.k_max)
        print(f"\nBest k={best_k}  (silhouette={silhouette_scores[best_k]:.4f})")

    # Train the final model and cache
    print(f"\nTraining final KMeans with k={best_k}...")
    clustered_df: DataFrame = train_kmeans_model(feature_df, best_k).cache()
    clustered_df.count()

    # Lightweight view — no dense feature vectors
    light_df: DataFrame = clustered_df.select(
        "title",
        "release_year",
        "genre",
        "genre_array",
        "vote_average",
        "popularity",
        "vote_count",
        "original_language",
        "cluster",
    )

    # Cluster profiles
    cluster_profiles: DataFrame = compute_cluster_profiles(light_df)

    # PCA projections for scatter visualisation
    pca_projection: PCAProjection = compute_pca_projections(clustered_df)

    # Save CSV results
    save_result(light_df.drop("genre_array"), local_output / "movies_with_clusters.csv")
    save_result(cluster_profiles, local_output / "cluster_profiles.csv")

    # Build and save recommendations for each example movie
    rec_dfs: list[DataFrame] = []
    for movie in config.example_movies:
        recs: DataFrame | None = recommend_similar(movie, light_df)
        if recs is not None:
            rec_dfs.append(recs.withColumn("because_you_watched", F.lit(movie)))
    if rec_dfs:
        all_recs: DataFrame = rec_dfs[0]
        for rec_df in rec_dfs[1:]:
            all_recs = all_recs.unionByName(rec_df)
        save_result(all_recs, local_output / "recommendations_examples.csv")

    # Generate charts
    save_ml_charts(local_output, silhouette_scores, cluster_profiles, pca_projection)

    print_results(cluster_profiles, local_output)

    spark.stop()


if __name__ == "__main__":
    main()
