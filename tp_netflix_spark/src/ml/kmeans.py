from __future__ import annotations

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import DataFrame

from config.settings import (
    KMEANS_MAX_ITER_FINAL,
    KMEANS_MAX_ITER_SEARCH,
    KMEANS_SEED,
    SilhouetteScores,
)


def find_best_k(
    feature_df: DataFrame, k_min: int, k_max: int
) -> tuple[int, SilhouetteScores]:
    """Evaluate KMeans for each k in [k_min, k_max] and return the best k by silhouette score."""
    evaluator: ClusteringEvaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette",
    )
    scores: SilhouetteScores = {}
    for k in range(k_min, k_max + 1):
        km: KMeans = KMeans(
            k=k,
            seed=KMEANS_SEED,
            featuresCol="features",
            predictionCol="cluster",
            maxIter=KMEANS_MAX_ITER_SEARCH,
        )
        model: KMeansModel = km.fit(feature_df)
        score: float = round(evaluator.evaluate(model.transform(feature_df)), 4)
        scores[k] = score
        print(f"  k={k:2d}  silhouette={score:.4f}")
    best_k: int = max(scores, key=scores.get)  # type: ignore[arg-type]
    return best_k, scores


def train_kmeans_model(feature_df: DataFrame, k: int) -> DataFrame:
    """Train a final KMeans model with the given k and return the clustered DataFrame."""
    km: KMeans = KMeans(
        k=k,
        seed=KMEANS_SEED,
        featuresCol="features",
        predictionCol="cluster",
        maxIter=KMEANS_MAX_ITER_FINAL,
    )
    model: KMeansModel = km.fit(feature_df)
    return model.transform(feature_df)
