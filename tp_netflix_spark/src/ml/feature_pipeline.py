from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, MinMaxScaler, PCA, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import Row

from config.settings import COUNT_VECTORIZER_MIN_DF, PCAProjection


def add_ml_features(df: DataFrame) -> DataFrame:
    """Add genre array and log-transformed columns required by the feature pipeline."""
    return (
        df.withColumn("genre_array", F.split(F.col("genre"), r",\s*"))
        .withColumn("log_popularity", F.log1p(F.col("popularity")))
        .withColumn("log_vote_count", F.log1p(F.col("vote_count")))
    )


def build_feature_pipeline() -> Pipeline:
    """
    Build the Spark ML feature pipeline for genre-driven clustering.

    Binary genre features (19 dims) and MinMax-scaled numerical features (3 dims)
    are combined into a 22-dimensional vector. Genres occupy ~86% of the feature
    space so the resulting clusters are semantically organised by film type.
    """
    cv: CountVectorizer = CountVectorizer(
        inputCol="genre_array",
        outputCol="genre_vec",
        binary=True,
        minDF=COUNT_VECTORIZER_MIN_DF,
    )
    num_assembler: VectorAssembler = VectorAssembler(
        inputCols=["log_popularity", "vote_average", "log_vote_count"],
        outputCol="num_raw",
    )
    scaler: MinMaxScaler = MinMaxScaler(inputCol="num_raw", outputCol="num_scaled")
    final_assembler: VectorAssembler = VectorAssembler(
        inputCols=["num_scaled", "genre_vec"],
        outputCol="features",
    )
    return Pipeline(stages=[cv, num_assembler, scaler, final_assembler])


def compute_pca_projections(clustered_df: DataFrame) -> PCAProjection:
    """Reduce feature vectors to 2D with PCA for cluster scatter visualisation."""
    pca_estimator: PCA = PCA(k=2, inputCol="features", outputCol="pca_out")
    projected_df: DataFrame = (
        pca_estimator.fit(clustered_df)
        .transform(clustered_df)
        .withColumn("pca_arr", vector_to_array("pca_out"))
        .select(
            F.col("cluster"),
            F.col("pca_arr")[0].alias("x"),
            F.col("pca_arr")[1].alias("y"),
        )
    )
    rows: list[Row] = projected_df.collect()
    x_values: list[float] = [float(row["x"]) for row in rows]
    y_values: list[float] = [float(row["y"]) for row in rows]
    cluster_ids: list[int] = [int(row["cluster"]) for row in rows]
    return x_values, y_values, cluster_ids
