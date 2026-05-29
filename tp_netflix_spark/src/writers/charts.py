from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import Row

from config.settings import MIN_RELEASE_YEAR_FOR_YEAR_CHART, TOP_CHART_LIMIT


def configure_plot_style() -> None:
    """Configure matplotlib style for all charts."""
    plot_style: dict[str, bool | float | int] = {
        "axes.grid": True,
        "grid.alpha": 0.25,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "font.size": 10,
    }
    plt.rcParams.update(plot_style)


def save_horizontal_bar_chart(
    labels: list[str],
    values: list[float],
    title: str,
    xlabel: str,
    ylabel: str,
    output_path: Path,
    color: str,
) -> None:
    """Save a horizontal bar chart to a file."""
    maximum_value: float = max(values) if len(values) > 0 else 0.0
    plt.figure(figsize=(11, 6))
    plt.barh(labels, values, color=color)
    plt.gca().invert_yaxis()
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if len(values) > 0 and maximum_value == 0:
        plt.xlim(0, 1)
    plt.tight_layout()
    plt.savefig(output_path, dpi=180)
    plt.close()


def save_vertical_bar_chart(
    labels: list[str],
    values: list[float],
    title: str,
    xlabel: str,
    ylabel: str,
    output_path: Path,
    color: str,
) -> None:
    """Save a vertical bar chart to a file."""
    maximum_value: float = max(values) if len(values) > 0 else 0.0
    plt.figure(figsize=(10, 5))
    plt.bar(labels, values, color=color)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if len(values) > 0 and maximum_value == 0:
        plt.ylim(0, 1)
    plt.tight_layout()
    plt.savefig(output_path, dpi=180)
    plt.close()


def save_line_chart(
    x_values: list[int],
    y_values: list[float],
    title: str,
    xlabel: str,
    ylabel: str,
    output_path: Path,
    color: str,
) -> None:
    """Save a line chart to a file."""
    plt.figure(figsize=(12, 5))
    plt.plot(x_values, y_values, marker="o", color=color)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(output_path, dpi=180)
    plt.close()


def save_annotated_vertical_bar_chart(
    labels: list[str],
    values: list[float],
    annotations: list[str],
    title: str,
    xlabel: str,
    ylabel: str,
    output_path: Path,
    color: str,
) -> None:
    """Save a vertical bar chart with a text annotation above each bar."""
    max_value: float = max(values) if values else 0.0
    plt.figure(figsize=(max(8, len(labels) * 1.2), 5))
    x_positions: list[int] = list(range(len(labels)))
    plt.bar(x_positions, values, color=color)
    plt.xticks(x_positions, labels)
    for i, (value, annotation) in enumerate(zip(values, annotations)):
        plt.text(
            i,
            value + max_value * 0.01,
            annotation,
            ha="center",
            va="bottom",
            fontsize=7,
            color="#333",
        )
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(output_path, dpi=180)
    plt.close()


_SCATTER_COLORS: list[str] = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
]


def save_scatter_chart(
    x_values: list[float],
    y_values: list[float],
    cluster_ids: list[int],
    title: str,
    xlabel: str,
    ylabel: str,
    output_path: Path,
) -> None:
    """Save a scatter chart with one colour per cluster to a file."""
    unique_clusters: list[int] = sorted(set(cluster_ids))
    plt.figure(figsize=(10, 7))
    for i, cluster_id in enumerate(unique_clusters):
        indices: list[int] = [j for j, c in enumerate(cluster_ids) if c == cluster_id]
        xs: list[float] = [x_values[j] for j in indices]
        ys: list[float] = [y_values[j] for j in indices]
        color: str = _SCATTER_COLORS[i % len(_SCATTER_COLORS)]
        plt.scatter(xs, ys, c=color, label=f"Cluster {cluster_id}", alpha=0.45, s=18)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend(loc="best", fontsize=8)
    plt.tight_layout()
    plt.savefig(output_path, dpi=180)
    plt.close()


def save_charts(
    local_output: Path,
    films_by_genre: DataFrame,
    films_by_language: DataFrame,
    films_by_year: DataFrame,
    avg_vote_by_genre: DataFrame,
    missing_values_report: DataFrame,
    outlier_thresholds: DataFrame,
) -> None:
    """Generate and save all charts to the output directory."""
    configure_plot_style()

    # Top genres chart
    genre_rows: list[Row] = films_by_genre.limit(TOP_CHART_LIMIT).collect()
    genre_labels: list[str] = [str(row["genre_name"]) for row in genre_rows]
    genre_values: list[float] = [float(row["movie_count"]) for row in genre_rows]
    save_horizontal_bar_chart(
        genre_labels,
        genre_values,
        "Top 10 Most Common Genres",
        "Number of movies",
        "Genre",
        local_output / "top_genres.png",
        "#2a9d8f",
    )

    # Top languages chart
    language_rows: list[Row] = films_by_language.limit(TOP_CHART_LIMIT).collect()
    language_labels: list[str] = [str(row["original_language"]) for row in language_rows]
    language_values: list[float] = [float(row["movie_count"]) for row in language_rows]
    save_vertical_bar_chart(
        language_labels,
        language_values,
        "Top 10 Original Languages",
        "Original language",
        "Number of movies",
        local_output / "top_languages.png",
        "#457b9d",
    )

    # Films by year chart
    year_rows: list[Row] = (
        films_by_year.where(F.col("release_year") >= MIN_RELEASE_YEAR_FOR_YEAR_CHART)
        .orderBy("release_year")
        .collect()
    )
    year_labels: list[int] = [int(row["release_year"]) for row in year_rows]
    year_values: list[float] = [float(row["movie_count"]) for row in year_rows]
    save_line_chart(
        year_labels,
        year_values,
        "Movie Count Evolution Since 1980",
        "Release year",
        "Number of movies",
        local_output / "films_by_year.png",
        "#2a6f97",
    )

    # Average vote by genre chart
    vote_rows: list[Row] = avg_vote_by_genre.limit(TOP_CHART_LIMIT).collect()
    vote_labels: list[str] = [str(row["genre_name"]) for row in vote_rows]
    vote_values: list[float] = [float(row["avg_vote"]) for row in vote_rows]
    save_horizontal_bar_chart(
        vote_labels,
        vote_values,
        "Highest-Rated Genres With At Least 50 Movies",
        "Average rating",
        "Genre",
        local_output / "avg_vote_by_genre.png",
        "#588157",
    )

    # Missing values chart
    missing_rows: list[Row] = missing_values_report.collect()
    missing_labels: list[str] = [str(row["column_name"]) for row in missing_rows]
    missing_values: list[float] = [float(row["missing_count"]) for row in missing_rows]
    save_horizontal_bar_chart(
        missing_labels,
        missing_values,
        "Missing Values by Important Column",
        "Number of missing values",
        "Column",
        local_output / "missing_values.png",
        "#b56576",
    )

    # Outliers chart
    outlier_rows: list[Row] = outlier_thresholds.collect()
    outlier_labels: list[str] = [str(row["metric"]) for row in outlier_rows]
    outlier_values: list[float] = [float(row["outlier_count"]) for row in outlier_rows]
    save_vertical_bar_chart(
        outlier_labels,
        outlier_values,
        "Outliers Detected With the IQR Rule",
        "Metric",
        "Number of outliers",
        local_output / "outliers_by_metric.png",
        "#e76f51",
    )
