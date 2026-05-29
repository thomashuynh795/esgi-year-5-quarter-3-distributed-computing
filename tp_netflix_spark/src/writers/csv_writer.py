from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import DataFrame


def save_result(df: DataFrame, local_path: Path, limit: int | None = None) -> None:
    """Save a DataFrame to a single CSV file."""
    output_df: DataFrame = df.limit(limit) if limit is not None else df
    temp_dir: Path = local_path.with_name(f".{local_path.stem}_spark_tmp")

    if local_path.exists():
        local_path.unlink()
    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    output_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(temp_dir))
    csv_part: Path = next(temp_dir.glob("part-*.csv"))
    shutil.move(str(csv_part), local_path)
    shutil.rmtree(temp_dir)
