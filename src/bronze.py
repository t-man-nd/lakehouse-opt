"""B2 append-only ingestion and the E1 public entry point."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

from delta import DeltaTable
from pyspark.sql import SparkSession, functions as F

MONTH_BATCHES = {f"2025-{month:02d}": f"batch_{month:02d}.json" for month in range(1, 4)}


def _batch_files(months: Sequence[str] | None) -> list[str]:
    """The fixed B1 contract maps Jan/Feb/Mar 2025 to batches 01/02/03."""
    selected = list(MONTH_BATCHES) if months is None else list(months)
    if not selected or len(set(selected)) != len(selected):
        raise ValueError("months must contain distinct B1 months")
    unknown = set(selected) - set(MONTH_BATCHES)
    if unknown:
        raise ValueError(f"B1 months must be YYYY-MM in {sorted(MONTH_BATCHES)}; got {sorted(unknown)}")
    return [MONTH_BATCHES[month] for month in selected]


def run(
    spark: SparkSession,
    months: Sequence[str] | None = None,
    overwrite: bool = False,
    *,
    raw_dir: str = "data/raw",
    bronze_dir: str = "data/bronze/taxi_trips",
) -> dict:
    """Append all raw records, including defects, and return reconciled counts.

    ``overwrite=True`` is rejected: Bronze is append-only. Repeating this call
    intentionally appends the input again. The E1 runner uses a fresh directory
    for each run; use that entry point for a reproducible complete pipeline.
    """
    if overwrite:
        raise ValueError("Bronze is append-only; choose a fresh run directory instead of overwrite=True")
    files = [Path(raw_dir) / name for name in _batch_files(months)]
    target = Path(bronze_dir).resolve()
    source = Path(raw_dir).resolve()
    if target == source or target in source.parents or source in target.parents:
        raise ValueError("Raw and Bronze directories must not overlap")
    for path in files:
        if not path.is_file():
            raise FileNotFoundError(f"Missing raw B1 input: {path}")
    if target.exists() and not DeltaTable.isDeltaTable(spark, bronze_dir):
        raise FileExistsError(f"Bronze destination exists but is not a Delta table: {target}")
    before = spark.read.format("delta").load(bronze_dir).count() if target.exists() else 0
    appended = 0
    for path in files:
        raw = spark.read.option("mode", "FAILFAST").json(str(path))
        if {"_ingest_ts", "_source_file", "_batch_id"}.intersection(raw.columns):
            raise ValueError("Raw source must not supply B2 ingestion metadata")
        batch_count = raw.count()
        (raw.withColumn("_ingest_ts", F.current_timestamp())
         .withColumn("_source_file", F.input_file_name())
         .withColumn("_batch_id", F.lit(path.stem))
         .write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_dir))
        appended += batch_count
        print(f"[B2] Appended {path.name}: {batch_count} rows", flush=True)
    written = spark.read.format("delta").load(bronze_dir)
    count = written.count()
    missing_metadata = written.filter(
        "_ingest_ts IS NULL OR _source_file IS NULL OR _source_file = '' "
        "OR _batch_id IS NULL OR _batch_id = ''"
    ).count()
    if count != before + appended or missing_metadata:
        raise RuntimeError("Bronze row conservation or required metadata validation failed")
    return {"bronze_count": count, "before_count": before, "appended_count": appended,
            "missing_metadata_count": missing_metadata,
            "batch_files": [path.name for path in files], "bronze_dir": str(target)}


def run_bronze(spark: SparkSession, raw_dir: str = "data/raw",
               bronze_dir: str = "data/bronze/taxi_trips") -> dict:
    """Compatibility entry point used by the B2/B3 exercises."""
    result = run(spark, raw_dir=raw_dir, bronze_dir=bronze_dir)
    return {"bronze_count": result["bronze_count"]}


if __name__ == "__main__":
    from src.delta_runtime import get_spark
    spark = get_spark(app_name="BronzeIngest")
    try:
        print(run(spark))
    finally:
        spark.stop()
