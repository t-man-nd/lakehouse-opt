#!/usr/bin/env python3
"""cdc_merge.py

Delta MERGE utility for the Lakehouse project.

* Reads the current *Silver* table (Delta format).
* Reads a CDC fixture located at ``data/cdc/late_updates.parquet``.
* Performs a Delta ``MERGE`` that:
    - Updates ``fare_amount``, ``tip_amount`` and ``total_amount`` when ``trip_id`` exists.
    - Inserts all rows that have a new ``trip_id``.
* Writes the result back to the Silver table.

The script is deliberately lightweight – it does **not** generate the CDC fixture
(it is produced by ``generator.py``).  It simply demonstrates the separation of
concerns: ``generator.py`` creates ``late_updates.parquet`` and ``cdc_merge.py``
applies it.
"""

import argparse
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, DataFrame

# DeltaTable is part of the ``delta`` package.  Import lazily so the script can be
# imported without the package being present (useful for static analysis).
try:
    from delta.tables import DeltaTable
except ImportError:  # pragma: no cover
    DeltaTable = None  # type: ignore


def _init_spark(app_name: str = "cdc_merge") -> SparkSession:
    """Create a Spark session with Delta support.

    The function mirrors the minimal configuration used elsewhere in the repo
    (e.g. ``bronze.py``).  ``spark.sql.extensions`` and the Delta catalog are
    enabled so that ``spark.read.format("delta")`` works out‑of‑the‑box.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return builder.getOrCreate()


def _read_silver(spark: SparkSession, path: Path) -> DataFrame:
    """Read the Silver Delta table.

    Parameters
    ----------
    spark:
        Active Spark session.
    path:
        Path to the Silver table directory (e.g. ``data/silver/taxi_trips``).
    """
    return spark.read.format("delta").load(str(path))


def _read_cdc(spark: SparkSession, path: Path) -> DataFrame:
    """Read the CDC fixture.

    The fixture is a Parquet file that contains the columns ``trip_id``,
    ``fare_amount``, ``tip_amount`` and ``total_amount``.  Any additional columns
    are ignored – they are simply dropped before the merge.
    """
    df = spark.read.parquet(str(path))
    # Keep only the columns we need for the merge.
    return df.select(
        "trip_id",
        "fare_amount",
        "tip_amount",
        "total_amount",
    )


def _perform_merge(silver_path: Path, cdc_path: Path, spark: SparkSession) -> None:
    """Execute the Delta MERGE.

    The function uses the Delta ``DeltaTable`` API to merge ``cdc_path`` into the
    Silver table located at ``silver_path``.
    """
    if DeltaTable is None:  # pragma: no cover
        raise RuntimeError(
            "DeltaTable class not available – install the 'delta-spark' package."
        )

    silver_dt = DeltaTable.forPath(spark, str(silver_path))
    changes_df = _read_cdc(spark, cdc_path)

    (
        silver_dt.alias("target")
        .merge(
            changes_df.alias("source"),
            "target.trip_id = source.trip_id",
        )
        .whenMatchedUpdate(
            set={
                "fare_amount": "source.fare_amount",
                "tip_amount": "source.tip_amount",
                "total_amount": "source.total_amount",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"✅ MERGE completed – Silver table at {silver_path} updated.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delta MERGE CDC updates into Silver.")
    parser.add_argument(
        "--silver-path",
        type=Path,
        default=Path("data/silver/taxi_trips"),
        help="Path to the Silver Delta table.",
    )
    parser.add_argument(
        "--cdc-path",
        type=Path,
        default=Path("data/cdc/late_updates.parquet"),
        help="Path to the CDC Parquet fixture.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = _init_spark()
    try:
        _perform_merge(args.silver_path, args.cdc_path, spark)
    finally:
        spark.stop()


if __name__ == "__main__":  # pragma: no cover
    main()
