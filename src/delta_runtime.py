"""Shared Spark/Delta runtime and snapshot checks for the classroom pipeline."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, functions as F


def get_spark(master: str = "local[2]", app_name: str = "LakehouseThroughE1") -> SparkSession:
    if master.startswith("local"):
        # Python-created CDC rows start workers: they must use the driver venv.
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    # Keep the small demo defaults; scale trials explicitly set these variables.
    partitions = int(os.environ.get("LAKEHOUSE_SHUFFLE_PARTITIONS", "2"))
    if partitions < 1:
        raise ValueError("LAKEHOUSE_SHUFFLE_PARTITIONS must be positive")
    builder = (
        SparkSession.builder.appName(app_name).master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", os.environ.get("LAKEHOUSE_DRIVER_MEMORY", "1g"))
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config("spark.default.parallelism", str(partitions))
        # VACUUM uses this bound for file discovery; the Spark default 10,000
        # launches thousands of empty tasks for the few local demo files.
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", str(partitions))
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def latest_version(spark: SparkSession, path: str) -> int:
    return int(DeltaTable.forPath(spark, path).history(1).first()["version"])


def snapshot(spark: SparkSession, path: str, version: int | None = None) -> DataFrame:
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.load(path)


def require_unique_keys(frame: DataFrame) -> None:
    if frame.filter(F.col("trip_id").isNull() | (F.trim("trip_id") == "")).limit(1).count():
        raise ValueError("trip_id must be nonempty and non-null")
    if frame.groupBy("trip_id").count().filter("count > 1").limit(1).count():
        raise ValueError("Duplicate trip_id: CDC/evolution requires unique keys")


def require_same_rows(left: DataFrame, right: DataFrame, label: str) -> None:
    """Multiset comparison, including nulls; intentionally sized for a class demo."""
    right = right.select(left.columns)
    if left.exceptAll(right).limit(1).count() or right.exceptAll(left).limit(1).count():
        raise ValueError(f"Row mismatch: {label}")


def write_json(path: str | Path, value: object) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(value, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
