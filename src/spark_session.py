"""One place to build a SparkSession with Delta Lake and the project's SQL settings.

Pinned settings (the report cites these):
* spark.sql.ansi.enabled=true        -> bad casts must use try_cast, never kill the job silently
* spark.sql.session.timeZone=UTC     -> TLC timestamps are NYC wall-clock values stored without
                                        a zone; pinning UTC means Spark never shifts them
* timeParserPolicy=CORRECTED         -> strict datetime parsing (no legacy lenient parser)
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession


def _default_driver_memory() -> str:
    """Half of total RAM (2–12 GB), so a laptop run does not die with the 1 GB JVM default."""
    try:
        total_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        return f"{max(2, min(12, int(total_bytes / (1024 ** 3) // 2)))}g"
    except (ValueError, OSError, AttributeError):
        return "4g"


def create_spark(
    app_name: str = "lakehouse",
    master: Optional[str] = None,
    driver_memory: Optional[str] = None,
    shuffle_partitions: Optional[int] = None,
    with_delta: bool = True,
) -> SparkSession:
    """Build (or reuse) a local SparkSession configured for this project."""
    java_home = os.environ.get("JAVA_HOME")
    if not (java_home and os.path.exists(os.path.join(java_home, "bin", "java"))) and not shutil.which("java"):
        raise RuntimeError(
            "Java was not found. Spark 4 needs Java 17 or 21: install OpenJDK 17 and set JAVA_HOME "
            "(see docs/PIPELINE_RUNBOOK.md, section 1)."
        )
    master = master or os.environ.get("SPARK_MASTER", "local[*]")
    driver_memory = driver_memory or os.environ.get("SPARK_DRIVER_MEMORY") or _default_driver_memory()
    shuffle_partitions = shuffle_partitions or int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "64"))

    # Spill/shuffle files go to a project folder on the data disk, not /tmp (which may be small
    # or quota-limited). SPARK_LOCAL_DIRS, if set, still takes precedence inside Spark.
    local_dir = os.environ.get("LAKEHOUSE_SPARK_TMP") or os.path.join(
        os.environ.get("LAKEHOUSE_ROOT", str(Path(__file__).resolve().parents[1])), "data", "spark-tmp")
    os.makedirs(local_dir, exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.local.dir", local_dir)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.ui.showConsoleProgress", "false")
        # Smaller input splits -> smaller per-task heap, which matters on a single machine.
        .config("spark.sql.files.maxPartitionBytes", os.environ.get("SPARK_MAX_PARTITION_BYTES", "64m"))
    )
    if with_delta:
        from delta import configure_spark_with_delta_pip

        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        builder = configure_spark_with_delta_pip(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    print(f"[spark] driver memory {driver_memory} · shuffle partitions {shuffle_partitions} · "
          f"spill dir {local_dir} (override with SPARK_DRIVER_MEMORY / SPARK_SHUFFLE_PARTITIONS)")
    return spark
