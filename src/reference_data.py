"""Reference data: TLC taxi zones and the MTA list of CBD (Congestion Relief Zone) zones.

Both files are small and replaced wholesale when they change, so they are
overwritten (not merged). Bronze keeps the raw CSV text; Silver `dim_zone`
is typed and adds two analysis flags:

* is_cbd      zone lies in the Congestion Relief Zone (from the MTA dataset)
* is_unknown  TLC placeholder zones (264 "Unknown", 265 "Outside of NYC")
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import DQThresholds, LakehousePaths, get_paths
from src.delta_utils import commit_metadata, latest_version


def read_csv_as_strings(spark: SparkSession, path: Path):
    return spark.read.option("header", "true").option("inferSchema", "false").csv(str(path))


def run(spark: SparkSession, paths: Optional[LakehousePaths] = None) -> Dict[str, object]:
    paths = paths or get_paths()
    zone_csv = paths.raw_ref_dir / "taxi_zone_lookup.csv"
    cbd_csv = paths.raw_ref_dir / "cbd_zones.csv"
    if not zone_csv.exists():
        raise FileNotFoundError(f"{zone_csv} missing; run scripts/download_sources.py")

    zones_raw = read_csv_as_strings(spark, zone_csv).withColumn("_source_file", F.lit(zone_csv.name))
    with commit_metadata(spark, {"layer": "bronze", "table": "zone_lookup"}):
        zones_raw.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(paths.bronze_zone_lookup)

    cbd_available = cbd_csv.exists()
    if cbd_available:
        cbd_raw = read_csv_as_strings(spark, cbd_csv).withColumn("_source_file", F.lit(cbd_csv.name))
        with commit_metadata(spark, {"layer": "bronze", "table": "cbd_zones"}):
            cbd_raw.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(paths.bronze_cbd_zones)
        source = F.col("source") if "source" in cbd_raw.columns else F.lit("mta_yfdc_w5jh")
        cbd_ids = (cbd_raw.select(F.col("LocationID").try_cast("int").alias("LocationID"), source.alias("cbd_source"))
                   .dropna(subset=["LocationID"]).dropDuplicates(["LocationID"]))
    else:
        print("[reference] WARNING: cbd_zones.csv missing; CBD analysis will report CBD_UNAVAILABLE")
        cbd_ids = spark.createDataFrame([], "LocationID int, cbd_source string")

    unknown_ids = list(DQThresholds().unknown_zone_ids)
    dim = (
        zones_raw.select(
            F.col("LocationID").try_cast("int").alias("LocationID"),
            F.trim("Borough").alias("borough"),
            F.trim("Zone").alias("zone"),
            F.trim("service_zone").alias("service_zone"),
        )
        .join(cbd_ids.withColumn("is_cbd", F.lit(True)), "LocationID", "left")
        .withColumn("cbd_source", F.coalesce("cbd_source", F.lit("not_cbd")))
        .withColumn("is_cbd", F.coalesce("is_cbd", F.lit(False)))
        .withColumn("is_unknown", F.col("LocationID").isin(*unknown_ids) | F.col("borough").isin("Unknown", "N/A"))
        .withColumn("cbd_reference_available", F.lit(cbd_available))
    )
    with commit_metadata(spark, {"layer": "silver", "table": "dim_zone"}):
        dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(paths.silver_dim_zone)

    result = spark.read.format("delta").load(paths.silver_dim_zone)
    return {
        "zones": result.count(),
        "cbd_zones": result.filter("is_cbd").count(),
        "cbd_zones_by_source": {r["cbd_source"]: r["count"] for r in
                                result.filter("is_cbd").groupBy("cbd_source").count().collect()},
        "unknown_zones": result.filter("is_unknown").count(),
        "cbd_reference_available": cbd_available,
        "dim_zone_version": latest_version(spark, paths.silver_dim_zone),
    }
