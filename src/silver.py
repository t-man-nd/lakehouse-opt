"""B3: B1-v1.0 cleaning and quarantine, before CDC/schema evolution."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, functions as F

DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
TARGET_TYPES = {
    "trip_id": "string",
    "VendorID": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int",
    "trip_distance": "double",
    "RatecodeID": "int",
    "store_and_fwd_flag": "string",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "Airport_fee": "double",
}
METADATA_COLUMNS = ("_ingest_ts", "_source_file", "_batch_id")
REASONS = (
    "INVALID_PICKUP_DATETIME", "MISSING_LOCATION", "INVALID_FARE", "DUPLICATE_TRIP",
)
_TYPED = "__b3_typed"


def _column(name: str):
    return F.col("`" + name.replace("`", "``") + "`")


def _classify(bronze: DataFrame) -> DataFrame:
    """Keep raw values alongside ANSI-safe conversions; apply only B1 rules.

    B1 defects are disjoint. For overlapping defects, use the first reason in
    pickup/location/fare order; deduplication happens among remaining candidates.
    A null/unparseable fare is not <= 0 and adds no fifth rejection rule.
    """
    bronze.sparkSession.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    bronze.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")
    missing = set(TARGET_TYPES).difference(bronze.columns)
    if missing:
        raise ValueError(f"Bronze is missing B1 columns: {sorted(missing)}")
    if {_TYPED, "reject_reason"}.intersection(bronze.columns):
        raise ValueError("Bronze contains reserved B3 column names")

    converted = []
    for name, target in TARGET_TYPES.items():
        if target == "timestamp":
            # Unlike a direct timestamp cast, this enforces the agreed format.
            value = F.try_to_timestamp(_column(name).try_cast("string"), F.lit(DATE_FORMAT))
        else:
            value = _column(name).try_cast(target)
        converted.append(value.alias(name))
    parsed = bronze.withColumn(_TYPED, F.struct(*converted))
    typed = F.col(_TYPED)
    return parsed.withColumn(
        "reject_reason",
        F.when(typed["tpep_pickup_datetime"].isNull(), "INVALID_PICKUP_DATETIME")
        .when(
            typed["PULocationID"].isNull() | typed["DOLocationID"].isNull(),
            "MISSING_LOCATION",
        )
        .when(typed["fare_amount"] <= 0, "INVALID_FARE"),
    )


def _verify_manifest(result: dict, manifest_path: str) -> None:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    if manifest["contract_version"] != "B1-v1.0":
        raise ValueError("Expected a B1-v1.0 error manifest")
    expected = manifest["totals"]
    pairs = {
        "bronze_count": "raw_rows",
        "silver_count": "expected_silver_rows",
        "rejected_count": "expected_rejected_rows",
    }
    for actual_key, expected_key in pairs.items():
        if result[actual_key] != expected[expected_key]:
            raise ValueError(
                f"B1 reconciliation: {actual_key}={result[actual_key]}, "
                f"expected {expected[expected_key]}"
            )
    for reason, key in {
        "DUPLICATE_TRIP": "duplicate_excess_rows",
        "INVALID_PICKUP_DATETIME": "malformed_date_rows",
        "MISSING_LOCATION": "missing_location_rows",
        "INVALID_FARE": "invalid_fare_rows",
    }.items():
        if result["reject_reason_counts"][reason] != expected[key]:
            raise ValueError(f"B1 reconciliation: wrong count for {reason}")


def build(
    spark: SparkSession,
    bronze_dir: str = "data/bronze/taxi_trips",
    silver_dir: str = "data/silver/taxi_trips",
    rejected_dir: str = "data/silver/silver_rejected",
    *,
    manifest_path: str | None = None,
) -> dict:
    """Rebuild the B3 snapshot and return counts, without modifying Bronze.

    Invoke before C1/C2: overwrite replaces the *current* Silver snapshot and
    would replace later CDC updates. Delta history remains available.
    The two output tables are separate Delta transactions, not one transaction.
    """
    paths = [str(Path(p).resolve()) for p in (bronze_dir, silver_dir, rejected_dir)]
    if len(set(paths)) != 3:
        raise ValueError("Bronze, Silver and rejected paths must be distinct")

    # Pin one input version so a concurrent Bronze append cannot change counts.
    version = DeltaTable.forPath(spark, bronze_dir).history(1).first()["version"]
    bronze = spark.read.format("delta").option("versionAsOf", version).load(bronze_dir)
    missing_metadata = set(METADATA_COLUMNS).difference(bronze.columns)
    if missing_metadata:
        raise ValueError(f"Missing B2 metadata: {sorted(missing_metadata)}")
    classified = _classify(bronze).persist()
    kept = None
    try:
        bronze_count = classified.count()
        missing_metadata_count = classified.filter(
            F.col("_ingest_ts").isNull()
            | F.col("_source_file").isNull() | (F.col("_source_file") == "")
            | F.col("_batch_id").isNull() | (F.col("_batch_id") == "")
        ).count()
        if missing_metadata_count:
            raise ValueError(f"B2 metadata is missing in {missing_metadata_count} rows")

        candidates = classified.filter(F.col("reject_reason").isNull()).drop("reject_reason")
        kept = candidates.dropDuplicates(["trip_id"]).persist()
        silver_count = kept.count()
        raw_columns = [_column(name) for name in bronze.columns]
        invalid = classified.filter(F.col("reject_reason").isNotNull()).select(
            *raw_columns, "reject_reason",
        )
        # Multiset subtraction preserves N-1 copies even when rows are identical.
        # Cache the actual survivor used for BOTH Silver and subtraction.
        duplicates = candidates.exceptAll(kept).select(*raw_columns).withColumn(
            "reject_reason", F.lit("DUPLICATE_TRIP"),
        )
        rejected = invalid.unionByName(duplicates)
        reason_counts = dict.fromkeys(REASONS, 0)
        reason_counts.update({
            row["reject_reason"]: row["count"]
            for row in rejected.groupBy("reject_reason").count().collect()
        })
        rejected_count = sum(reason_counts.values())
        if silver_count + rejected_count != bronze_count:
            raise RuntimeError("B3 row conservation failed; no output written")
        result = {
            "bronze_count": bronze_count,
            "silver_count": silver_count,
            "rejected_count": rejected_count,
            "reject_reason_counts": reason_counts,
            "bronze_version": version,
        }
        if manifest_path is not None:
            _verify_manifest(result, manifest_path)

        # Keep metadata and any future columns without inventing cleaning rules.
        extra = [_column(name) for name in bronze.columns if name not in TARGET_TYPES]
        silver = kept.select(f"{_TYPED}.*", *extra)
        silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_dir)
        rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(rejected_dir)
        for path, expected_count in ((silver_dir, silver_count), (rejected_dir, rejected_count)):
            if spark.read.format("delta").load(path).count() != expected_count:
                raise RuntimeError(f"Persisted Delta count differs at {path}")
        return result
    finally:
        if kept is not None:
            kept.unpersist()
        classified.unpersist()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-dir", default="data/bronze/taxi_trips")
    parser.add_argument("--silver-dir", default="data/silver/taxi_trips")
    parser.add_argument("--rejected-dir", default="data/silver/silver_rejected")
    parser.add_argument("--manifest", help="Check B1 totals before writing output")
    parser.add_argument("--output", help="Write the count summary as JSON")
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args()
    builder = (
        SparkSession.builder.appName("SilverCleaning").master(args.master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = build(
            spark, args.bronze_dir, args.silver_dir, args.rejected_dir,
            manifest_path=args.manifest,
        )
        text = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        print(text)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
