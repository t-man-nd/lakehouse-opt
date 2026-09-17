"""C1: reproducible late fare/tip updates and complete new trips in one MERGE.

Integrates the C1 branch's fare/tip/total update contract with main's B3 schema.
This is a single-writer classroom batch demo, not a source-database CDC connector.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path

from delta import DeltaTable
from pyspark.sql import SparkSession, functions as F

from src.delta_runtime import get_spark, latest_version, require_same_rows, require_unique_keys, snapshot, write_json
from src.silver import METADATA_COLUMNS, TARGET_TYPES

UPDATE_COLUMNS = ("fare_amount", "tip_amount", "total_amount")


def generate_fixture(
    spark: SparkSession, silver_path: str, output_path: str,
    *, seed: int = 42, updates: int = 5, inserts: int = 3,
) -> dict:
    """Select real keys in a stable seeded order; write absolute replacement values."""
    if updates < 1 or inserts < 1:
        raise ValueError("C1 demo requires at least one update and one insert")
    if Path(output_path).exists():
        raise FileExistsError(f"CDC fixture already exists: {output_path}")
    version = latest_version(spark, silver_path)
    before = snapshot(spark, silver_path, version)
    require_unique_keys(before)
    chosen = (
        before.filter("fare_amount > 0 AND tip_amount IS NOT NULL AND total_amount IS NOT NULL")
        .orderBy(F.sha2(F.concat(F.lit(f"{seed}:"), F.col("trip_id")), 256), "trip_id")
        .limit(updates + inserts).collect()
    )
    if len(chosen) < updates + inserts:
        raise ValueError("Not enough eligible Silver rows to generate the CDC fixture")
    rows, changes = [], []
    for index, row in enumerate(chosen):
        record = row.asDict()
        if index < updates:
            old = {name: record[name] for name in UPDATE_COLUMNS}
            record["fare_amount"] = round(record["fare_amount"] + 1.25, 2)
            record["tip_amount"] = round(record["tip_amount"] + 2.0, 2)
            record["total_amount"] = round(record["total_amount"] + 3.25, 2)
            changes.append({"trip_id": record["trip_id"], "before": old,
                            "after": {name: record[name] for name in UPDATE_COLUMNS}})
        else:
            record["trip_id"] = f"cdc_{seed}_{index - updates + 1:04d}"
            for name in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
                if record[name] is not None:
                    record[name] += timedelta(days=1)
            record["_batch_id"] = f"cdc_seed_{seed}"
            record["_source_file"] = Path(output_path).resolve().as_uri()
            record["_ingest_ts"] = datetime(2025, 4, 2, 0, 0, 0)
        rows.append(record)
    new_ids = [row["trip_id"] for row in rows[updates:]]
    if before.filter(F.col("trip_id").isin(new_ids)).limit(1).count():
        raise ValueError("Generated insert keys already exist; generate from a B3 baseline")
    spark.createDataFrame(rows, before.schema).coalesce(1).write.mode("errorifexists").parquet(output_path)
    result = {"seed": seed, "source_silver_version": version, "updates": changes, "insert_ids": new_ids}
    write_json(Path(output_path).parent / (Path(output_path).name + ".json"), result)
    return result


def apply_cdc(spark: SparkSession, silver_path: str, cdc_path: str) -> dict:
    version = latest_version(spark, silver_path)
    before = snapshot(spark, silver_path, version)
    incoming = spark.read.parquet(cdc_path)
    required = set(TARGET_TYPES) | set(METADATA_COLUMNS)
    if not required.issubset(before.columns):
        raise ValueError("CDC target must be the typed B3 Silver table")
    if set(incoming.columns) != set(before.columns):
        raise ValueError("CDC source must provide every target column for complete inserts")
    if any(incoming.schema[name].dataType != before.schema[name].dataType for name in before.columns):
        raise ValueError("CDC source types must match the Silver schema")
    require_unique_keys(before)
    require_unique_keys(incoming)
    invalid = F.lit(False)
    for name in UPDATE_COLUMNS:
        invalid = invalid | F.col(name).isNull() | F.isnan(name) | (F.abs(F.col(name)) == float("inf"))
    invalid = (invalid | (F.col("fare_amount") <= 0)
               | F.col("tpep_pickup_datetime").isNull()
               | F.col("PULocationID").isNull() | F.col("DOLocationID").isNull())
    for name in METADATA_COLUMNS:
        invalid = invalid | F.col(name).isNull()
        if name != "_ingest_ts":
            invalid = invalid | (F.trim(name) == "")
    if incoming.filter(invalid).limit(1).count():
        raise ValueError("Invalid CDC fixture: amounts, trip fields or provenance are missing/invalid")
    joined = before.alias("t").join(incoming.alias("s"), "trip_id")
    changed = F.lit(False)
    for name in UPDATE_COLUMNS:
        changed = changed | ~F.col(f"t.{name}").eqNullSafe(F.col(f"s.{name}"))
    matched_count = joined.count()
    updated_count = joined.filter(changed).count()
    inserts = incoming.join(before.select("trip_id"), "trip_id", "left_anti")
    inserted_count = inserts.count()
    before_count = before.count()
    source_count = incoming.count()
    if source_count != matched_count + inserted_count or source_count == 0:
        raise ValueError("CDC source is empty or cannot be reconciled")
    update_condition = " OR ".join(f"NOT (t.`{name}` <=> s.`{name}`)" for name in UPDATE_COLUMNS)
    (
        DeltaTable.forPath(spark, silver_path).alias("t")
        .merge(incoming.alias("s"), "t.trip_id = s.trip_id")
        .whenMatchedUpdate(condition=update_condition, set={name: f"s.`{name}`" for name in UPDATE_COLUMNS})
        .whenNotMatchedInsertAll().execute()
    )
    after_version = latest_version(spark, silver_path)
    after = snapshot(spark, silver_path, after_version)
    after_count = after.count()
    if after_count != before_count + inserted_count:
        raise RuntimeError("CDC row conservation failed")
    expected = before.alias("t").join(incoming.alias("s"), F.col("t.trip_id") == F.col("s.trip_id"), "left").select(*[
        (F.when(F.col("s.trip_id").isNotNull(), F.col(f"s.`{name}`")).otherwise(F.col(f"t.`{name}`"))
         if name in UPDATE_COLUMNS else F.col(f"t.`{name}`")).alias(name)
        for name in before.columns
    ]).unionByName(inserts)
    require_same_rows(after, expected, "CDC persisted values and untouched columns")
    metrics = DeltaTable.forPath(spark, silver_path).history(1).first()["operationMetrics"]
    if int(metrics["numTargetRowsUpdated"]) != updated_count or int(metrics["numTargetRowsInserted"]) != inserted_count:
        raise RuntimeError("MERGE history metrics disagree with source reconciliation")
    return {"before_version": version, "after_version": after_version, "before_count": before_count,
            "source_count": source_count, "matched_count": matched_count, "updated_count": updated_count,
            "inserted_count": inserted_count, "after_count": after_count, "operation_metrics": metrics}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-path", default="data/silver/taxi_trips")
    parser.add_argument("--cdc-path", default="data/cdc/late_updates.parquet")
    parser.add_argument("--generate", action="store_true", help="Generate once from the current baseline before merging")
    parser.add_argument("--output", default="data/cdc/merge_result.json")
    args = parser.parse_args()
    spark = get_spark()
    try:
        if args.generate:
            generate_fixture(spark, args.silver_path, args.cdc_path)
        result = apply_cdc(spark, args.silver_path, args.cdc_path)
        write_json(args.output, result)
        print(result)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
