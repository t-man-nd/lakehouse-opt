"""C2: append surcharge_fee to Bronze and Silver, once per batch.

Layers commit independently. A rerun can resume after the Bronze commit;
an identical batch is a no-op, while reusing its ID for changed data is rejected.
The replay check assumes one writer, not concurrent exactly-once delivery.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from delta import DeltaTable
from pyspark.sql import SparkSession, functions as F

from src.delta_runtime import get_spark, latest_version, require_same_rows, require_unique_keys, snapshot, write_json
from src.silver import METADATA_COLUMNS, TARGET_TYPES, _classify, _TYPED


def ensure_baseline(
    spark: SparkSession, raw_dir: str, bronze_dir: str, silver_dir: str,
    rejected_dir: str = "data/silver/silver_rejected",
) -> None:
    paths = [Path(p).resolve() for p in (bronze_dir, silver_dir, rejected_dir)]
    if len(set(paths)) != 3:
        raise ValueError("Bronze, Silver and rejected paths must be distinct")
    if not DeltaTable.isDeltaTable(spark, bronze_dir):
        from src.bronze import run_bronze
        run_bronze(spark, raw_dir, bronze_dir)
    if not DeltaTable.isDeltaTable(spark, silver_dir):
        if "surcharge_fee" in snapshot(spark, bronze_dir).columns:
            raise ValueError("Create B3 before schema evolution; baseline Bronze already evolved")
        from src.silver import build
        build(spark, bronze_dir, silver_dir, rejected_dir)


def _clean_batch(raw):
    if "surcharge_fee" not in raw.columns:
        raise ValueError("C2 batch must contain surcharge_fee")
    classified = _classify(raw)
    if classified.filter(F.col("reject_reason").isNotNull()).limit(1).count():
        raise ValueError("C2 fixture violates B1 rules; no rows were silently discarded")
    require_unique_keys(raw)
    extra = [F.col(f"`{name}`") for name in raw.columns if name not in TARGET_TYPES]
    typed = classified.select(f"{_TYPED}.*", *extra).withColumn("surcharge_fee", F.col("surcharge_fee").try_cast("double"))
    if typed.filter(F.col("surcharge_fee").isNull() | F.isnan("surcharge_fee")
                    | (F.abs(F.col("surcharge_fee")) == float("inf"))).limit(1).count():
        raise ValueError("C2 surcharge_fee must be a finite number")
    if typed.limit(1).count() == 0:
        raise ValueError("C2 batch must not be empty")
    return typed


def evolve_bronze(
    spark: SparkSession, batch_path: str = "data/raw/batch_04.json",
    bronze_dir: str = "data/bronze/taxi_trips", batch_id: str = "batch_04",
) -> dict:
    if not batch_id.strip():
        raise ValueError("batch_id must be nonempty")
    version = latest_version(spark, bronze_dir)
    before = snapshot(spark, bronze_dir, version)
    raw = spark.read.json(batch_path)
    if set(METADATA_COLUMNS).intersection(raw.columns):
        raise ValueError("C2 raw source must not supply ingestion metadata")
    _clean_batch(raw)
    old = before.filter(F.col("_batch_id") == batch_id)
    before_count, batch_count = before.count(), raw.count()
    replay = old.limit(1).count() > 0
    if replay:
        if set(raw.columns) != set(before.columns) - set(METADATA_COLUMNS):
            raise ValueError("Existing C2 batch has a different schema")
        require_same_rows(raw, old.select(raw.columns), "C2 batch ID reused with changed data")
    else:
        if raw.join(before.select("trip_id"), "trip_id").limit(1).count():
            raise ValueError("C2 insert batch overlaps existing Bronze trip_id")
        (raw.withColumn("_ingest_ts", F.current_timestamp()).withColumn("_source_file", F.input_file_name())
         .withColumn("_batch_id", F.lit(batch_id)).write.format("delta").mode("append")
         .option("mergeSchema", "true").save(bronze_dir))
    after_version = latest_version(spark, bronze_dir)
    after = snapshot(spark, bronze_dir, after_version)
    added = 0 if replay else batch_count
    if after.count() != before_count + added:
        raise RuntimeError("C2 Bronze row conservation failed")
    return {"before_version": version, "after_version": after_version, "bronze_count_before": before_count,
            "bronze_count_after": before_count + added, "new_rows": added, "replayed": replay,
            "has_col_before": "surcharge_fee" in before.columns, "has_col_after": "surcharge_fee" in after.columns}


def evolve_silver(
    spark: SparkSession, bronze_dir: str = "data/bronze/taxi_trips",
    silver_dir: str = "data/silver/taxi_trips", batch_id: str = "batch_04",
) -> dict:
    if Path(bronze_dir).resolve() == Path(silver_dir).resolve():
        raise ValueError("Bronze and Silver paths must be distinct")
    version = latest_version(spark, silver_dir)
    before = snapshot(spark, silver_dir, version)
    require_unique_keys(before)
    bronze_version = latest_version(spark, bronze_dir)
    raw = snapshot(spark, bronze_dir, bronze_version).filter(F.col("_batch_id") == batch_id)
    incoming = _clean_batch(raw)
    old = before.filter(F.col("_batch_id") == batch_id)
    before_count, new_count = before.count(), incoming.count()
    replay = old.limit(1).count() > 0
    if replay:
        require_same_rows(incoming, old, "C2 Silver replay differs")
    else:
        if incoming.join(before.select("trip_id"), "trip_id").limit(1).count():
            raise ValueError("C2 insert batch overlaps existing Silver trip_id; use CDC for updates")
        incoming.write.format("delta").mode("append").option("mergeSchema", "true").save(silver_dir)
    after_version = latest_version(spark, silver_dir)
    after = snapshot(spark, silver_dir, after_version)
    added = 0 if replay else new_count
    if after.count() != before_count + added:
        raise RuntimeError("C2 Silver row conservation failed")
    if not replay:
        require_same_rows(before, after.filter(F.col("_batch_id") != batch_id).select(before.columns), "C2 preserves preexisting Silver rows")
        require_same_rows(incoming, after.filter(F.col("_batch_id") == batch_id), "C2 inserted values")
    return {"before_version": version, "after_version": after_version, "silver_count_before": before_count,
            "silver_count_after": before_count + added, "clean_new_rows": added, "replayed": replay,
            "has_col_before": "surcharge_fee" in before.columns, "has_col_after": "surcharge_fee" in after.columns,
            "bronze_version": bronze_version}


def verify_evidence(
    spark: SparkSession, bronze_dir: str = "data/bronze/taxi_trips",
    silver_dir: str = "data/silver/taxi_trips", batch_id: str = "batch_04",
) -> dict:
    summary = {}
    for name, path in (("BRONZE", bronze_dir), ("SILVER", silver_dir)):
        frame = snapshot(spark, path)
        old = frame.filter(F.col("_batch_id") != batch_id)
        new = frame.filter(F.col("_batch_id") == batch_id)
        old_count, new_count = old.count(), new.count()
        if old_count == 0 or new_count == 0:
            raise ValueError("C2 evidence requires old and new rows")
        if old.filter(F.col("surcharge_fee").isNotNull()).count() or new.filter(F.col("surcharge_fee").isNull()).count():
            raise ValueError("Expected all old rows NULL and every new row non-NULL")
        summary[name] = {"total_rows": old_count + new_count, "null_rows": old_count, "not_null_rows": new_count,
                         "latest_version": latest_version(spark, path)}
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--batch-file", default="data/raw/batch_04.json")
    parser.add_argument("--bronze-dir", default="data/bronze/taxi_trips")
    parser.add_argument("--silver-dir", default="data/silver/taxi_trips")
    parser.add_argument("--rejected-dir", default="data/silver/silver_rejected")
    parser.add_argument("--num-records", type=int, default=100)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--output", default="data/c2_result.json")
    args = parser.parse_args()
    if not Path(args.batch_file).exists():
        from src.generate_batch_c2 import generate_batch_04
        generate_batch_04(args.batch_file, args.num_records)
    spark = get_spark(args.master)
    try:
        ensure_baseline(spark, args.raw_dir, args.bronze_dir, args.silver_dir, args.rejected_dir)
        result = {"bronze": evolve_bronze(spark, args.batch_file, args.bronze_dir),
                  "silver": evolve_silver(spark, args.bronze_dir, args.silver_dir),
                  "evidence": verify_evidence(spark, args.bronze_dir, args.silver_dir)}
        write_json(args.output, result)
        print(result)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
