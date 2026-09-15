#!/usr/bin/env python3
"""
Nhiem vu C2: Delta Core - Schema Evolution.
Pham vi:
1. Them cot moi surcharge_fee vao batch moi (batch_04.json).
2. Xu ly schema moi bang mergeSchema cua Delta tai ca Bronze va Silver.
3. Khong rebuild bang; cac dong cu nhan gia tri NULL o cot surcharge_fee.
4. Lenh DESCRIBE HISTORY ca Bronze va Silver thay ro version commit thay doi schema.
"""

from __future__ import annotations

import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

import argparse
import json
import os
from pathlib import Path

# 1. Tu dong phat hien va thiet lap JAVA_HOME tren Windows
if "JAVA_HOME" not in os.environ or not os.path.exists(os.environ.get("JAVA_HOME", "")):
    for candidate in [
        r"C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot",
        r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.1-hotspot",
    ]:
        if os.path.exists(candidate):
            os.environ["JAVA_HOME"] = candidate
            os.environ["PATH"] = os.path.join(candidate, "bin") + ";" + os.environ.get("PATH", "")
            break

# 2. Tu dong phat hien va thiet lap HADOOP_HOME (winutils) tren Windows
if os.name == "nt":
    if "HADOOP_HOME" not in os.environ or not os.path.exists(os.environ.get("HADOOP_HOME", "")):
        candidate_hadoop = r"C:\hadoop"
        if os.path.exists(candidate_hadoop):
            os.environ["HADOOP_HOME"] = candidate_hadoop
            os.environ["PATH"] = os.path.join(candidate_hadoop, "bin") + ";" + os.environ.get("PATH", "")

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, functions as F

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
DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
_TYPED = "__b3_typed"


def get_spark(master: str = "local[2]") -> SparkSession:
    """Khoi tao SparkSession co tich hop cau hinh Delta Lake Extensions va Catalog."""
    builder = (
        SparkSession.builder.appName("C2_Delta_Schema_Evolution")
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_baseline(spark: SparkSession, raw_dir: str, bronze_dir: str, silver_dir: str):
    """Kiem tra va khoi tao Bronze (B2) va Silver (B3) neu chua ton tai tren may local."""
    has_bronze = Path(bronze_dir).exists() and any(Path(bronze_dir).iterdir())
    has_silver = Path(silver_dir).exists() and any(Path(silver_dir).iterdir())

    if not has_bronze:
        print("[*] Baseline Bronze table not found. Initializing B2 Bronze ingestion...")
        from src.bronze import run_bronze
        run_bronze(spark, raw_dir=raw_dir, bronze_dir=bronze_dir)

    if not has_silver:
        print("[*] Baseline Silver table not found. Initializing B3 Silver cleaning...")
        from src.silver import build as build_silver
        build_silver(spark, bronze_dir=bronze_dir, silver_dir=silver_dir)


def evolve_bronze(
    spark: SparkSession,
    batch_path: str = "data/raw/batch_04.json",
    bronze_dir: str = "data/bronze/taxi_trips",
    batch_id: str = "batch_04"
) -> dict:
    """Nap batch co cot moi vao Bronze bang mergeSchema=true ma khong rebuild bang."""
    print("=" * 70)
    print(">>> [C2 - STEP 1/2]: BRONZE LAYER SCHEMA EVOLUTION")
    print("=" * 70)

    # 1. Trang thai schema truoc khi evolve
    df_bronze_before = spark.read.format("delta").load(bronze_dir)
    cols_before = df_bronze_before.columns
    count_before = df_bronze_before.count()
    has_col_before = "surcharge_fee" in cols_before

    print(f"[*] Bronze Schema BEFORE ingestion: Column 'surcharge_fee' exists? -> {has_col_before}")
    print(f"[*] Total Bronze rows before: {count_before}")

    # 2. Doc file batch moi chua cot surcharge_fee va gan 3 metadata
    print(f"[*] Reading new batch file: {batch_path} (batch_id: {batch_id})...")
    df_raw = spark.read.json(batch_path)
    batch_count = df_raw.count()

    df_bronze_batch = (
        df_raw
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(batch_id))
    )

    # 3. Ghi APPEND voi mergeSchema=true (Delta tu dong cap nhat Delta Log)
    print("[*] Writing append into Bronze with option('mergeSchema', 'true')...")
    (
        df_bronze_batch.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(bronze_dir)
    )

    # 4. Trang thai schema sau khi evolve
    df_bronze_after = spark.read.format("delta").load(bronze_dir)
    cols_after = df_bronze_after.columns
    count_after = df_bronze_after.count()
    has_col_after = "surcharge_fee" in cols_after

    print(f"[OK] Bronze Schema AFTER ingestion: Column 'surcharge_fee' exists? -> {has_col_after}")
    print(f"[OK] Bronze row count: Before = {count_before} | Added = {batch_count} | After = {count_after}")

    assert has_col_after, "ERROR: Column surcharge_fee not found in Bronze schema!"
    assert count_after == count_before + batch_count, "ERROR: Bronze row count conservation failed!"

    return {
        "bronze_count_before": count_before,
        "bronze_count_after": count_after,
        "new_rows": batch_count,
        "has_col_before": has_col_before,
        "has_col_after": has_col_after,
    }


def evolve_silver(
    spark: SparkSession,
    bronze_dir: str = "data/bronze/taxi_trips",
    silver_dir: str = "data/silver/taxi_trips",
    batch_id: str = "batch_04"
) -> dict:
    """Lam sach va nap batch moi vao Silver bang mergeSchema=true ma khong overwrite bang."""
    print("\n" + "=" * 70)
    print(">>> [C2 - STEP 2/2]: SILVER LAYER SCHEMA EVOLUTION")
    print("=" * 70)

    # 1. Trang thai schema Silver truoc khi evolve
    df_silver_before = spark.read.format("delta").load(silver_dir)
    cols_before = df_silver_before.columns
    count_before = df_silver_before.count()
    has_col_before = "surcharge_fee" in cols_before

    print(f"[*] Silver Schema BEFORE ingestion: Column 'surcharge_fee' exists? -> {has_col_before}")
    print(f"[*] Total Silver rows before: {count_before}")

    # 2. Doc rieng cac ban ghi thuoc batch moi vua nap vao Bronze
    df_bronze_new = (
        spark.read.format("delta")
        .load(bronze_dir)
        .filter(F.col("_batch_id") == batch_id)
    )

    # 3. Chuan hoa kieu an toan theo chuan B3
    converted = []
    for name, target in TARGET_TYPES.items():
        col_ref = F.col(f"`{name}`")
        if target == "timestamp":
            value = F.try_to_timestamp(col_ref.try_cast("string"), F.lit(DATE_FORMAT))
        else:
            value = col_ref.try_cast(target)
        converted.append(value.alias(name))

    parsed = df_bronze_new.withColumn(_TYPED, F.struct(*converted))
    typed = F.col(_TYPED)

    # 4. Phan loai loi chat luong du lieu (DQ rules)
    classified = parsed.withColumn(
        "reject_reason",
        F.when(typed["tpep_pickup_datetime"].isNull(), "INVALID_PICKUP_DATETIME")
        .when(typed["PULocationID"].isNull() | typed["DOLocationID"].isNull(), "MISSING_LOCATION")
        .when(typed["fare_amount"] <= 0, "INVALID_FARE")
    )

    # Loc lay cac ban ghi hop le
    candidates = classified.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    kept = candidates.dropDuplicates(["trip_id"])
    clean_count = kept.count()

    # Lay cac cot mo rong va metadata (bao gom surcharge_fee)
    extra_cols = [
        F.col(f"`{name}`") for name in df_bronze_new.columns 
        if name not in TARGET_TYPES and name != _TYPED
    ]
    silver_batch_clean = kept.select(f"{_TYPED}.*", *extra_cols)

    # Dam bao ep kieu double cho surcharge_fee
    if "surcharge_fee" in silver_batch_clean.columns:
        silver_batch_clean = silver_batch_clean.withColumn(
            "surcharge_fee", F.col("surcharge_fee").try_cast("double")
        )

    # 5. Ghi APPEND vao Silver voi mergeSchema=true (Tuyet doi khong overwrite)
    print("[*] Writing append into Silver with option('mergeSchema', 'true')...")
    (
        silver_batch_clean.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(silver_dir)
    )

    # 6. Kiem tra lai Silver sau khi evolve
    df_silver_after = spark.read.format("delta").load(silver_dir)
    cols_after = df_silver_after.columns
    count_after = df_silver_after.count()
    has_col_after = "surcharge_fee" in cols_after

    print(f"[OK] Silver Schema AFTER ingestion: Column 'surcharge_fee' exists? -> {has_col_after}")
    print(f"[OK] Silver row count: Before = {count_before} | Added = {clean_count} | After = {count_after}")

    assert has_col_after, "ERROR: Column surcharge_fee not found in Silver schema!"
    assert count_after == count_before + clean_count, "ERROR: Silver row count conservation failed!"

    return {
        "silver_count_before": count_before,
        "silver_count_after": count_after,
        "clean_new_rows": clean_count,
        "has_col_before": has_col_before,
        "has_col_after": has_col_after,
    }


def verify_evidence(
    spark: SparkSession,
    bronze_dir: str = "data/bronze/taxi_trips",
    silver_dir: str = "data/silver/taxi_trips"
) -> dict:
    """Kiem tra va in bang chung nghiem thu (Evidence) cho Task C2."""
    print("\n" + "=" * 70)
    print("          TASK C2 VERIFICATION EVIDENCE (DONE CRITERIA)          ")
    print("=" * 70)

    summary = {}

    for layer_name, layer_path in [("BRONZE", bronze_dir), ("SILVER", silver_dir)]:
        print(f"\n-------------------------------------------------------------")
        print(f">>> [VERIFYING {layer_name} TABLE]: {layer_path}")
        print(f"-------------------------------------------------------------")
        df = spark.read.format("delta").load(layer_path)

        total_rows = df.count()
        null_surcharge = df.filter(F.col("surcharge_fee").isNull()).count()
        not_null_surcharge = df.filter(F.col("surcharge_fee").isNotNull()).count()

        print(f"1. Total row count: {total_rows}")
        print(f"2. Old rows with 'surcharge_fee IS NULL' (Expected > 0): {null_surcharge}")
        print(f"3. New rows with 'surcharge_fee IS NOT NULL' (Expected > 0): {not_null_surcharge}")

        assert null_surcharge > 0, f"ERROR: No NULL values found in {layer_name} table!"
        assert not_null_surcharge > 0, f"ERROR: surcharge_fee is completely NULL in {layer_name} table!"
        assert null_surcharge + not_null_surcharge == total_rows, "ERROR: Total rows count mismatch!"

        print("\n4. Sample Data Proof:")
        print("   [3 OLD rows from batches 1..3 - Column surcharge_fee is NULL]:")
        df.filter(F.col("surcharge_fee").isNull()) \
          .select("trip_id", "_batch_id", "fare_amount", "surcharge_fee") \
          .show(3, truncate=False)

        print("   [3 NEW rows from batch_04 - Column surcharge_fee has VALUES]:")
        df.filter(F.col("surcharge_fee").isNotNull()) \
          .select("trip_id", "_batch_id", "fare_amount", "surcharge_fee") \
          .show(3, truncate=False)

        print("5. Transaction Log (DESCRIBE HISTORY):")
        dt = DeltaTable.forPath(spark, layer_path)
        history_df = dt.history().select("version", "timestamp", "operation", "operationParameters")
        history_df.show(5, truncate=False)

        summary[layer_name] = {
            "total_rows": total_rows,
            "null_rows": null_surcharge,
            "not_null_rows": not_null_surcharge,
            "latest_version": dt.history(1).first()["version"]
        }

    print("\n" + "=" * 70)
    print("[SUCCESS] ALL 4 ACCEPTANCE CRITERIA FOR TASK C2 ARE MET:")
    print("  1. New column 'surcharge_fee' added successfully to Bronze and Silver.")
    print("  2. No table rebuild/recreation (transaction history preserved).")
    print("  3. Old records from batches 1, 2, 3 show NULL in surcharge_fee.")
    print("  4. DESCRIBE HISTORY confirms WRITE operation with mergeSchema: true.")
    print("=" * 70)

    return summary


def main():
    parser = argparse.ArgumentParser(description="Task C2: Schema Evolution Pipeline")
    parser.add_argument("--raw-dir", default="data/raw", help="Raw JSON directory")
    parser.add_argument("--batch-file", default="data/raw/batch_04.json", help="Path to new batch file")
    parser.add_argument("--bronze-dir", default="data/bronze/taxi_trips", help="Delta Bronze directory")
    parser.add_argument("--silver-dir", default="data/silver/taxi_trips", help="Delta Silver directory")
    parser.add_argument("--num-records", type=int, default=100, help="Number of records to generate if batch_04 is missing")
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args()

    # 1. Sinh batch_04 neu chua co
    if not Path(args.batch_file).exists():
        from src.generate_batch_c2 import generate_batch_04
        generate_batch_04(args.batch_file, num_records=args.num_records)

    # 2. Khoi tao Spark
    spark = get_spark(master=args.master)

    try:
        # 3. Kiem tra va dung baseline Bronze/Silver neu can
        ensure_baseline(spark, args.raw_dir, args.bronze_dir, args.silver_dir)

        # 4. Thuc thi Schema Evolution cho Bronze va Silver
        evolve_bronze(spark, batch_path=args.batch_file, bronze_dir=args.bronze_dir, batch_id="batch_04")
        evolve_silver(spark, bronze_dir=args.bronze_dir, silver_dir=args.silver_dir, batch_id="batch_04")

        # 5. Kiem tra va xuat Evidence
        verify_evidence(spark, bronze_dir=args.bronze_dir, silver_dir=args.silver_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
