"""TLC-v2 data-quality rules on synthetic TLC-shaped files (plain Spark, no Delta)."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.dq_rules import TYPED, classify_trips, harmonize_bronze_columns, to_rejected_rows, to_silver_rows
from tests.tlc_like import build_dataset

MONTHS = ["2024-01", "2024-02", "2025-01"]


@pytest.fixture(scope="module")
def dataset(tmp_path_factory):
    root = tmp_path_factory.mktemp("tlc_like")
    info = build_dataset(root, MONTHS, clean=120, seed=7)
    return root, info


def bronze_like(spark, root: Path, month: str):
    df = spark.read.parquet(str(root / "data" / "raw" / "tlc" / f"yellow_tripdata_{month}.parquet"))
    df, _ = harmonize_bronze_columns(df)
    return (df.withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_source_file", F.lit(f"yellow_tripdata_{month}.parquet"))
            .withColumn("_batch_id", F.lit(f"yellow_{month}_test"))
            .withColumn("_source_month", F.lit(month))
            .withColumn("_file_sha256", F.lit("0" * 64)))


def counts(df, column):
    return {r[column]: r["count"] for r in df.groupBy(column).count().collect()}


def test_harmonize_fixes_real_cross_year_drift(spark, dataset):
    root, _ = dataset
    raw_2024 = spark.read.parquet(str(root / "data/raw/tlc/yellow_tripdata_2024-01.parquet"))
    assert "airport_fee" in raw_2024.columns
    assert raw_2024.schema["passenger_count"].dataType.simpleString() == "double"
    assert raw_2024.schema["tpep_pickup_datetime"].dataType.simpleString() in ("timestamp_ntz", "timestamp")
    harmonized, changes = harmonize_bronze_columns(raw_2024)
    assert "Airport_fee" in harmonized.columns and "airport_fee" not in harmonized.columns
    assert harmonized.schema["passenger_count"].dataType.simpleString() == "bigint"
    assert harmonized.schema["VendorID"].dataType.simpleString() == "bigint"
    assert harmonized.schema["tpep_pickup_datetime"].dataType.simpleString() == "timestamp"
    assert "airport_fee->Airport_fee" in changes["renamed"]
    assert harmonized.count() == raw_2024.count()           # widening never drops rows
    assert "cbd_congestion_fee" not in harmonized.columns   # absent columns are not invented


@pytest.mark.parametrize("month", MONTHS)
def test_planted_rejects_and_flags_are_found_exactly(spark, dataset, month):
    root, info = dataset
    expected = info["expected"][month]
    classified = classify_trips(bronze_like(spark, root, month)).cache()
    rejected = classified.filter("reject_reason IS NOT NULL")
    accepted = classified.filter("reject_reason IS NULL")

    assert classified.count() == expected["rows"]
    assert counts(rejected, "reject_reason") == expected["reject_reasons"]
    flags = counts(accepted.select(F.explode("dq_flags").alias("flag")), "flag")
    assert flags == expected["flags"]
    details = counts(rejected.filter("reject_reason = 'INVALID_FARE'"), "reject_detail")
    assert details == {"REVERSAL_OF_ACCEPTED_TRIP": 1, "ZERO_FARE": 1}
    # Conservation inside one batch: every row lands in exactly one place.
    assert accepted.count() + rejected.count() == expected["rows"]
    assert accepted.select("trip_key").distinct().count() == accepted.count()
    classified.unpersist()


def test_trip_key_ignores_money_but_not_identity(spark):
    base = {"VendorID": 2, "tpep_pickup_datetime": "2025-01-10 08:00:00",
            "tpep_dropoff_datetime": "2025-01-10 08:20:00", "PULocationID": 161, "DOLocationID": 230,
            "fare_amount": 20.0, "tip_amount": 4.0, "_source_month": "2025-01", "_source_file": "f"}
    rows = [dict(base), {**base, "fare_amount": 25.0, "tip_amount": 1.0},
            {**base, "tpep_pickup_datetime": "2025-01-10 08:00:01"}]
    df = spark.createDataFrame(rows)
    rows_out = classify_trips(df).select(
        "trip_key", F.col(f"{TYPED}.fare_amount").alias("fare"),
        F.date_format(F.col(f"{TYPED}.tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss").alias("pickup"),
    ).collect()   # format in Spark (session UTC), not in Python (machine time zone)
    keys = [r["trip_key"] for r in rows_out]
    by_fare = {(r["fare"], r["pickup"]): r["trip_key"] for r in rows_out}
    assert by_fare[(20.0, "2025-01-10 08:00:00")] == by_fare[(25.0, "2025-01-10 08:00:00")]
    assert by_fare[(20.0, "2025-01-10 08:00:01")] != by_fare[(20.0, "2025-01-10 08:00:00")]
    assert len(keys) == 3


@pytest.mark.parametrize("bad_value", ["not-a-date", "2025-02-30 12:00:00", "2025-01-10T08:00:00", ""])
def test_unparseable_pickup_is_quarantined_not_crashing(spark, bad_value):
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"
    row = {"VendorID": "2", "tpep_pickup_datetime": bad_value, "tpep_dropoff_datetime": "2025-01-10 08:20:00",
           "PULocationID": "161", "DOLocationID": "230", "fare_amount": "20.0",
           "_source_month": "2025-01", "_source_file": "strings.json"}
    result = classify_trips(spark.createDataFrame([row])).first()
    assert result["reject_reason"] == "INVALID_PICKUP_DATETIME"


def test_bad_numeric_strings_become_null_under_ansi(spark):
    row = {"VendorID": "bad-vendor", "tpep_pickup_datetime": "2025-01-10 08:00:00",
           "tpep_dropoff_datetime": "2025-01-10 08:20:00", "PULocationID": "2147483648", "DOLocationID": "230",
           "fare_amount": "bad-fare", "_source_month": "2025-01", "_source_file": "strings.json"}
    result = classify_trips(spark.createDataFrame([row])).first()
    assert result[TYPED]["VendorID"] is None
    assert result["reject_reason"] == "MISSING_LOCATION"   # int overflow -> NULL -> location missing


def test_classification_is_deterministic(spark, dataset):
    root, _ = dataset
    first = classify_trips(bronze_like(spark, root, "2024-02"))
    second = classify_trips(bronze_like(spark, root, "2024-02").orderBy(F.rand(3)))
    project = lambda df: sorted((r["row_hash"], r["reject_reason"] or "") for r in df.select("row_hash", "reject_reason").collect())
    assert project(first) == project(second)


def test_silver_rows_keep_each_batch_schema(spark, dataset):
    root, _ = dataset
    silver_2024 = to_silver_rows(classify_trips(bronze_like(spark, root, "2024-01")))
    silver_2025 = to_silver_rows(classify_trips(bronze_like(spark, root, "2025-01")))
    assert "cbd_congestion_fee" not in silver_2024.columns
    assert "cbd_congestion_fee" in silver_2025.columns
    for column in ("trip_key", "pickup_date", "pickup_hour", "pickup_month", "dq_flags", "row_hash",
                   "_batch_id", "_source_file", "_ingest_ts", "_silver_ts", "is_late_arrival"):
        assert column in silver_2025.columns
    assert silver_2025.schema["PULocationID"].dataType.simpleString() == "int"
    late = silver_2025.filter("is_late_arrival").select("pickup_month").distinct().collect()
    assert [r["pickup_month"] for r in late] == ["2024-12"]   # late rows go to their true month


def test_rejected_rows_keep_original_bronze_values(spark, dataset):
    root, _ = dataset
    bronze = bronze_like(spark, root, "2025-01")
    rejected = to_rejected_rows(classify_trips(bronze), bronze.columns)
    assert set(bronze.columns).issubset(rejected.columns)
    assert rejected.filter("reject_reason IS NULL").count() == 0
    reversal = rejected.filter("reject_detail = 'REVERSAL_OF_ACCEPTED_TRIP'").first()
    assert reversal["fare_amount"] < 0


def test_missing_required_columns_fail_fast(spark):
    with pytest.raises(ValueError, match="missing required columns"):
        classify_trips(spark.range(1))


TOTAL_BASE = {"VendorID": 2, "tpep_pickup_datetime": "2025-02-10 08:00:00",
              "tpep_dropoff_datetime": "2025-02-10 08:20:00", "PULocationID": 161, "DOLocationID": 230,
              "fare_amount": 10.0, "extra": 1.0, "mta_tax": 0.5, "tip_amount": 2.0, "tolls_amount": 0.0,
              "improvement_surcharge": 1.0, "_source_month": "2025-02", "_source_file": "f.parquet"}
# certain components sum to 14.50; congestion 2.50 + CBD 0.75 are the uncertain group.
TOTAL_SCHEMA = ("VendorID int, tpep_pickup_datetime string, tpep_dropoff_datetime string, "
                "PULocationID int, DOLocationID int, fare_amount double, extra double, mta_tax double, "
                "tip_amount double, tolls_amount double, improvement_surcharge double, "
                "congestion_surcharge double, cbd_congestion_fee double, total_amount double, "
                "_source_month string, _source_file string")


def total_row(**overrides):
    """One row in a fixed schema (an all-NULL column has no type Spark can infer)."""
    merged = {**TOTAL_BASE, "congestion_surcharge": None, "cbd_congestion_fee": None, **overrides}
    return [tuple(merged.get(f.split()[0]) for f in TOTAL_SCHEMA.split(", "))]


@pytest.mark.parametrize("label,row,flagged", [
    ("surcharges itemised and included", {"congestion_surcharge": 2.5, "cbd_congestion_fee": 0.75, "total_amount": 17.75}, False),
    ("surcharges itemised but excluded from total", {"congestion_surcharge": 2.5, "cbd_congestion_fee": 0.75, "total_amount": 14.5}, False),
    ("only the CBD fee excluded", {"congestion_surcharge": 2.5, "cbd_congestion_fee": 0.75, "total_amount": 17.0}, False),
    ("charged but not itemised (NULL block)", {"congestion_surcharge": None, "cbd_congestion_fee": None, "total_amount": 17.0}, False),
    ("total below every component", {"congestion_surcharge": 2.5, "cbd_congestion_fee": 0.75, "total_amount": 10.25}, True),
    ("total above the widest sum", {"congestion_surcharge": 2.5, "cbd_congestion_fee": 0.75, "total_amount": 25.0}, True),
])
def test_total_mismatch_tolerates_the_inconsistent_congestion_surcharge(spark, label, row, flagged):
    """TLC sometimes excludes the congestion surcharges from total_amount and sometimes charges
    them without itemising them (docs/evidence/findings.md). Only genuine gaps should flag."""
    df = spark.createDataFrame(total_row(**row), TOTAL_SCHEMA)
    result = classify_trips(df).first()
    assert result["reject_reason"] is None
    assert ("TOTAL_MISMATCH" in result["dq_flags"]) is flagged, label


def test_total_mismatch_still_flags_2024_rows_without_a_cbd_column(spark):
    row = {k: v for k, v in TOTAL_BASE.items()}
    row.update({"_source_month": "2024-02", "tpep_pickup_datetime": "2024-02-10 08:00:00",
                "tpep_dropoff_datetime": "2024-02-10 08:20:00", "congestion_surcharge": 2.5})
    schema = TOTAL_SCHEMA.replace("cbd_congestion_fee double, ", "")   # 2024 files have no CBD column
    build = lambda total: spark.createDataFrame(
        [tuple({**row, "total_amount": total}.get(f.split()[0]) for f in schema.split(", "))], schema)
    ok = classify_trips(build(17.0)).first()
    assert "TOTAL_MISMATCH" not in ok["dq_flags"]          # 14.50 + 2.50, surcharge included
    bad = classify_trips(build(30.0)).first()
    assert "TOTAL_MISMATCH" in bad["dq_flags"]             # far above any plausible sum
