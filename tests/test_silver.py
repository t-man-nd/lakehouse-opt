"""B3 contract and real Spark/Delta integration checks (all data is isolated)."""

import json
from copy import deepcopy
from pathlib import Path

import pytest
from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

from src.bronze import run_bronze
from src.generator import assign_trip_ids, inject_defects, calculate_manifest
from src.silver import build, _classify, _TYPED

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    builder = (
        SparkSession.builder.master("local[2]").appName("B3Tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def fixture_rows():
    return [json.loads(line) for line in (ROOT / "docs/fixture_mini.json").read_text().splitlines()]


def write_json(path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_ansi_parsing_and_exact_four_rules(spark, tmp_path):
    base = fixture_rows()[0]
    # Deliberately put all JSON values in strings, including malformed numerics.
    cases = [
        ({"tpep_pickup_datetime": "not-a-date"}, "INVALID_PICKUP_DATETIME"),
        ({"tpep_pickup_datetime": "2025-02-30 12:00:00"}, "INVALID_PICKUP_DATETIME"),
        ({"tpep_pickup_datetime": "2025-01-01T12:00:00"}, "INVALID_PICKUP_DATETIME"),
        ({"tpep_pickup_datetime": "2025-01-01 12:00:00junk"}, "INVALID_PICKUP_DATETIME"),
        ({"PULocationID": None}, "MISSING_LOCATION"),
        ({"DOLocationID": "bad-zone"}, "MISSING_LOCATION"),
        ({"PULocationID": "2147483648"}, "MISSING_LOCATION"),
        ({"fare_amount": "0"}, "INVALID_FARE"),
        ({"fare_amount": "-5"}, "INVALID_FARE"),
        ({"fare_amount": "0.01"}, None),
        ({"tpep_dropoff_datetime": "not-a-date", "tip_amount": "bad-tip",
          "VendorID": "bad-vendor", "passenger_count": "-1", "trip_distance": "-2"}, None),
        ({"fare_amount": "bad-fare"}, None),
        ({"fare_amount": None}, None),
        ({"PULocationID": "0", "DOLocationID": "-1"}, None),
        ({"tpep_pickup_datetime": "2024-02-29 23:59:59"}, None),
        ({"tpep_pickup_datetime": "bad", "PULocationID": None, "fare_amount": "0"},
         "INVALID_PICKUP_DATETIME"),
    ]
    rows = []
    for index, (changes, _) in enumerate(cases):
        row = {k: None if v is None else str(v) for k, v in base.items()}
        row.update(changes, trip_id=f"case_{index}")
        rows.append(row)
    path = tmp_path / "strings.json"
    write_json(path, rows)
    classified = _classify(spark.read.json(str(path)))
    actual = {r.trip_id: r for r in classified.collect()}
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"
    assert spark.conf.get("spark.sql.legacy.timeParserPolicy") == "CORRECTED"
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    for index, (_, reason) in enumerate(cases):
        assert actual[f"case_{index}"].reject_reason == reason
    assert actual["case_10"][_TYPED].tpep_dropoff_datetime is None
    assert actual["case_10"][_TYPED].tip_amount is None
    assert actual["case_10"][_TYPED].VendorID is None
    assert actual["case_11"][_TYPED].fare_amount is None
    schema = classified.schema[_TYPED].dataType
    assert schema["tpep_pickup_datetime"].dataType.simpleString() == "timestamp"
    assert schema["PULocationID"].dataType.simpleString() == "int"
    assert schema["fare_amount"].dataType.simpleString() == "double"


def test_mini_bronze_quarantine_and_rebuild(spark, tmp_path):
    rows = fixture_rows()
    raw = tmp_path / "raw"
    raw.mkdir()
    for index in range(3):
        write_json(raw / f"batch_{index + 1:02d}.json", rows[index * 6:(index + 1) * 6])
    bronze_path = str(tmp_path / "bronze")
    silver_path = str(tmp_path / "silver")
    rejected_path = str(tmp_path / "silver_rejected")
    assert run_bronze(spark, str(raw), bronze_path) == {"bronze_count": 18}
    before = DeltaTable.forPath(spark, bronze_path).history(1).first()["version"]
    bronze = spark.read.format("delta").load(bronze_path)
    source = spark.read.json(str(raw / "batch_*.json"))
    assert bronze.select(source.columns).exceptAll(source).count() == 0
    assert source.exceptAll(bronze.select(source.columns)).count() == 0
    assert bronze.filter("_ingest_ts IS NULL OR _source_file = '' OR _batch_id IS NULL").count() == 0
    result = build(spark, bronze_path, silver_path, rejected_path)
    expected = json.loads((ROOT / "docs/fixture_mini_expected.json").read_text())
    assert result["bronze_count"] == expected["raw_rows"]
    assert result["silver_count"] == expected["silver"] == 8
    assert result["rejected_count"] == expected["rejected"] == 10
    assert result["reject_reason_counts"] == {k: expected[k] for k in result["reject_reason_counts"]}
    silver = spark.read.format("delta").load(silver_path)
    rejected = spark.read.format("delta").load(rejected_path)
    assert silver.select("trip_id").distinct().count() == silver.count()
    assert rejected.filter("reject_reason IS NULL OR reject_reason = ''").count() == 0
    assert rejected.filter("reject_reason = 'INVALID_PICKUP_DATETIME'").filter(
        F.col("tpep_pickup_datetime").isin("not-a-date", "2024-13-40 25:61:00")
    ).count() == 2
    # Independently check the multiset of original raw rows in quarantine.
    remaining_ids = {r.trip_id for r in silver.select("trip_id").collect()}
    expected_rejected = []
    seen = set()
    for row in rows:
        key = row["trip_id"]
        if key not in remaining_ids or key in seen:
            expected_rejected.append(row)
        seen.add(key)
    original_columns = sorted(rows[0])
    actual_raw = [r.asDict() for r in rejected.select(original_columns).collect()]
    canonical = lambda records: sorted(json.dumps(r, sort_keys=True) for r in records)
    assert canonical(actual_raw) == canonical(expected_rejected)
    previous = canonical([r.asDict() for r in silver.select("trip_id", "fare_amount").collect()])
    assert build(spark, bronze_path, silver_path, rejected_path) == result
    current = spark.read.format("delta").load(silver_path)
    assert canonical([r.asDict() for r in current.select("trip_id", "fare_amount").collect()]) == previous
    assert DeltaTable.forPath(spark, bronze_path).history(1).first()["version"] == before
    (tmp_path / "counts.json").write_text(json.dumps(result, indent=2) + "\n")


def test_controlled_3060_rows(spark, tmp_path):
    """Synthetic scale check using B1 injection; NOT the missing official batches."""
    raw = tmp_path / "raw"
    raw.mkdir()
    source = fixture_rows()[0]
    for batch in range(1, 4):
        clean = [deepcopy(source) for _ in range(1000)]
        assign_trip_ids(clean, batch)
        rows = inject_defects(
            clean, batch_no=batch, seed=42, malformed_dates=10,
            missing_pu=10, missing_do=10, invalid_fares=10, duplicates=20,
        )
        assert calculate_manifest(rows)["expected_rejected_rows"] == 60
        write_json(raw / f"batch_{batch:02d}.json", rows)
    bronze = str(tmp_path / "bronze")
    silver = str(tmp_path / "silver")
    rejected = str(tmp_path / "silver_rejected")
    assert run_bronze(spark, str(raw), bronze)["bronze_count"] == 3060
    result = build(spark, bronze, silver, rejected, manifest_path=str(ROOT / "docs/error_manifest.json"))
    assert (result["silver_count"], result["rejected_count"]) == (2880, 180)
    assert result["reject_reason_counts"] == {
        "DUPLICATE_TRIP": 60, "INVALID_PICKUP_DATETIME": 30,
        "MISSING_LOCATION": 60, "INVALID_FARE": 30,
    }
    cleaned = spark.read.format("delta").load(silver)
    quarantined = spark.read.format("delta").load(rejected)
    assert {r._batch_id: r["count"] for r in cleaned.groupBy("_batch_id").count().collect()} == {
        f"batch_{b:02d}": 960 for b in range(1, 4)
    }
    assert {r._batch_id: r["count"] for r in quarantined.groupBy("_batch_id").count().collect()} == {
        f"batch_{b:02d}": 60 for b in range(1, 4)
    }
    (tmp_path / "counts.json").write_text(json.dumps({"synthetic": True, **result}, indent=2) + "\n")


def test_rejects_missing_contract_columns(spark):
    with pytest.raises(ValueError, match="missing B1 columns"):
        _classify(spark.range(1))


def test_rejects_output_path_equal_to_bronze(spark, tmp_path):
    path = str(tmp_path / "bronze")
    with pytest.raises(ValueError, match="must be distinct"):
        build(spark, path, path, str(tmp_path / "rejected"))
