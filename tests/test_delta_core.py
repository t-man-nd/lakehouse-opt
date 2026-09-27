"""C1/C2 regression cases; the official-data runner supplies the full C3 rehearsal."""

import json
import shutil
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.bronze import run_bronze
from src.cdc_merge import apply_cdc, generate_fixture
from src.delta_runtime import get_spark, latest_version, snapshot
from src.generate_batch_c2 import generate_batch_04
from src.schema_evolution import evolve_bronze, evolve_silver, verify_evidence
from src.silver import build
from src.time_travel import FINGERPRINT_ALGORITHM, fingerprint, vacuum_copy_demo

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def spark():
    session = get_spark(app_name="DeltaCoreTests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def baseline(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("core_baseline")
    raw = root / "raw"
    raw.mkdir()
    lines = (ROOT / "docs/fixture_mini.json").read_text().splitlines()
    for index in range(3):
        (raw / f"batch_{index + 1:02d}.json").write_text("\n".join(lines[index * 6:(index + 1) * 6]) + "\n")
    run_bronze(spark, str(raw), str(root / "bronze"))
    result = build(spark, str(root / "bronze"), str(root / "silver"), str(root / "rejected"))
    assert result["silver_count"] == 8
    return root


@pytest.fixture
def tables(baseline, tmp_path):
    for name in ("bronze", "silver"):
        shutil.copytree(baseline / name, tmp_path / name)
    return str(tmp_path / "bronze"), str(tmp_path / "silver")


def test_cdc_update_insert_and_absolute_value_replay(spark, tables, tmp_path):
    _, silver = tables
    cdc = str(tmp_path / "cdc.parquet")
    fixture = generate_fixture(spark, silver, cdc, updates=2, inserts=1)
    result = apply_cdc(spark, silver, cdc)
    assert (result["before_count"], result["matched_count"], result["updated_count"], result["inserted_count"], result["after_count"]) == (8, 2, 2, 1, 9)
    actual = snapshot(spark, silver)
    for change in fixture["updates"]:
        before = snapshot(spark, silver, 0).filter(F.col("trip_id") == change["trip_id"]).first().asDict()
        after = actual.filter(F.col("trip_id") == change["trip_id"]).first().asDict()
        assert after == {**before, **change["after"]}
    signature = fingerprint(actual)
    replay = apply_cdc(spark, silver, cdc)
    assert (replay["matched_count"], replay["updated_count"], replay["inserted_count"], replay["after_count"]) == (3, 0, 0, 9)
    assert fingerprint(snapshot(spark, silver)) == signature


@pytest.mark.parametrize("defect,match", [("missing", "every target column"), ("duplicate", "Duplicate trip_id"), ("fare", "Invalid CDC fixture")])
def test_invalid_cdc_does_not_commit(spark, tables, tmp_path, defect, match):
    _, silver = tables
    fixture_path = str(tmp_path / "cdc.parquet")
    generate_fixture(spark, silver, fixture_path, updates=1, inserts=1)
    source = spark.read.parquet(fixture_path)
    if defect == "missing":
        source = source.drop("PULocationID")
    elif defect == "duplicate":
        source = source.unionByName(source.limit(1))
    else:
        source = source.withColumn("fare_amount", F.lit(-1.0))
    bad_path = str(tmp_path / "bad.parquet")
    source.write.parquet(bad_path)
    before = fingerprint(snapshot(spark, silver))
    with pytest.raises(ValueError, match=match):
        apply_cdc(spark, silver, bad_path)
    assert latest_version(spark, silver) == 0
    assert fingerprint(snapshot(spark, silver)) == before


def test_evolution_resume_replay_and_changed_batch_rejected(spark, tables, tmp_path):
    bronze, silver = tables
    batch = generate_batch_04(str(tmp_path / "batch_04.json"), 6)
    first = evolve_bronze(spark, batch, bronze)
    assert first["new_rows"] == 6
    assert evolve_bronze(spark, batch, bronze)["replayed"]
    assert latest_version(spark, bronze) == first["after_version"]
    assert evolve_silver(spark, bronze, silver)["clean_new_rows"] == 6
    version = latest_version(spark, silver)
    assert evolve_silver(spark, bronze, silver)["replayed"]
    assert latest_version(spark, silver) == version
    proof = verify_evidence(spark, bronze, silver)
    assert (proof["BRONZE"]["null_rows"], proof["BRONZE"]["not_null_rows"]) == (18, 6)
    assert (proof["SILVER"]["null_rows"], proof["SILVER"]["not_null_rows"]) == (8, 6)
    rows = [json.loads(line) for line in Path(batch).read_text().splitlines()]
    rows[0]["fare_amount"] += 5
    Path(batch).write_text("".join(json.dumps(row) + "\n" for row in rows))
    with pytest.raises(ValueError, match="changed data"):
        evolve_bronze(spark, batch, bronze)
    assert latest_version(spark, bronze) == first["after_version"]


@pytest.mark.parametrize("defect,match", [("date", "violates B1"), ("fee", "finite number"), ("duplicate", "Duplicate trip_id")])
def test_invalid_evolution_fixture_does_not_append(spark, tables, tmp_path, defect, match):
    bronze, _ = tables
    batch = generate_batch_04(str(tmp_path / "batch.json"), 2)
    rows = [json.loads(line) for line in Path(batch).read_text().splitlines()]
    if defect == "date":
        rows[0]["tpep_pickup_datetime"] = "not-a-date"
    elif defect == "fee":
        rows[0]["surcharge_fee"] = "bad-number"
    else:
        rows.append(rows[0])
    Path(batch).write_text("".join(json.dumps(row) + "\n" for row in rows))
    with pytest.raises(ValueError, match=match):
        evolve_bronze(spark, batch, bronze)
    assert latest_version(spark, bronze) == 2


def test_vacuum_rejects_overlapping_copy_paths(tmp_path):
    source = tmp_path / "silver"
    for destination in (source, source / "nested", tmp_path):
        with pytest.raises(ValueError, match="separate"):
            vacuum_copy_demo(None, str(source), str(destination), [0, 1])


def test_c2_generator_timestamps_cross_hour(tmp_path):
    path = generate_batch_04(str(tmp_path / "batch.json"), 100)
    for row in map(json.loads, Path(path).read_text().splitlines()):
        assert (datetime.fromisoformat(row["tpep_dropoff_datetime"]) - datetime.fromisoformat(row["tpep_pickup_datetime"])).total_seconds() == 720


def test_fingerprint_is_order_partition_and_timezone_invariant(spark):
    rows = [(i, None if i % 3 else "Địa điểm", datetime(2025, 1, 10, 12, 30, 1, 123456), i / 10)
            for i in range(16)]
    frame = spark.createDataFrame(rows, "id long, label string, at timestamp, amount double")
    expected = fingerprint(frame)
    assert expected["fingerprint_algorithm"] == FINGERPRINT_ALGORITHM
    assert expected["count"] == len(rows)
    assert len(expected["schema_sha256"]) == len(expected["rows_sha256"]) == 64
    old_timezone = spark.conf.get("spark.sql.session.timeZone")
    old_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    try:
        spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        spark.conf.set("spark.sql.shuffle.partitions", "3")
        assert fingerprint(frame.orderBy(F.desc("id")).repartition(4)) == expected
    finally:
        spark.conf.set("spark.sql.session.timeZone", old_timezone)
        spark.conf.set("spark.sql.shuffle.partitions", old_partitions)


def test_fingerprint_preserves_duplicate_multiplicity_nulls_values_and_schema(spark):
    schema = "id long, label string"
    expected = fingerprint(spark.createDataFrame([(1, None), (1, None), (2, "")], schema))
    for rows in ([(1, None), (2, ""), (2, "")],  # Same distinct set and count; different multiplicities.
                 [(1, ""), (1, ""), (2, "")],  # Nulls must not vanish from row serialization.
                 [(1, None), (1, None), (2, "changed")]):
        actual = fingerprint(spark.createDataFrame(rows, schema))
        assert actual["count"] == expected["count"]
        assert actual["schema_sha256"] == expected["schema_sha256"]
        assert actual["rows_sha256"] != expected["rows_sha256"]
    empty = fingerprint(spark.createDataFrame([], schema))
    assert empty["count"] == 0
    assert empty == fingerprint(spark.createDataFrame([], schema).repartition(3))
    changed_schema = fingerprint(spark.createDataFrame([], "id string, label string"))
    assert empty["schema_sha256"] != changed_schema["schema_sha256"]


def test_fingerprint_collects_only_bounded_bucket_summaries(spark, monkeypatch):
    from pyspark.core.rdd import RDD
    from pyspark.sql.classic.dataframe import DataFrame

    real_collect = RDD.collect
    observed = []

    def summaries_only(rdd):
        rows = real_collect(rdd)
        assert len(rows) <= 256
        assert all(len(row) == 3 and len(row[0]) == 2 and len(row[2]) == 64 for row in rows)
        observed.append(sum(row[1] for row in rows))
        return rows

    def forbid_frame_collect(*args, **kwargs):
        raise AssertionError("Fingerprint must not collect full table rows to the driver")

    monkeypatch.setattr(DataFrame, "collect", forbid_frame_collect)
    monkeypatch.setattr(RDD, "collect", summaries_only)
    result = fingerprint(spark.range(2000).withColumn("nullable", F.lit(None).cast("string")))
    assert result["count"] == 2000
    assert observed == [2000]
