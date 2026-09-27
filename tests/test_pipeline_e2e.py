"""End-to-end run of the TLC lane on synthetic TLC-shaped files (needs Delta Lake).

Covers the acceptance criteria of B2, B3, C1, C2, C3, D1, D2, E1 and E2 on a
small dataset with exactly known answers. Skipped automatically when the Delta
JARs cannot be loaded.
"""

from __future__ import annotations

import json

import pytest
from pyspark.sql import functions as F

from src import gold, reference_data, time_travel, tlc_bronze, tlc_profile, tlc_silver
from src.config import get_paths
from tests.tlc_like import EXPECTED_REJECTS, build_dataset

pytestmark = pytest.mark.delta
MONTHS = ["2024-01", "2024-02", "2025-01", "2025-02"]
N_UPDATES, N_INSERTS = 6, 2


@pytest.fixture(scope="module")
def env(spark, tmp_path_factory):
    from tests.conftest import _STATE

    if not _STATE["delta"]:
        pytest.skip(_STATE["reason"])
    root = tmp_path_factory.mktemp("e2e")
    info = build_dataset(root, MONTHS, clean=150, seed=11)
    paths = get_paths(root=root, lake=root / "data" / "lakehouse")
    results = {
        "reference": reference_data.run(spark, paths),
        "bronze": tlc_bronze.run(spark, MONTHS, paths=paths),
        "profile": tlc_profile.run(spark, paths),
        "silver": tlc_silver.build(spark, paths),
    }
    results["cdc"] = tlc_silver.apply_cdc(spark, paths, n_updates=N_UPDATES, n_inserts=N_INSERTS)
    results["evolution"] = tlc_silver.apply_evolution(spark, paths)
    results["time_travel"] = time_travel.run(spark, paths)
    results["gold"] = gold.run(spark, paths, MONTHS)
    return {"root": root, "paths": paths, "info": info, "results": results}


def test_b2_bronze_is_complete_and_tagged(spark, env):
    bronze = env["results"]["bronze"]
    manifest = env["info"]["manifest"]
    assert bronze["bronze_count"] == sum(m["rows"] for m in manifest.values())
    assert len(bronze["ingested"]) == len(MONTHS) and bronze["null_metadata_rows"] == 0
    for month, meta in manifest.items():
        assert bronze["rows_per_batch"][tlc_bronze.make_batch_id(month, meta["sha256"])] == meta["rows"]


def test_b3_silver_reconciles_exactly(spark, env):
    silver = env["results"]["silver"]
    expected = env["info"]["expected"]
    assert silver["conservation_ok"], silver["conservation"]
    assert silver["reject_reason_counts"] == {k: v * len(MONTHS) for k, v in EXPECTED_REJECTS.items()}
    accepted = sum(e["accepted"] for e in expected.values())
    noop = sum(e["noop"] for e in expected.values())
    assert silver["silver_count"] == accepted - noop
    assert silver["totals"]["expected_noop"] == noop                      # cross-file resubmissions
    assert silver["late_arrivals_in_silver"] == 2 * len(MONTHS) - noop


def test_c1_cdc_merge_updates_and_inserts_in_one_command(spark, env):
    cdc = env["results"]["cdc"]
    assert cdc["checks_ok"], cdc["checks"]
    assert cdc["merge_metrics"] == {"inserted": N_INSERTS, "updated": N_UPDATES}
    assert cdc["count_after"] == cdc["count_before"] + N_INSERTS
    history = spark.sql(f"DESCRIBE HISTORY delta.`{env['paths'].silver_trips}`")
    op = history.filter(F.col("version") == cdc["silver_version_after"]).first()
    assert op["operation"] == "MERGE"
    changed = [s for s in cdc["samples"] if s["before"]]
    assert changed and all(s["after"]["tip_amount"] == pytest.approx(s["before"]["tip_amount"] + 2.0) for s in changed)
    again = tlc_silver.apply_cdc(spark, env["paths"], n_updates=N_UPDATES, n_inserts=N_INSERTS)
    assert again["already_applied"]


def test_c2_schema_evolved_without_rebuild(spark, env):
    evo = env["results"]["evolution"]
    assert evo["status"] == "SCHEMA_EVOLVED"
    for table in ("bronze", "silver"):
        assert evo[table]["evolution_event"]["version"] > 0
        assert evo[table]["column_absent_before"] and evo[table]["column_present_after"]
        assert evo[table]["not_a_rebuild"]
    assert evo["silver"]["operation"] == "MERGE"
    assert evo["old_rows_null"]


def test_c3_time_travel_history_and_vacuum_copy(spark, env):
    tt = env["results"]["time_travel"]
    assert tt["old_versions_queried"] >= 2
    assert tt["merge_commits"] >= len(MONTHS)
    assert tt["vacuum_demo"]["real_table_oldest_version_rows"] > 0
    assert tt["vacuum_demo"]["broken_versions"], "the CDC MERGE rewrote files, so some old version must break"
    assert (env["root"] / "docs/evidence/delta_log_annotated.md").exists()
    assert (env["root"] / "docs/evidence/silver_history.json").exists()


def test_d1_gold_matches_manual_sql_and_rebuilds_identically(spark, env):
    first = env["results"]["gold"]
    assert first["verification"]["all_match"]
    assert {r["month_of_year"] for r in first["cbd_yoy"]} == {1, 2}
    second = gold.run(spark, env["paths"], MONTHS)
    assert second["gold_trips"] == first["gold_trips"]
    assert second["zone_hourly_rows"] == first["zone_hourly_rows"]


def test_e2_rerun_is_idempotent(spark, env):
    before = env["results"]["silver"]
    bronze_again = tlc_bronze.run(spark, MONTHS, paths=env["paths"])
    assert bronze_again["ingested"] == [] and len(bronze_again["skipped"]) == len(MONTHS)
    silver_again = tlc_silver.build(spark, env["paths"])
    assert silver_again["processed_this_run"] == []
    assert silver_again["silver_count"] == before["silver_count"] + N_INSERTS   # only the CDC inserts added
    assert silver_again["rejected_count"] == before["rejected_count"]


def test_d2_benchmark_runs_and_results_agree(spark, env):
    import optimization_benchmark

    result = optimization_benchmark.run(spark, env["paths"], rounds=3, baseline_files=8, target_file_mb=1)
    assert all(result["summary"]["results_identical_across_variants"].values())
    assert set(result["variants"]) == set(optimization_benchmark.VARIANTS)
    report = json.loads((env["root"] / "docs/benchmark/benchmark_results.json").read_text())
    assert all(r["files_scanned"] >= 0 for r in report["runs"])


def test_empty_bronze_commit_is_not_taken_as_the_batch(spark, env, tmp_path):
    """Regression: an empty append (failed feed) must not shadow the real append of the same batch."""
    from pyspark.sql import functions as F

    path = str(tmp_path / "bronze_probe")
    real = spark.read.format("delta").load(env["paths"].bronze_trips).limit(3)
    empty = real.limit(0)
    for df in (empty, real):
        tlc_bronze.append_batch(spark, df.drop("_ingest_ts", "_source_file", "_batch_id", "_source_month", "_file_sha256"),
                                path, "retry_batch", "2025-02", "probe", "0" * 64, None)
    version = tlc_bronze.find_batch_versions(spark, path)["retry_batch"]
    rows = spark.read.format("delta").option("versionAsOf", version).load(path).filter(F.col("_batch_id") == "retry_batch")
    assert rows.count() == 3
