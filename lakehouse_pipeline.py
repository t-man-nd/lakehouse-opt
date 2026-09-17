"""Reproducible B1 verification -> B2 -> B3 -> C1 -> C2 -> C3 (Gold follows in D1)."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
from importlib.metadata import version as package_version
import json
from pathlib import Path
import time

from src.bronze import run_bronze
from src.cdc_merge import apply_cdc, generate_fixture
from src.delta_runtime import get_spark, latest_version, require_same_rows, snapshot, write_json
from src.generate_batch_c2 import generate_batch_04
from src.schema_evolution import evolve_bronze, evolve_silver, verify_evidence
from src.silver import build
from src.time_travel import audit_history, vacuum_copy_demo


def verify_inputs(raw_dir: str, checksum_file: str) -> dict:
    expected = {line.split()[1]: line.split()[0] for line in Path(checksum_file).read_text().splitlines() if line.strip()}
    result = {}
    for index in range(1, 4):
        name = f"batch_{index:02d}.json"
        path = Path(raw_dir) / name
        if not path.is_file():
            raise FileNotFoundError(f"Missing {path}. Download the TLC Parquets and run python -m src.generator first.")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != expected[name]:
            raise ValueError(f"B1 input checksum differs: {path}")
        result[name] = {"sha256": digest, "rows": len(path.read_text().splitlines())}
    return result


def run(spark, config: dict, run_dir: str) -> dict:
    start = time.monotonic()
    inputs = verify_inputs(config["raw_dir"], config["checksums"])
    root = Path(run_dir).resolve()
    root.mkdir(parents=True, exist_ok=False)  # A fresh run never overwrites downstream state.
    bronze, silver, rejected = (str(root / name) for name in ("bronze", "silver", "silver_rejected"))
    evidence = root / "evidence"
    result = {"scope": "A1-C3; D1-F1 pending", "started_at_utc": datetime.now(timezone.utc).isoformat(),
              "run_dir": str(root), "paths": {"bronze": bronze, "silver": silver, "rejected": rejected},
              "runtime": {"spark": spark.version, "delta_spark": package_version("delta-spark"),
                          "master": spark.sparkContext.master, "timezone": spark.conf.get("spark.sql.session.timeZone")},
              "b1": inputs}

    def record(name, value):
        result[name] = value
        write_json(evidence / "pipeline_progress.json", result)
        print(f"[{name.upper()}] {json.dumps(value, default=str, ensure_ascii=False)}", flush=True)

    record("b2", run_bronze(spark, config["raw_dir"], bronze))
    record("b3", build(spark, bronze, silver, rejected, manifest_path=config["manifest"]))
    baseline = latest_version(spark, silver)
    fixture_path = str(root / "cdc" / "late_updates.parquet")
    fixture = generate_fixture(spark, silver, fixture_path, seed=config["cdc_seed"],
                               updates=config["cdc_updates"], inserts=config["cdc_inserts"])
    write_json(evidence / "cdc_fixture.json", fixture)
    record("c1", apply_cdc(spark, silver, fixture_path))
    if result["c1"]["updated_count"] != config["cdc_updates"] or result["c1"]["inserted_count"] != config["cdc_inserts"]:
        raise AssertionError("Fresh C1 demo did not update and insert the requested rows")
    merge_version = latest_version(spark, silver)
    batch_file = generate_batch_04(str(root / "raw" / "batch_04.json"), config["evolution_rows"])
    bronze_before = latest_version(spark, bronze)
    record("c2", {"bronze": evolve_bronze(spark, batch_file, bronze),
                  "silver": evolve_silver(spark, bronze, silver), "evidence": verify_evidence(spark, bronze, silver)})
    versions = {"baseline": baseline, "merge": merge_version, "evolution": latest_version(spark, silver),
                "bronze_before_evolution": bronze_before, "bronze_evolution": latest_version(spark, bronze)}
    # Replays are checked without introducing extra Silver commits into C3's timeline.
    replay = {"bronze": evolve_bronze(spark, batch_file, bronze), "silver": evolve_silver(spark, bronze, silver)}
    if not all(item["replayed"] and item["before_version"] == item["after_version"] for item in replay.values()):
        raise AssertionError("Identical C2 replay should be a no-op in both layers")
    record("c2_replay", replay)
    old_bronze = snapshot(spark, bronze, bronze_before)
    require_same_rows(old_bronze, snapshot(spark, bronze).filter("_batch_id <> 'batch_04'").select(old_bronze.columns),
                      "C2 preserves every raw Bronze value")
    audit = audit_history(spark, bronze, silver, versions, fixture, str(evidence))
    record("c3_audit", {"versions": versions, "snapshot_counts": {key: value["count"] for key, value in audit["snapshots"].items()},
                        "audit_file": str(evidence / "audit.json")})
    vacuum = vacuum_copy_demo(spark, silver, str(root / "vacuum_copy"), [baseline, merge_version, versions["evolution"]])
    write_json(evidence / "vacuum.json", vacuum)
    record("c3_vacuum", {"deleted_data_file_count": len(vacuum["deleted_data_files"]),
                        "copy_old_read_failed_as_expected": vacuum["copy_old_read_failure"]["expected_missing_data_file"],
                        "original_versions_readable": list(vacuum["original_versions_after_vacuum"]),
                        "original_files_unchanged": vacuum["original_files_unchanged"],
                        "retention_check_restored": vacuum["retention_check_restored"]})
    result["elapsed_seconds"] = round(time.monotonic() - start, 3)
    result["status"] = "passed"
    write_json(evidence / "pipeline_run.json", result)
    print(f"[PASS] Through C3: {evidence / 'pipeline_run.json'}", flush=True)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/pipeline.json")
    parser.add_argument("--run-dir", help="New output directory; existing directories are refused")
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    run_dir = args.run_dir or str(Path(config["run_root"]) / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    verify_inputs(config["raw_dir"], config["checksums"])
    if Path(run_dir).exists():
        raise FileExistsError("Choose a new --run-dir; existing runs are preserved")
    spark = get_spark(config["master"])
    try:
        run(spark, config, run_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
