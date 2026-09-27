"""E1: one command verifies B1, runs B2-C3, rebuilds Gold and benchmarks Silver."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from importlib.metadata import version as package_version
import json
from pathlib import Path
import shutil
import time

from src import bronze, gold, silver
from src.cdc_merge import generate_fixture
from src.delta_runtime import get_spark, latest_version, require_same_rows, snapshot, write_json
from src.generate_batch_c2 import generate_batch_04
from src.schema_evolution import evolve_bronze, evolve_silver
from src.time_travel import audit_history, stream_file_signature, vacuum_copy_demo


def verify_inputs(raw_dir: str, checksum_file: str) -> dict:
    expected = {line.split()[1]: line.split()[0] for line in Path(checksum_file).read_text().splitlines() if line.strip()}
    result = {}
    for index in range(1, 4):
        name = f"batch_{index:02d}.json"
        path = Path(raw_dir) / name
        if not path.is_file():
            raise FileNotFoundError(f"Missing {path}. Download the TLC Parquets and run python -m src.generator first.")
        digest, rows = stream_file_signature(path, count_lines=True)
        if digest != expected[name]:
            raise ValueError(f"B1 input checksum differs: {path}")
        result[name] = {"sha256": digest, "rows": rows, "path": str(path.resolve())}
    return result


def _sampling_provenance(config: dict) -> dict | None:
    """Verify optional sampling metadata and keep only compact strata totals here."""
    if not config.get("sampling_manifest"):
        return None
    path = Path(config["sampling_manifest"])
    expected = {line.split()[1]: line.split()[0] for line in Path(config["checksums"]).read_text().splitlines() if line.strip()}
    digest = stream_file_signature(path)[0]
    if digest != expected.get(path.name):
        raise ValueError("Sampling manifest checksum differs or is missing")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    batches = []
    for batch in manifest.get("batches", []):
        sampling = batch.get("sampling")
        if sampling is None:
            continue
        strata = sampling.get("strata", [])
        batches.append({"batch": batch["batch"], "source": batch.get("source"),
                        "sampling": {key: value for key, value in sampling.items() if key != "strata"},
                        "strata_summary": {"eligible_strata": len(strata),
                                           "sampled_strata": sum(item["selected_rows"] > 0 for item in strata),
                                           "selected_rows": sum(item["selected_rows"] for item in strata)}})
    if not batches:
        raise ValueError("Sampling manifest must contain batch sampling provenance")
    return {"manifest_path": str(path.resolve()), "sha256": digest, "batches": batches}


def _publication_paths(config: dict, run_dir: Path, output_path: str | None,
                       config_path: str | None = None) -> dict[str, Path]:
    """Preflight externally published artifacts before creating any run output.

    Internal evidence paths are fixed by the runner. User-configurable summary
    paths must stay outside input trees and this run's generated tables/evidence.
    Resolving symlinks catches aliases of both protected paths and other outputs.
    """
    paths = {
        "manifest": Path(output_path or config.get("manifest_output", "docs/pipeline_run.json")).resolve(),
        "gold_summary": Path(config.get("gold_summary_output", "docs/d1_gold_run.json")).resolve(),
        "benchmark_summary": Path(config.get("benchmark_summary_output", "docs/d2_benchmark_results.json")).resolve(),
        "benchmark_report": Path(config.get("benchmark_report_output", "docs/D2_PERFORMANCE.md")).resolve(),
    }
    protected_trees = [Path(config["raw_dir"]).resolve(), run_dir.resolve()]
    protected_files = [Path(config[key]).resolve() for key in ("checksums", "manifest", "sampling_manifest") if config.get(key)]
    if config_path is not None:
        protected_files.append(Path(config_path).resolve())
    for name, path in paths.items():
        if any(path == tree or tree in path.parents or path in tree.parents for tree in protected_trees):
            raise ValueError(f"Published {name} must not overlap raw inputs or the run directory: {path}")
        if any(path == source or path in source.parents or source in path.parents for source in protected_files):
            raise ValueError(f"Published {name} must not overlap an input metadata/config file: {path}")
        if path.is_dir():
            raise ValueError(f"Published {name} must be a file path: {path}")
    values = list(paths.values())
    if any(left == right or left in right.parents or right in left.parents
           for index, left in enumerate(values) for right in values[index + 1:]):
        raise ValueError("Published artifact paths must be distinct and non-overlapping")
    return paths


def run(spark, config: dict, run_dir: str, *, output_path: str | None = None,
        config_path: str | None = None) -> dict:
    """Run in a NEW directory; a second invocation never resets earlier tables.

    Raw Bronze ingestion itself is deliberately append-only, not idempotent.
    Complete reruns use independent directories. The latest manifest points to
    all stage evidence, while each run keeps its own immutable output paths.
    """
    start = time.monotonic()
    root = Path(run_dir).resolve()
    if root.exists():
        raise FileExistsError("Choose a new --run-dir; existing runs are preserved")
    enabled = config.get("benchmark_enabled", True)
    iterations = config.get("benchmark_iterations", 3)
    if enabled and (not isinstance(iterations, int) or isinstance(iterations, bool) or iterations < 3):
        raise ValueError("D2 requires benchmark_iterations >= 3")
    publications = _publication_paths(config, root, output_path, config_path)
    inputs = verify_inputs(config["raw_dir"], config["checksums"])
    if "manifest" in config:
        expected = {line.split()[1]: line.split()[0] for line in Path(config["checksums"]).read_text().splitlines() if line.strip()}
        manifest_hash = stream_file_signature(Path(config["manifest"]))[0]
        if manifest_hash != expected.get("error_manifest.json"):
            raise ValueError("B1 error manifest checksum differs")
    sampling = _sampling_provenance(config)
    b1_seconds = time.monotonic() - start
    root.mkdir(parents=True, exist_ok=False)
    evidence = root / "evidence"
    paths = {name: str(root / name) for name in ("bronze", "silver", "silver_rejected", "gold", "benchmark")}
    paths.update({"evidence": str(evidence), "pipeline_manifest": str(evidence / "pipeline_run.json")})
    published = publications["manifest"]
    result = {
        "scope": "B1-E1" if enabled else "B1-D1 (D2 skipped; E1 acceptance incomplete)",
        "status": "running", "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_dir": str(root), "paths": paths, "published_manifest": str(published),
        "runtime": {"spark": spark.version, "delta_spark": package_version("delta-spark"),
                    "master": spark.sparkContext.master, "timezone": "UTC"},
        "stages": {},
    }
    if sampling is not None:
        sampling["evidence_path"] = str(evidence / "input_sampling.json")
        evidence.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(sampling["manifest_path"], sampling["evidence_path"])
        result["input_sampling"] = sampling
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    current_stage = "b1"

    def persist():
        result["elapsed_seconds"] = round(time.monotonic() - start, 3)
        write_json(evidence / "pipeline_progress.json", result)
        write_json(evidence / "pipeline_run.json", result)
        write_json(published, result)

    def record(name, value, elapsed, summary=None):
        stage_path = evidence / f"{name}.json"
        write_json(stage_path, value)
        if name in {"c1_fixture", "c3_vacuum"}:
            alias = "cdc_fixture.json" if name == "c1_fixture" else "vacuum.json"
            write_json(evidence / alias, value)
        result[name] = value if summary is None else summary(value)
        result["stages"][name] = {"status": "passed", "elapsed_seconds": round(elapsed, 3),
                                  "evidence_path": str(stage_path)}
        persist()
        print(f"[{name.upper()}] PASS in {elapsed:.3f}s; evidence: {stage_path}", flush=True)
        return value

    def execute(name, action, summary=None):
        nonlocal current_stage
        current_stage = name
        print(f"[{name.upper()}] Starting", flush=True)
        stage_start = time.monotonic()
        value = action()
        return record(name, value, time.monotonic() - stage_start, summary)

    bronze_path, silver_path = paths["bronze"], paths["silver"]
    try:
        record("b1", inputs, b1_seconds)
        execute("b2", lambda: bronze.run(spark, raw_dir=config["raw_dir"], bronze_dir=bronze_path))
        execute("b3", lambda: silver.build(spark, bronze_path, silver_path, paths["silver_rejected"],
                                          manifest_path=config["manifest"]))
        baseline = latest_version(spark, silver_path)
        fixture_path = str(root / "cdc" / "late_updates.parquet")
        fixture = execute("c1_fixture", lambda: generate_fixture(
            spark, silver_path, fixture_path, seed=config["cdc_seed"],
            updates=config["cdc_updates"], inserts=config["cdc_inserts"]))
        execute("c1", lambda: silver.apply_cdc(spark, silver_path, fixture_path))
        if result["c1"]["updated_count"] != config["cdc_updates"] or result["c1"]["inserted_count"] != config["cdc_inserts"]:
            raise AssertionError("Fresh C1 demo did not update and insert the requested rows")
        merge_version = latest_version(spark, silver_path)
        batch_file = generate_batch_04(str(root / "raw" / "batch_04.json"), config["evolution_rows"])
        bronze_before = latest_version(spark, bronze_path)
        execute("c2", lambda: silver.apply_evolution(spark, bronze_path, silver_path, batch_path=batch_file))
        versions = {"baseline": baseline, "merge": merge_version, "evolution": latest_version(spark, silver_path),
                    "bronze_before_evolution": bronze_before, "bronze_evolution": latest_version(spark, bronze_path)}

        def check_replay():
            replay = {"bronze": evolve_bronze(spark, batch_file, bronze_path),
                      "silver": evolve_silver(spark, bronze_path, silver_path)}
            if not all(item["replayed"] and item["before_version"] == item["after_version"] for item in replay.values()):
                raise AssertionError("Identical C2 replay should be a no-op in both layers")
            old_bronze = snapshot(spark, bronze_path, bronze_before)
            require_same_rows(old_bronze, snapshot(spark, bronze_path).filter("_batch_id <> 'batch_04'").select(old_bronze.columns),
                              "C2 preserves every raw Bronze value")
            return replay

        execute("c2_replay", check_replay)
        execute("c3_audit", lambda: audit_history(spark, bronze_path, silver_path, versions, fixture, str(evidence)),
                lambda audit: {"versions": versions,
                               "snapshot_counts": {key: value["count"] for key, value in audit["snapshots"].items()},
                               "audit_file": str(evidence / "audit.json")})
        execute("c3_vacuum", lambda: vacuum_copy_demo(spark, silver_path, str(root / "vacuum_copy"),
                                                      [baseline, merge_version, versions["evolution"]]),
                lambda vacuum: {"deleted_data_file_count": len(vacuum["deleted_data_files"]),
                                "copy_old_read_failed_as_expected": vacuum["copy_old_read_failure"]["expected_missing_data_file"],
                                "original_versions_readable": list(vacuum["original_versions_after_vacuum"]),
                                "original_files_unchanged": vacuum["original_files_unchanged"],
                                "retention_check_restored": vacuum["retention_check_restored"]})
        execute("d1", lambda: gold.run(spark, silver_path, paths["gold"], grain=config.get("gold_grain", "zone_hourofday"),
                                      source_version=versions["evolution"]))
        gold_summary = publications["gold_summary"]
        write_json(gold_summary, result["d1"])
        paths["published_gold_summary"] = str(gold_summary)
        if enabled:
            from optimization_benchmark import run_benchmark
            benchmark = execute("d2", lambda: run_benchmark(
                spark, silver_path, paths["benchmark"], iterations=iterations,
                source_version=versions["evolution"], target_file_size=config.get("benchmark_target_file_size", 32768),
                baseline_files=config.get("benchmark_baseline_files", 32),
                sampling_metadata=result.get("input_sampling")),
                lambda benchmark: {"source": benchmark.get("source"), "validation": benchmark.get("validation"),
                                   "conditions": benchmark.get("conditions"), "metadata": benchmark.get("benchmark_metadata"),
                                   "results_file": str(Path(paths["benchmark"]) / "results.json"),
                                   "report_file": str(Path(paths["benchmark"]) / "report.md")})
            if latest_version(spark, silver_path) != versions["evolution"]:
                raise AssertionError("D2 changed the source Silver table")
            benchmark_summary = publications["benchmark_summary"]
            benchmark_report = publications["benchmark_report"]
            write_json(benchmark_summary, benchmark)
            benchmark_report.parent.mkdir(parents=True, exist_ok=True)
            benchmark_report.write_text((Path(paths["benchmark"]) / "report.md").read_text(encoding="utf-8"), encoding="utf-8")
            paths.update({"published_benchmark_results": str(benchmark_summary),
                          "published_benchmark_report": str(benchmark_report)})
        else:
            result["d2"] = {"status": "skipped", "reason": "benchmark_enabled=false"}
        result["counts"] = {"b1_raw": sum(value["rows"] for value in inputs.values()),
                            "b2_bronze": result["b2"]["bronze_count"],
                            "b3_silver": result["b3"]["silver_count"],
                            "b3_rejected": result["b3"]["rejected_count"],
                            "final_bronze": result["c2"]["bronze"]["bronze_count_after"],
                            "final_silver": result["d1"]["checks"]["silver_rows"],
                            "gold_buckets": result["d1"]["rows_written"],
                            "gold_trip_count_sum": result["d1"]["checks"]["gold_trip_count_sum"]}
        result["status"] = "passed" if enabled else "partial"
        result["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        persist()
        print(f"[{result['status'].upper()}] {result['scope']}: {published}", flush=True)
        return result
    except Exception as exc:
        result["status"] = "failed"
        result["failed_stage"] = current_stage
        result["error"] = {"type": type(exc).__name__, "message": str(exc)}
        result["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        persist()
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/pipeline.json")
    parser.add_argument("--run-dir", help="New output directory; existing directories are refused")
    parser.add_argument("--output", help="Latest run manifest (default: docs/pipeline_run.json)")
    parser.add_argument("--skip-benchmark", action="store_true", help="Development-only partial run; does not satisfy E1 acceptance")
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    if args.skip_benchmark:
        config["benchmark_enabled"] = False
    run_dir = args.run_dir or str(Path(config["run_root"]) / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    verify_inputs(config["raw_dir"], config["checksums"])
    if Path(run_dir).exists():
        raise FileExistsError("Choose a new --run-dir; existing runs are preserved")
    spark = get_spark(config["master"], app_name="LakehouseThroughE1")
    try:
        run(spark, config, run_dir, output_path=args.output, config_path=args.config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
