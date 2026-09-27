#!/usr/bin/env python3
"""E1 — Run the whole lakehouse with one command.

Real data (default):
    python scripts/download_sources.py            # once: 6 months + reference files
    python lakehouse_pipeline.py                   # reference -> bronze -> profile -> silver
                                                   # -> cdc -> evolution -> time travel -> gold
    python lakehouse_pipeline.py --with-benchmark  # + D2 performance lab

Synthetic fixture (B1-v1.0 contract, exact 3,060 / 2,880 / 180 reconciliation):
    python lakehouse_pipeline.py --source fixture

Useful flags:
    --steps silver gold      run only some steps (each step is idempotent)
    --rebuild                drop Silver/Gold state first (Bronze is never dropped)
    --dev-sample 1           deterministic 1% sample, written to data/lakehouse-dev

Every run writes docs/pipeline_run.json (step results, timings, counts) and
docs/RESULTS.md (the numbers the report quotes).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from src.config import DEFAULT_MONTHS, LakehousePaths, get_paths
from src.delta_utils import to_jsonable, write_json

TLC_STEPS = ["reference", "bronze", "profile", "silver", "cdc", "evolution", "time_travel", "gold", "benchmark"]


def log(message: str) -> None:
    print(f"{datetime.now().strftime('%H:%M:%S')} | {message}", flush=True)


def build_tlc_steps(spark, paths: LakehousePaths, args: argparse.Namespace) -> Dict[str, Callable[[], Dict[str, Any]]]:
    import optimization_benchmark
    from src import gold, reference_data, time_travel, tlc_bronze, tlc_profile, tlc_silver

    return {
        "reference": lambda: reference_data.run(spark, paths),
        "bronze": lambda: tlc_bronze.run(spark, args.months, overwrite=False, paths=paths,
                                         dev_sample_pct=args.dev_sample),
        "profile": lambda: tlc_profile.run(spark, paths),
        "silver": lambda: tlc_silver.build(spark, paths, rebuild=args.rebuild),
        "cdc": lambda: tlc_silver.apply_cdc(spark, paths),
        "evolution": lambda: tlc_silver.apply_evolution(spark, paths),
        "time_travel": lambda: time_travel.run(spark, paths),
        "gold": lambda: gold.run(spark, paths, args.months),
        "benchmark": lambda: optimization_benchmark.run(spark, paths, rounds=args.bench_rounds,
                                                        target_file_mb=args.target_file_mb),
    }


def run_fixture(spark, paths: LakehousePaths) -> Dict[str, Any]:
    """B1-v1.0 lane: existing, verified B2/B3 code on the synthetic dirty JSON."""
    from src.bronze import run_bronze
    from src.silver import build as build_b1_silver

    lake = paths.lake
    raw_dir = paths.root / "data" / "raw"
    manifest = paths.docs_dir / "error_manifest.json"   # B1-v1.0 contract (team file)
    if not (raw_dir / "batch_01.json").exists():
        raise FileNotFoundError("data/raw/batch_0{1,2,3}.json missing; run `python -m src.generator --output-dir data/raw`")
    bronze = str(lake / "bronze" / "fixture_trips")
    import shutil
    if Path(bronze).exists():
        shutil.rmtree(bronze)  # fixture Bronze is rebuilt so the exact 3,060 check is repeatable
    result = {"bronze": run_bronze(spark, str(raw_dir), bronze)}
    result["silver"] = build_b1_silver(
        spark, bronze, str(lake / "silver" / "fixture_trips"), str(lake / "silver" / "fixture_trips_rejected"),
        manifest_path=str(manifest) if manifest.exists() else None)
    return result


def render_results(run: Dict[str, Any], paths: LakehousePaths) -> str:
    steps = {s["step"]: s for s in run["steps"]}
    lines = ["# Pipeline results (auto-generated)", "",
             f"Run started {run['started_at']} · status **{run['status']}** · source `{run['source']}` · "
             f"months {', '.join(run['months'])}", "",
             "| Step | Status | Seconds | Key result |", "|---|---|---:|---|"]
    key_fields = {
        "reference": ("zones", "cbd_zones"), "bronze": ("bronze_count", "bronze_version"),
        "profile": ("keys_seen_in_more_than_one_file",), "silver": ("silver_count", "rejected_count", "conservation_ok"),
        "cdc": ("checks_ok",), "evolution": ("status",), "time_travel": ("silver_versions", "merge_commits"),
        "gold": ("gold_trips", "zone_hourly_rows"), "benchmark": ("rows",),
    }
    for step in run["steps"]:
        res = step.get("result") or {}
        key = ", ".join(f"{k}={res.get(k)}" for k in key_fields.get(step["step"], ()) if k in res)
        lines.append(f"| {step['step']} | {step['status']} | {step['seconds']} | {key or step.get('error', '')[:80]} |")

    silver = steps.get("silver", {}).get("result")
    if silver:
        lines += ["", "## Silver reconciliation", "",
                  f"Bronze {silver['bronze_count']:,} = rejected {silver['totals']['rejected_rows']:,} + inserted "
                  f"{silver['totals']['expected_inserts']:,} + updated {silver['totals']['expected_updates']:,} + "
                  f"no-op {silver['totals']['expected_noop']:,}", "",
                  "| Batch | Bronze rows | Rejected | Inserted | Updated | No-op | Silver version |",
                  "|---|---:|---:|---:|---:|---:|---:|"]
        for b in silver["batches"]:
            lines.append(f"| {b['batch_id']} | {b['bronze_rows']:,} | {b['rejected_rows']:,} | {b['expected_inserts']:,} | "
                         f"{b['expected_updates']:,} | {b['expected_noop']:,} | {b['silver_version']} |")
        lines += ["", "Reject reasons: " + ", ".join(f"{k} {v:,}" for k, v in silver["reject_reason_counts"].items())]
    cdc = steps.get("cdc", {}).get("result")
    if cdc and "merge_metrics" in cdc:
        lines += ["", "## CDC MERGE (C1)", "",
                  f"Silver v{cdc['silver_version_before']} -> v{cdc['silver_version_after']}: planned {cdc['planned']}, "
                  f"Delta metrics {cdc['merge_metrics']}, rows {cdc['count_before']:,} -> {cdc['count_after']:,}. "
                  f"Checks: {cdc['checks']}"]
    gold = steps.get("gold", {}).get("result")
    if gold:
        lines += ["", "## CBD flows, 2025 vs 2024 (trips per calendar day)", "",
                  "| Month | Flow | 2024/day | 2025/day | YoY % | vs non-CBD (pp) |", "|---:|---|---:|---:|---:|---:|"]
        for r in gold.get("cbd_yoy", []):
            lines.append(f"| {r['month_of_year']} | {r['cbd_flow']} | {r['trips_per_day_prev']} | {r['trips_per_day']} | "
                         f"{r['yoy_trips_per_day_pct']} | {r['relative_to_non_cbd_pp']} |")
        lines += ["", f"Gold vs manual SQL on Silver: all_match = {gold['verification']['all_match']}. "
                  f"External reconciliation: {gold['external_reconciliation'].get('status')}."]
    lines += ["", "Evidence files: docs/evidence/, docs/profile/, docs/benchmark/."]
    return "\n".join(lines) + "\n"


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", choices=["tlc", "fixture"], default="tlc")
    parser.add_argument("--months", nargs="+", default=DEFAULT_MONTHS)
    parser.add_argument("--steps", nargs="+", choices=TLC_STEPS, help="subset of steps (default: all except benchmark)")
    parser.add_argument("--with-benchmark", action="store_true")
    parser.add_argument("--bench-rounds", type=int, default=5)
    parser.add_argument("--target-file-mb", type=int, default=32)
    parser.add_argument("--rebuild", action="store_true", help="drop Silver state before building (Bronze is kept)")
    parser.add_argument("--dev-sample", type=float, default=None, help="percent of rows to keep, e.g. 1 for 1%%")
    parser.add_argument("--master", default=None)
    args = parser.parse_args(argv)

    if args.dev_sample and "LAKEHOUSE_DIR" not in os.environ:
        os.environ["LAKEHOUSE_DIR"] = str(Path(get_paths().root) / "data" / "lakehouse-dev")
    paths = get_paths()

    from src.spark_session import create_spark
    spark = create_spark("lakehouse-pipeline", master=args.master)
    run: Dict[str, Any] = {"started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                           "source": args.source, "months": sorted(args.months), "lake": str(paths.lake),
                           "spark": spark.version, "steps": [], "status": "RUNNING"}
    exit_code = 0
    try:
        if args.source == "fixture":
            steps = {"fixture": lambda: run_fixture(spark, paths)}
            selected = ["fixture"]
        else:
            steps = build_tlc_steps(spark, paths, args)
            selected = args.steps or [s for s in TLC_STEPS if s != "benchmark"]
            if args.with_benchmark and "benchmark" not in selected:
                selected.append("benchmark")
        for name in selected:
            log(f"▶ {name}")
            started = time.perf_counter()
            entry: Dict[str, Any] = {"step": name}
            try:
                entry["result"] = steps[name]()
                entry["status"] = "OK"
            except Exception as exc:  # noqa: BLE001 - record, then stop the run
                entry["status"] = "FAILED"
                entry["error"] = f"{type(exc).__name__}: {exc}"
                entry["traceback"] = traceback.format_exc()
                raise
            finally:
                entry["seconds"] = round(time.perf_counter() - started, 1)
                run["steps"].append(entry)
                log(f"{'✔' if entry.get('status') == 'OK' else '✖'} {name} ({entry['seconds']} s)")
                if entry.get("status") == "FAILED":
                    log(f"  error: {entry['error']}")
        run["status"] = "SUCCESS"
    except Exception:  # noqa: BLE001
        run["status"] = "FAILED"
        exit_code = 1
    finally:
        run["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        suffix = "" if args.source == "tlc" and not args.dev_sample else f"_{args.source}{'_dev' if args.dev_sample else ''}"
        write_json(paths.docs_dir / f"pipeline_run{suffix}.json", run)
        (paths.docs_dir / f"RESULTS{suffix}.md").write_text(render_results(to_jsonable(run), paths), encoding="utf-8")
        log(f"run {run['status']} -> docs/pipeline_run{suffix}.json")
        try:
            spark.stop()
        except Exception as exc:  # noqa: BLE001
            # The JVM may already be gone (e.g. killed by the OS out-of-memory killer).
            # Never let shutdown noise hide the real failure above.
            log(f"note: Spark shutdown failed ({type(exc).__name__}); the JVM had already exited. "
                "If no step error is shown above, the JVM was killed externally - check "
                "`dmesg -T | tail` for an out-of-memory kill and lower SPARK_DRIVER_MEMORY.")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
