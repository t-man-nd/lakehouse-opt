"""C3 — Time travel, table history and the transaction log, as evidence files.

Produces (under docs/evidence/):
  silver_history.json        DESCRIBE HISTORY of Silver (every batch MERGE, the CDC MERGE, schema change)
  bronze_history.json        DESCRIBE HISTORY of Bronze (one append per source file)
  time_travel.json           row counts at every Silver version + before/after CDC comparison
  vacuum_demo.json           VACUUM on a *copy*: what breaks and what still works
  delta_log_annotated.md     one real commit file, with the data-skipping statistics explained

The VACUUM demo sets spark.databricks.delta.retentionDurationCheck.enabled=false.
That is a demo-only hack: it disables the safety check that stops you from
deleting files still needed by readers or by time travel. It is applied to a
throw-away copy and restored immediately.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import LakehousePaths, get_paths
from src.delta_utils import (
    history_records, latest_version, list_commit_versions, summarize_add_actions,
    temporary_conf, write_json,
)


def count_per_version(spark: SparkSession, path: str) -> List[Dict[str, Any]]:
    rows = []
    for record in history_records(spark, path):
        version = int(record["version"])
        df = spark.read.format("delta").option("versionAsOf", version).load(path)
        rows.append({"version": version, "operation": record["operation"], "rows": df.count(),
                     "columns": len(df.columns), "userMetadata": record.get("userMetadata")})
    return rows


def compare_versions(spark: SparkSession, path: str, before: int, after: int, key: str = "trip_key",
                     columns: tuple = ("fare_amount", "tip_amount", "total_amount")) -> Dict[str, Any]:
    """Rows whose tracked columns differ between two versions (the MERGE's footprint)."""
    old = spark.read.format("delta").option("versionAsOf", before).load(path)
    new = spark.read.format("delta").option("versionAsOf", after).load(path)
    cols = [c for c in columns if c in old.columns and c in new.columns]
    joined = old.select(key, *[F.col(c).alias(f"old_{c}") for c in cols]).join(
        new.select(key, *[F.col(c).alias(f"new_{c}") for c in cols]), key, "full_outer")
    changed = joined.filter(" OR ".join(f"NOT (old_{c} <=> new_{c})" for c in cols))
    only_new = joined.filter(f"old_{cols[0]} IS NULL AND new_{cols[0]} IS NOT NULL")
    return {
        "before_version": before, "after_version": after,
        "rows_before": old.count(), "rows_after": new.count(),
        "rows_changed_or_added": changed.count(), "rows_added": only_new.count(),
        "sample": [r.asDict() for r in changed.limit(5).collect()],
    }


def run_vacuum_demo(spark: SparkSession, source_path: str, scratch_dir: Path) -> Dict[str, Any]:
    """Show the VACUUM / time-travel interaction on a copy, never on the real table."""
    from delta import DeltaTable

    copy_path = scratch_dir / "silver_vacuum_copy"
    if copy_path.exists():
        shutil.rmtree(copy_path)
    shutil.copytree(source_path, copy_path)
    copy = str(copy_path)
    versions = [int(r["version"]) for r in history_records(spark, copy)]
    oldest, newest = versions[0], versions[-1]

    with temporary_conf(spark, {"spark.databricks.delta.retentionDurationCheck.enabled": "false"}):
        dry_run = spark.sql(f"VACUUM delta.`{copy}` RETAIN 0 HOURS DRY RUN").count()
        DeltaTable.forPath(spark, copy).vacuum(0.0)

    result: Dict[str, Any] = {"copy_path": copy, "files_listed_by_dry_run": dry_run,
                              "oldest_version": oldest, "newest_version": newest}
    try:
        result["latest_version_rows_after_vacuum"] = spark.read.format("delta").load(copy).count()
    except Exception as exc:  # noqa: BLE001
        result["latest_version_error"] = str(exc)[:300]
    # Probe every old version: versions whose files were later removed (rewritten by an
    # UPDATE/MERGE or OPTIMIZE) break; versions made only of still-live files survive.
    per_version = {}
    # Reading a vacuumed version is *expected* to fail; keep Spark from printing the stack trace.
    spark.sparkContext.setLogLevel("OFF")
    for version in versions:
        try:
            # count() alone is served from log statistics and never opens a data file.
            snapshot = spark.read.format("delta").option("versionAsOf", version).load(copy)
            snapshot.agg(F.sum(F.crc32(F.col("trip_key")))).collect()
            per_version[version] = "readable"
        except Exception as exc:  # noqa: BLE001
            per_version[version] = f"FAILED: {type(exc).__name__}: {str(exc).splitlines()[0][:160]}"
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    result["versions_after_vacuum"] = per_version
    result["broken_versions"] = [v for v, s in per_version.items() if s != "readable"]
    result["explanation"] = (
        "VACUUM deleted data files no longer referenced by the latest version. Any older version "
        "that still pointed at those files can no longer be read; this is why the default 7-day "
        "retention exists and why the real table was never vacuumed with 0 hours."
    )
    return result


def annotate_delta_log(table_path: str, version: int, out_file: Path, title: str) -> Dict[str, Any]:
    """Write one real commit with a short explanation of each action type."""
    summary = summarize_add_actions(table_path, version)
    raw = (Path(table_path) / "_delta_log" / f"{version:020d}.json").read_text(encoding="utf-8").splitlines()
    first_add = next((json.loads(line) for line in raw if '"add"' in line), None)
    lines = [
        f"# Annotated `_delta_log` commit: {title}",
        "",
        f"File: `{Path(table_path).name}/_delta_log/{version:020d}.json`  ",
        f"Actions: {summary['action_counts']}",
        "",
        "Each line of a commit file is one action. Readers replay these actions (from the latest",
        "checkpoint forward) to know exactly which Parquet files form a version; that replay is",
        "what gives atomicity (a commit file either exists or not) and snapshot isolation",
        "(a reader pins one version number).",
        "",
        "## commitInfo",
        "",
        "Audit record behind `DESCRIBE HISTORY`: operation, parameters, metrics, and our",
        "`userMetadata` (batch id and source file hash).",
        "",
        "```json",
        json.dumps(summary["commitInfo"], indent=2)[:4000],
        "```",
        "",
        "## One `add` action",
        "",
        "`path` is a Parquet file now part of the table. `stats` holds `numRecords`, and per-column",
        "`minValues`, `maxValues`, `nullCount`. A query such as `WHERE PULocationID = 132` compares",
        "132 with each file's min/max and skips files that cannot contain it: this is data",
        "skipping. Z-ORDER and liquid clustering work by making these min/max ranges narrow.",
        "",
        "```json",
        json.dumps(first_add, indent=2)[:4000] if first_add else "(no add action in this commit)",
        "```",
    ]
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"file": str(out_file), "version": version, "action_counts": summary["action_counts"]}


def run(spark: SparkSession, paths: Optional[LakehousePaths] = None) -> Dict[str, Any]:
    paths = paths or get_paths()
    evidence = paths.evidence_dir
    silver_history = history_records(spark, paths.silver_trips)
    bronze_history = history_records(spark, paths.bronze_trips)
    write_json(evidence / "silver_history.json", silver_history)
    write_json(evidence / "bronze_history.json", bronze_history)

    per_version = count_per_version(spark, paths.silver_trips)
    cdc_file = evidence / "cdc_merge.json"
    comparison = None
    if cdc_file.exists():
        cdc = json.loads(cdc_file.read_text(encoding="utf-8"))
        if "silver_version_before" in cdc:
            comparison = compare_versions(spark, paths.silver_trips,
                                          cdc["silver_version_before"], cdc["silver_version_after"])
    time_travel = {"silver_rows_per_version": per_version, "cdc_version_comparison": comparison,
                   "old_versions_queried": len([v for v in per_version if v["version"] < per_version[-1]["version"]])}
    write_json(evidence / "time_travel.json", time_travel)

    vacuum = run_vacuum_demo(spark, paths.silver_trips, paths.scratch_dir)
    # The real table was never vacuumed, so its history is intact:
    vacuum["real_table_oldest_version_rows"] = spark.read.format("delta").option(
        "versionAsOf", per_version[0]["version"]).load(paths.silver_trips).count()
    write_json(evidence / "vacuum_demo.json", vacuum)

    bronze_versions = list_commit_versions(paths.bronze_trips)
    annotated = annotate_delta_log(paths.bronze_trips, bronze_versions[-1],
                                   evidence / "delta_log_annotated.md", "latest Bronze append")
    checkpoints = sorted(p.name for p in (Path(paths.silver_trips) / "_delta_log").glob("*.checkpoint*.parquet"))
    merge_ops = [h for h in silver_history if h["operation"] == "MERGE"]
    return {
        "silver_versions": len(silver_history),
        "silver_latest_version": latest_version(spark, paths.silver_trips),
        "merge_commits": len(merge_ops),
        "old_versions_queried": time_travel["old_versions_queried"],
        "cdc_rows_changed_or_added": comparison["rows_changed_or_added"] if comparison else None,
        "vacuum_demo": {k: vacuum[k] for k in ("broken_versions", "real_table_oldest_version_rows") if k in vacuum},
        "delta_log_annotation": annotated,
        "silver_checkpoints": checkpoints,
    }
