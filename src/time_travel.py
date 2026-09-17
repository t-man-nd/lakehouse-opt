"""C3: materialized historical reads, transaction-log audit and isolated VACUUM."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from urllib.parse import unquote, urlparse

from delta import DeltaTable
from pyspark.sql import SparkSession, functions as F

from src.delta_runtime import latest_version, snapshot, write_json


def fingerprint(frame) -> dict:
    """Read every value, not a metadata-only count; bounded classroom dataset."""
    rows = sorted(json.dumps(row.asDict(recursive=True), sort_keys=True, default=str) for row in frame.collect())
    return {"count": len(rows), "rows_sha256": hashlib.sha256("\n".join(rows).encode()).hexdigest(),
            "schema": frame.schema.jsonValue()}


def file_hashes(root: Path) -> dict:
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(root.rglob("*")) if path.is_file()}


def inspect_log(table: str, version: int) -> dict:
    path = Path(table) / "_delta_log" / f"{version:020d}.json"
    actions = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    counts = {}
    for action in actions:
        for kind in action:
            counts[kind] = counts.get(kind, 0) + 1
    return {"version": version, "file": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "action_counts": counts, "actions": actions}


def audit_history(
    spark: SparkSession, bronze_dir: str, silver_dir: str, versions: dict, fixture: dict, evidence_dir: str,
) -> dict:
    output = Path(evidence_dir)
    snapshots = {}
    probe_id = fixture["updates"][0]["trip_id"]
    latest = latest_version(spark, silver_dir)
    if len({versions["baseline"], versions["merge"]} - {latest}) < 2:
        raise ValueError("C3 requires at least two distinct old Silver versions")
    history = {}
    for layer, table in (("bronze", bronze_dir), ("silver", silver_dir)):
        escaped = table.replace("`", "``")
        # Format in Spark's UTC session before collect: Python otherwise creates
        # naive datetimes in the host timezone, which would mislabel the audit.
        history_frame = spark.sql(f"DESCRIBE HISTORY delta.`{escaped}`").withColumn(
            "timestamp", F.date_format("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        )
        history[layer] = [row.asDict(recursive=True) for row in history_frame.collect()]
        write_json(output / f"{layer}_history.json", history[layer])
    if next(row for row in history["silver"] if row["version"] == versions["merge"])["operation"] != "MERGE":
        raise AssertionError("Expected a real MERGE history entry")
    for stage in ("baseline", "merge", "evolution"):
        version = versions[stage]
        frame = snapshot(spark, silver_dir, version)
        probe = frame.filter(F.col("trip_id") == probe_id).select("trip_id", "fare_amount", "tip_amount", "total_amount").first().asDict()
        expected = fixture["updates"][0]["before" if stage == "baseline" else "after"]
        if any(probe[name] != value for name, value in expected.items()):
            raise AssertionError(f"Incorrect historical CDC values at {stage}")
        insert_count = frame.filter(F.col("trip_id").isin(fixture["insert_ids"])).count()
        if insert_count != (0 if stage == "baseline" else len(fixture["insert_ids"])):
            raise AssertionError("Historical insert visibility is wrong")
        if ("surcharge_fee" in frame.columns) != (stage == "evolution"):
            raise AssertionError("Historical schema does not match its version")
        snapshots[stage] = {"version": version, **fingerprint(frame), "updated_trip": probe,
                            "cdc_insert_count": insert_count}

    logs = {}
    for layer, table, selected in (
        ("silver", silver_dir, [versions[k] for k in ("baseline", "merge", "evolution")]),
        ("bronze", bronze_dir, [versions["bronze_before_evolution"], versions["bronze_evolution"]]),
    ):
        logs[layer] = [inspect_log(table, version) for version in selected]
        destination = output / "delta_log" / layer
        destination.mkdir(parents=True, exist_ok=True)
        for entry in logs[layer]:
            shutil.copy2(Path(table) / "_delta_log" / entry["file"], destination / entry["file"])
        evolved = logs[layer][-1]
        metadata = [action["metaData"] for action in evolved["actions"] if "metaData" in action]
        if len(metadata) != 1:
            raise AssertionError(f"{layer} evolution commit must contain a metaData action")
        fields = json.loads(metadata[0]["schemaString"])["fields"]
        if not any(field["name"] == "surcharge_fee" and field["type"] == "double" for field in fields):
            raise AssertionError("New surcharge_fee double missing in transaction metadata")
        if evolved["action_counts"].get("remove", 0):
            raise AssertionError("C2 must append without replacing old files")
    merge_log = logs["silver"][1]
    if not merge_log["action_counts"].get("add") or not merge_log["action_counts"].get("remove"):
        raise AssertionError("Expected MERGE add/remove actions in this non-DV demo")

    addition = next(action["add"] for action in logs["silver"][-1]["actions"] if "add" in action)
    stats = json.loads(addition["stats"])
    low, high = stats["minValues"]["PULocationID"], stats["maxValues"]["PULocationID"]
    outside = high + 1
    actual = spark.read.parquet(str(Path(silver_dir) / unquote(addition["path"])))
    if actual.filter(F.col("PULocationID") == outside).count() != 0:
        raise AssertionError("File min/max bounds do not match its values")
    skipping = {"file": addition["path"], "numRecords": stats["numRecords"], "minValues": stats["minValues"],
                "maxValues": stats["maxValues"], "nullCount": stats["nullCount"],
                "example_predicate": f"PULocationID = {outside}", "file_can_be_skipped": True,
                "explanation": f"{outside} is outside [{low}, {high}]; this proves the bound, not measured scan savings."}
    result = {"versions": versions, "snapshots": snapshots, "log_summary": {
        layer: [{k: v for k, v in entry.items() if k != "actions"} for entry in entries]
        for layer, entries in logs.items()}, "data_skipping": skipping}
    write_json(output / "audit.json", result)
    return result


def vacuum_copy_demo(spark: SparkSession, original: str, copy_path: str, versions: list[int]) -> dict:
    """Physically copy all local data/log files; NEVER vacuum the source or a shallow clone."""
    source, destination = Path(original).resolve(), Path(copy_path).resolve()
    if source == destination or source in destination.parents or destination in source.parents:
        raise ValueError("VACUUM copy must be separate from the source and its ancestors")
    if not versions:
        raise ValueError("Provide historical versions to verify")
    if destination.exists():
        raise FileExistsError("VACUUM destination must be a new directory owned by this demo")
    if not (source / "_delta_log").is_dir():
        raise ValueError("VACUUM demo supports only an existing local Delta source")
    if any(path.is_symlink() for path in source.rglob("*")):
        raise ValueError("VACUUM source must not contain symlinks")
    for path in (source / "_delta_log").glob("*.json"):
        for line in path.read_text().splitlines():
            action = json.loads(line)
            for kind in ("add", "remove"):
                if kind in action:
                    relative = unquote(action[kind]["path"])
                    parsed = urlparse(relative)
                    if parsed.scheme or Path(relative).is_absolute() or ".." in Path(relative).parts:
                        raise ValueError("Copy demo requires all Delta data paths to be relative and internal")
    original_files = file_hashes(source)
    signatures = {str(version): fingerprint(snapshot(spark, original, version)) for version in versions}
    original_latest = latest_version(spark, original)
    current_signature = fingerprint(snapshot(spark, original, original_latest))
    shutil.copytree(source, destination, copy_function=shutil.copy2)
    if file_hashes(destination) != original_files:
        raise AssertionError("Physical copy differs before VACUUM")
    old_version = min(versions)
    if fingerprint(snapshot(spark, str(destination), old_version)) != signatures[str(old_version)]:
        raise AssertionError("Copied history was not readable before VACUUM")
    before_files = {str(path.relative_to(destination)) for path in destination.rglob("*.parquet")}
    setting = "spark.databricks.delta.retentionDurationCheck.enabled"
    saved = spark.conf.get(setting, "true")
    try:
        # DEMO-ONLY HACK. This new physical copy has no other readers/writers.
        spark.conf.set(setting, "false")
        escaped = str(destination).replace("`", "``")
        dry_run = [row.asDict() for row in spark.sql(f"VACUUM delta.`{escaped}` RETAIN 0 HOURS DRY RUN").collect()]
        DeltaTable.forPath(spark, str(destination)).vacuum(0)
    finally:
        spark.conf.set(setting, saved)
    deleted = sorted(before_files - {str(path.relative_to(destination)) for path in destination.rglob("*.parquet")})
    if not deleted:
        raise AssertionError("VACUUM demonstration deleted no obsolete data files")
    if fingerprint(snapshot(spark, str(destination))) != current_signature:
        raise AssertionError("VACUUM changed the copy's current snapshot")
    try:
        fingerprint(snapshot(spark, str(destination), old_version))
    except Exception as error:
        message = str(error)
        if not any(marker in message for marker in ("DELTA_FILE_NOT_FOUND", "FileNotFoundException", "FAILED_READ_FILE.FILE_NOT_EXIST")):
            raise
        failure = {"exception_type": type(error).__name__, "expected_missing_data_file": True,
                   "message_excerpt": message[:900]}
    else:
        raise AssertionError("Old copy version unexpectedly remained readable after deleting its files")
    after = {str(version): fingerprint(snapshot(spark, original, version)) for version in versions}
    if after != signatures or file_hashes(source) != original_files or latest_version(spark, original) != original_latest:
        raise AssertionError("Original table/history changed during the copy-only VACUUM demo")
    return {"source": str(source), "copy": str(destination), "retention_hours": 0, "demo_only": True,
            "retention_check_restored": spark.conf.get(setting) == saved, "dry_run": dry_run,
            "deleted_data_files": deleted, "copy_latest_count": current_signature["count"],
            "copy_old_version": old_version, "copy_old_read_failure": failure,
            "original_files_unchanged": True, "original_versions_after_vacuum": after}
