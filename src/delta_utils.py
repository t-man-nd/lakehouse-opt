"""Small helpers around Delta Lake used by every layer.

The `_delta_log` readers are plain Python (no Spark) so the report evidence
(schema changes, add-action statistics) comes straight from the JSON commits
that define the table.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from pyspark.sql import SparkSession


# ----------------------------------------------------------------------------
# Table state
# ----------------------------------------------------------------------------
def table_exists(spark: SparkSession, path: str) -> bool:
    from delta import DeltaTable

    return Path(path, "_delta_log").exists() and DeltaTable.isDeltaTable(spark, path)


def latest_version(spark: SparkSession, path: str) -> int:
    from delta import DeltaTable

    return int(DeltaTable.forPath(spark, path).history(1).first()["version"])


def to_jsonable(value: Any) -> Any:
    """Convert Row/Map/datetime values into JSON-serialisable Python objects."""
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "asDict"):
        return to_jsonable(value.asDict(recursive=True))
    return value


def history_records(spark: SparkSession, path: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """DESCRIBE HISTORY as a list of dicts, oldest version first."""
    from delta import DeltaTable

    table = DeltaTable.forPath(spark, path)
    history = table.history(limit) if limit else table.history()
    rows = history.select(
        "version", "timestamp", "operation", "operationParameters",
        "operationMetrics", "userMetadata",
    ).orderBy("version").collect()
    return [to_jsonable(r.asDict(recursive=True)) for r in rows]


def last_operation_metrics(spark: SparkSession, path: str) -> Dict[str, int]:
    """operationMetrics of the latest commit, converted to integers where possible."""
    from delta import DeltaTable

    row = DeltaTable.forPath(spark, path).history(1).first()
    metrics = row["operationMetrics"] or {}
    result: Dict[str, int] = {}
    for key, value in metrics.items():
        try:
            result[key] = int(value)
        except (TypeError, ValueError):
            continue
    result["version"] = int(row["version"])
    return result


def table_detail(spark: SparkSession, path: str) -> Dict[str, Any]:
    """numFiles / sizeInBytes / partitionColumns / clusteringColumns from DESCRIBE DETAIL."""
    from delta import DeltaTable

    row = DeltaTable.forPath(spark, path).detail().first().asDict()
    keep = ("numFiles", "sizeInBytes", "partitionColumns", "clusteringColumns", "properties", "minReaderVersion", "minWriterVersion", "tableFeatures")
    return to_jsonable({k: row.get(k) for k in keep if k in row})


@contextmanager
def commit_metadata(spark: SparkSession, message: Dict[str, Any]) -> Iterator[None]:
    """Attach userMetadata to every Delta commit made inside the block (incl. MERGE)."""
    key = "spark.databricks.delta.commitInfo.userMetadata"
    previous = spark.conf.get(key, None)
    spark.conf.set(key, json.dumps(message, sort_keys=True))
    try:
        yield
    finally:
        if previous is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, previous)


@contextmanager
def temporary_conf(spark: SparkSession, settings: Dict[str, str]) -> Iterator[None]:
    """Set Spark SQL confs for the duration of a block, restoring previous values."""
    previous = {k: spark.conf.get(k, None) for k in settings}
    for key, value in settings.items():
        spark.conf.set(key, value)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


# ----------------------------------------------------------------------------
# _delta_log readers (pure Python)
# ----------------------------------------------------------------------------
def read_commit(table_path: str, version: int) -> List[Dict[str, Any]]:
    """Return the actions of one JSON commit file."""
    log_file = Path(table_path) / "_delta_log" / f"{version:020d}.json"
    return [json.loads(line) for line in log_file.read_text(encoding="utf-8").splitlines() if line.strip()]


def list_commit_versions(table_path: str) -> List[int]:
    log_dir = Path(table_path) / "_delta_log"
    versions = []
    for file in log_dir.glob("*.json"):
        stem = file.stem
        if stem.isdigit():
            versions.append(int(stem))
    return sorted(versions)


def _schema_fields(schema_string: str) -> Dict[str, str]:
    schema = json.loads(schema_string)
    return {f["name"]: json.dumps(f["type"]) for f in schema.get("fields", [])}


def find_schema_changes(table_path: str) -> List[Dict[str, Any]]:
    """Scan every commit for metaData actions and report column additions/removals.

    Version 0 always contains a metaData action (table creation). Later
    metaData actions whose field list differs are schema changes; this is the
    evidence for schema evolution without rewriting old data files.
    """
    changes: List[Dict[str, Any]] = []
    previous: Optional[Dict[str, str]] = None
    for version in list_commit_versions(table_path):
        actions = read_commit(table_path, version)
        operation = next((a["commitInfo"].get("operation") for a in actions if "commitInfo" in a), None)
        for action in actions:
            if "metaData" not in action:
                continue
            fields = _schema_fields(action["metaData"]["schemaString"])
            if previous is None:
                changes.append({"version": version, "operation": operation, "event": "CREATE",
                                "columns": list(fields)})
            else:
                added = [c for c in fields if c not in previous]
                removed = [c for c in previous if c not in fields]
                retyped = [c for c in fields if c in previous and fields[c] != previous[c]]
                if added or removed or retyped:
                    changes.append({"version": version, "operation": operation, "event": "SCHEMA_CHANGE",
                                    "added": added, "removed": removed, "retyped": retyped})
            previous = fields
    return changes


def summarize_add_actions(table_path: str, version: int, max_files: int = 3) -> Dict[str, Any]:
    """Extract commitInfo and parsed per-file statistics from one commit.

    The `stats` JSON on each add action (numRecords, minValues, maxValues,
    nullCount) is what Delta uses for data skipping.
    """
    actions = read_commit(table_path, version)
    commit_info = next((a["commitInfo"] for a in actions if "commitInfo" in a), {})
    adds = [a["add"] for a in actions if "add" in a]
    removes = [a["remove"] for a in actions if "remove" in a]
    sample = []
    for add in adds[:max_files]:
        stats = json.loads(add["stats"]) if add.get("stats") else None
        sample.append({
            "path": add["path"],
            "size": add.get("size"),
            "partitionValues": add.get("partitionValues"),
            "dataChange": add.get("dataChange"),
            "stats": stats,
        })
    return {
        "version": version,
        "commitInfo": commit_info,
        "action_counts": {
            "add": len(adds), "remove": len(removes),
            "metaData": sum(1 for a in actions if "metaData" in a),
            "protocol": sum(1 for a in actions if "protocol" in a),
        },
        "sample_add_actions": sample,
    }


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(payload), indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
