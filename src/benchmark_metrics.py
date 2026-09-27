"""Collect executed Spark scan and input counters for one benchmark action.

These are Spark 4 JVM metrics, not estimates from Delta add-file statistics.
``numFiles`` describes files selected by the executed file scan;
``filesSize`` is their full stored length. Task ``inputBytes`` measures bytes
reported by the filesystem readers, including reads satisfied by the OS cache.
It is neither physical disk traffic nor a sum of selected file lengths.
"""

from __future__ import annotations

import time
import uuid
from typing import Any


class MetricsUnavailableError(RuntimeError):
    """A benchmark cannot substantiate its per-query scan/input counters."""


def _scala_items(values):
    iterator = values.iterator()
    while iterator.hasNext():
        yield iterator.next()


def _require_clean_attempt(attempt: dict[str, Any]) -> None:
    """Retry/speculation overhead makes a sample unsuitable for this lab."""
    if (
        attempt["status"] != "COMPLETE"
        or attempt["attempt_id"] != 0
        or attempt["failed_tasks"] != 0
        or attempt["killed_tasks"] != 0
        or attempt["completed_tasks"] != attempt["num_tasks"]
    ):
        raise MetricsUnavailableError(
            "Benchmark sample contains failed, retried, killed or incomplete "
            f"tasks/stages: {attempt}"
        )


def _scan_metrics(spark, plan) -> list[dict[str, Any]]:
    """Traverse the final plan, including AQE stages hidden from children()."""
    scans = []
    pending = [plan]
    visited = set()
    while pending:
        node = pending.pop()
        identity = int(spark._jvm.java.lang.System.identityHashCode(node))
        if identity in visited:
            continue
        visited.add(identity)
        class_name = str(node.getClass().getSimpleName())
        if class_name == "AdaptiveSparkPlanExec":
            pending.append(node.executedPlan())
        elif class_name.endswith("QueryStageExec"):
            pending.append(node.plan())
        elif class_name == "ReusedExchangeExec":
            pending.append(node.child())
        else:
            pending.extend(_scala_items(node.children()))

        if class_name == "FileSourceScanExec":
            metrics = {
                str(entry._1()): int(entry._2().value())
                for entry in _scala_items(node.metrics())
            }
            if "numFiles" not in metrics or "filesSize" not in metrics:
                raise MetricsUnavailableError(
                    f"Executed {class_name} has no numFiles/filesSize counters"
                )
            if metrics["numFiles"] < 0 or metrics["filesSize"] < 0:
                raise MetricsUnavailableError("Executed scan counters are negative")
            scans.append({
                "node_id": int(node.id()),
                "node_name": str(node.nodeName()),
                "class_name": class_name,
                "metrics": metrics,
            })
    if not scans:
        raise MetricsUnavailableError(
            "No executed FileSourceScanExec found. Use an uncached file-table "
            "query with a data aggregate (not a metadata-only COUNT)."
        )
    return scans


def _stage_metrics(spark, job_ids: list[int], started_ms: int):
    store = spark.sparkContext._jsc.sc().statusStore()
    stage_ids = set()
    for job_id in job_ids:
        job = store.job(job_id)
        if str(job.status()) != "SUCCEEDED":
            raise MetricsUnavailableError(f"Query job {job_id} did not succeed")
        stage_ids.update(int(stage_id) for stage_id in _scala_items(job.stageIds()))

    attempts = []
    for stage_id in sorted(stage_ids):
        stages = store.stageData(
            stage_id, False, getattr(store, "stageData$default$3")(),
            False, getattr(store, "stageData$default$5")(),
        )
        for stage in _scala_items(stages):
            # A later AQE job can list an earlier completed stage. Count every
            # new attempt once, excluding reused work preceding this action.
            submitted = stage.submissionTime()
            if not submitted.isDefined():
                if str(stage.status()) == "SKIPPED":
                    continue
                raise MetricsUnavailableError(f"Stage {stage_id} has no start time")
            if int(submitted.get().getTime()) < started_ms:
                continue
            if not stage.completionTime().isDefined():
                raise MetricsUnavailableError(f"Stage {stage_id} has no final metrics")
            attempt = {
                "stage_id": stage_id,
                "attempt_id": int(stage.attemptId()),
                "status": str(stage.status()),
                "input_bytes": int(stage.inputBytes()),
                "input_records": int(stage.inputRecords()),
                "num_tasks": int(stage.numTasks()),
                "completed_tasks": int(stage.numCompleteTasks()),
                "failed_tasks": int(stage.numFailedTasks()),
                "killed_tasks": int(stage.numKilledTasks()),
            }
            _require_clean_attempt(attempt)
            attempts.append(attempt)
    if not attempts:
        raise MetricsUnavailableError("No new completed stage metrics for query jobs")
    return attempts


def collect_query_metrics(spark, df, group_id: str | None = None):
    """Return ``(collected_rows, metrics)`` for one synchronous collect action.

    Construct/analyze the physical plan before the measured region so Delta
    snapshot preparation is outside the scan benchmark. Caller must clear its
    Spark cache and use a freshly constructed, uncached query for each run.
    This helper does not flush OS caches. It restores caller job properties,
    including when execution or counter collection fails, and fails explicitly
    if Spark cannot provide trustworthy metrics instead of inventing values.
    Samples containing failed/retried/killed stages or tasks are rejected.
    """
    sc = spark.sparkContext
    group_id = group_id or f"d2-query-{uuid.uuid4().hex}"
    tracker = sc.statusTracker()
    if tracker.getJobIdsForGroup(group_id):
        raise ValueError(f"Measurement job group must be unique: {group_id}")
    df._jdf.queryExecution().executedPlan()
    listener_bus = sc._jsc.sc().listenerBus()
    listener_bus.waitUntilEmpty(10000)
    properties = {
        key: sc.getLocalProperty(key)
        for key in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel")
    }
    try:
        sc.setJobGroup(group_id, "D2 benchmark query", interruptOnCancel=True)
        started_ms = int(spark._jvm.java.lang.System.currentTimeMillis())
        started = time.perf_counter()
        rows = df.collect()
        elapsed_ms = (time.perf_counter() - started) * 1000
        # Task/SQL listener updates are asynchronous even after collect returns.
        # Flush outside the timed interval so counters are final and attributable.
        listener_bus.waitUntilEmpty(10000)
        job_ids = sorted(int(job_id) for job_id in tracker.getJobIdsForGroup(group_id))
        if not job_ids:
            raise MetricsUnavailableError("No Spark jobs recorded for query action")
        scans = _scan_metrics(spark, df._jdf.queryExecution().executedPlan())
        attempts = _stage_metrics(spark, job_ids, started_ms)
        return rows, {
            "execution_time_ms": elapsed_ms,
            "files_scanned": sum(scan["metrics"]["numFiles"] for scan in scans),
            "scan_file_bytes": sum(scan["metrics"]["filesSize"] for scan in scans),
            "bytes_read": sum(stage["input_bytes"] for stage in attempts),
            "job_group": group_id,
            "job_ids": job_ids,
            "stage_attempts": attempts,
            "scan_nodes": scans,
            "metric_provenance": {
                "spark_version": spark.version,
                "files_scanned": "Executed FileSourceScanExec SQLMetric numFiles",
                "scan_file_bytes": "Executed FileSourceScanExec SQLMetric filesSize",
                "bytes_read": "AppStatusStore StageData.inputBytes for unique, successful, unretried query stage attempts",
                "bytes_read_semantics": "Spark task filesystem input bytes; includes cached reads, not physical disk IO",
                "timing": "df.collect wall time; physical planning and metrics extraction excluded",
            },
        }
    finally:
        for key, value in properties.items():
            sc.setLocalProperty(key, value)
