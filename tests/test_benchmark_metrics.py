"""Real file-scan checks for benchmark metrics, isolated from project data."""

import pytest

from src.benchmark_metrics import (
    MetricsUnavailableError, _require_clean_attempt, collect_query_metrics,
)
from src.delta_runtime import get_spark


@pytest.mark.parametrize("change", [
    {"status": "FAILED"}, {"status": "ACTIVE"}, {"attempt_id": 1},
    {"failed_tasks": 1}, {"killed_tasks": 1}, {"completed_tasks": 1},
    {"completed_tasks": 3},
])
def test_retry_failure_and_incomplete_samples_are_rejected(change):
    attempt = {"stage_id": 1, "attempt_id": 0, "status": "COMPLETE", "num_tasks": 2,
               "completed_tasks": 2, "failed_tasks": 0, "killed_tasks": 0}
    _require_clean_attempt(attempt)
    with pytest.raises(MetricsUnavailableError, match="failed, retried, killed or incomplete"):
        _require_clean_attempt({**attempt, **change})


@pytest.fixture(scope="module")
def spark_metrics():
    # Load Delta at the first JVM launch so later module fixtures can use it.
    session = get_spark('local[2]', 'D2MetricsTests')
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.mark.parametrize("adaptive", [False, True])
def test_executed_scan_counters_and_query_job_scope(spark_metrics, tmp_path, adaptive):
    spark = spark_metrics
    spark.conf.set("spark.sql.adaptive.enabled", str(adaptive).lower())
    path = tmp_path / "parquet"
    spark.range(256).selectExpr("id", "id * 3 AS amount").repartition(4).write.parquet(str(path))
    stored_bytes = sum(file.stat().st_size for file in path.glob("*.parquet"))
    query = lambda: spark.read.parquet(str(path)).where("id < 64").selectExpr(
        "COUNT(*) AS n", "SUM(amount) AS amount"
    )

    # Setup work and an earlier query must not leak into this query's bytes.
    _, earlier = collect_query_metrics(spark, query())
    sc = spark.sparkContext
    sc.setJobGroup("caller-group", "caller description", interruptOnCancel=False)
    rows, measured = collect_query_metrics(spark, query())
    assert rows[0].asDict() == {"n": 64, "amount": sum(i * 3 for i in range(64))}
    assert measured["files_scanned"] == 4
    assert measured["scan_file_bytes"] == stored_bytes
    assert measured["bytes_read"] > 0
    assert measured["bytes_read"] == sum(s["input_bytes"] for s in measured["stage_attempts"])
    assert measured["execution_time_ms"] > 0
    assert set(measured["job_ids"]).isdisjoint(earlier["job_ids"])
    assert sorted(sc.statusTracker().getJobIdsForGroup(measured["job_group"])) == measured["job_ids"]
    attempts = [(s["stage_id"], s["attempt_id"]) for s in measured["stage_attempts"]]
    assert len(attempts) == len(set(attempts))
    assert sc.getLocalProperty("spark.jobGroup.id") == "caller-group"
    assert sc.getLocalProperty("spark.job.description") == "caller description"
    assert sc.getLocalProperty("spark.job.interruptOnCancel") == "false"
    for key in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel"):
        sc.setLocalProperty(key, None)

    with pytest.raises(ValueError, match="must be unique"):
        collect_query_metrics(spark, query(), measured["job_group"])


def test_missing_file_scan_is_explicit_and_restores_caller_properties(spark_metrics):
    sc = spark_metrics.sparkContext
    sc.setJobGroup("outer", "restore on failure", interruptOnCancel=False)
    with pytest.raises(MetricsUnavailableError, match="No executed FileSourceScanExec"):
        collect_query_metrics(spark_metrics, spark_metrics.range(8).selectExpr("SUM(id) AS total"))
    assert sc.getLocalProperty("spark.jobGroup.id") == "outer"
    assert sc.getLocalProperty("spark.job.description") == "restore on failure"
    assert sc.getLocalProperty("spark.job.interruptOnCancel") == "false"
    for key in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel"):
        sc.setLocalProperty(key, None)
