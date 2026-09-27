#!/usr/bin/env python3
"""D2 — Performance lab on Silver (real TLC data, ~20M rows).

Variants (all built from the same Silver snapshot, unpartitioned so every
variant stores identical rows):
  1 baseline        Silver rewritten as many small, randomly ordered files
                    (the small-file state that streaming/MERGE ingestion leaves)
  2 compacted       baseline + OPTIMIZE (bin-packing only)
  3 zorder_1col     baseline + OPTIMIZE ZORDER BY (PULocationID)
  4 zorder_2col     baseline + OPTIMIZE ZORDER BY (PULocationID, tpep_pickup_datetime)
  5 liquid_cluster  CLUSTER BY (PULocationID, tpep_pickup_datetime) + OPTIMIZE FULL

Queries
  q1_zone        1-D filter: one pickup zone (132 = JFK Airport)
  q2_zone_week   2-D filter: zone 161 (Midtown Center) within one week
  q3_control     filter on payment_type only (NOT a clustering column):
                 clustering should not help here; if it does, suspect caching

Method
  * One discarded warm-up round, then N rounds. Inside every round the
    variants run in fixed interleaved order 1,2,3,4,5 (never all runs of one
    variant back to back), so JVM warm-up and OS cache drift spread evenly.
  * Each run: spark.catalog.clearCache(), a fresh DataFrame (new snapshot
    resolution), collect() of a tiny aggregate so result transfer is negligible.
  * Metrics per run: wall time, files scanned and bytes read (from the scan
    node's SQL metrics in the executed plan, i.e. *after* Delta data skipping).
  * Median over runs; improvement % = (baseline - variant) / baseline.
  * Results must be identical across variants (correctness check).

Usage
  python optimization_benchmark.py                 # build variants if missing, 5 rounds
  python optimization_benchmark.py --rounds 3 --rebuild --target-file-mb 32
"""

from __future__ import annotations

import argparse
import json
import shutil
import statistics
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import LakehousePaths, get_paths
from src.delta_utils import table_detail, table_exists, temporary_conf, write_json

VARIANTS = ["baseline", "compacted", "zorder_1col", "zorder_2col", "liquid_cluster"]
CLUSTER_COLUMNS = ("PULocationID", "tpep_pickup_datetime")


def summary_aggregate(df: DataFrame) -> DataFrame:
    return df.agg(F.count(F.lit(1)).alias("trips"), F.round(F.sum("fare_amount"), 2).alias("fare_sum"),
                  F.round(F.avg("tip_amount"), 6).alias("avg_tip"))


QUERIES: Dict[str, Tuple[str, Callable[[DataFrame], DataFrame]]] = {
    "q1_zone": ("PULocationID = 132 (JFK)",
                lambda df: summary_aggregate(df.filter(F.col("PULocationID") == 132))),
    "q2_zone_week": ("PULocationID = 161 AND pickup in 2025-02-10..16",
                     lambda df: summary_aggregate(df.filter(
                         (F.col("PULocationID") == 161)
                         & (F.col("tpep_pickup_datetime") >= F.lit("2025-02-10 00:00:00").cast("timestamp"))
                         & (F.col("tpep_pickup_datetime") < F.lit("2025-02-17 00:00:00").cast("timestamp"))))),
    "q3_control": ("payment_type = 2 (not clustered; control)",
                   lambda df: summary_aggregate(df.filter(F.col("payment_type") == 2))),
}


# ----------------------------------------------------------------------------
# Scan metrics from the executed physical plan (works with and without AQE)
# ----------------------------------------------------------------------------
def iter_plan_nodes(plan: Any):
    stack = [plan]
    while stack:
        node = stack.pop()
        yield node
        cls = node.getClass().getSimpleName()
        if cls == "AdaptiveSparkPlanExec":
            stack.append(node.executedPlan())
            continue
        if cls.endswith("QueryStageExec"):
            stack.append(node.plan())
            continue
        iterator = node.children().iterator()
        while iterator.hasNext():
            stack.append(iterator.next())


def collect_scan_metrics(df: DataFrame) -> Dict[str, int]:
    """Sum numFiles / filesSize over every file-scan node of an *executed* DataFrame."""
    plan = df._jdf.queryExecution().executedPlan()
    totals = {"files_scanned": 0, "bytes_read": 0, "scan_nodes": 0}
    for node in iter_plan_nodes(plan):
        if "FileSourceScan" not in node.getClass().getSimpleName():
            continue
        totals["scan_nodes"] += 1
        metrics = node.metrics()
        for key, target in (("numFiles", "files_scanned"), ("filesSize", "bytes_read")):
            option = metrics.get(key)
            if option.isDefined():
                totals[target] += int(option.get().value())
    return totals


def time_query(spark: SparkSession, path: str, query: Callable[[DataFrame], DataFrame], fmt: str = "delta") -> Dict[str, Any]:
    spark.catalog.clearCache()
    df = query(spark.read.format(fmt).load(path))
    start = time.perf_counter()
    rows = df.collect()
    elapsed_ms = (time.perf_counter() - start) * 1000
    return {"ms": round(elapsed_ms, 2), "result": [r.asDict() for r in rows], **collect_scan_metrics(df)}


# ----------------------------------------------------------------------------
# Variant preparation
# ----------------------------------------------------------------------------
def build_variants(spark: SparkSession, paths: LakehousePaths, baseline_files: int, target_file_mb: int,
                   rebuild: bool) -> Dict[str, Dict[str, Any]]:
    from delta import DeltaTable

    bench = paths.benchmark_dir
    variant_paths = {v: str(bench / v) for v in VARIANTS}
    if rebuild and bench.exists():
        shutil.rmtree(bench)
    info: Dict[str, Dict[str, Any]] = {}
    optimize_conf = {"spark.databricks.delta.optimize.maxFileSize": str(target_file_mb * 1024 * 1024)}

    if not table_exists(spark, variant_paths["baseline"]):
        silver = spark.read.format("delta").load(paths.silver_trips).drop("dq_flags")
        silver.repartition(baseline_files).write.format("delta").save(variant_paths["baseline"])
    info["baseline"] = {"prepared_by": f"repartition({baseline_files}) of Silver (random order)"}

    for variant, action in (
        ("compacted", lambda t: t.optimize().executeCompaction()),
        ("zorder_1col", lambda t: t.optimize().executeZOrderBy("PULocationID")),
        ("zorder_2col", lambda t: t.optimize().executeZOrderBy(*CLUSTER_COLUMNS)),
    ):
        path = variant_paths[variant]
        if not table_exists(spark, path):
            shutil.copytree(variant_paths["baseline"], path)
            before = table_detail(spark, path)
            with temporary_conf(spark, optimize_conf):
                metrics = action(DeltaTable.forPath(spark, path)).first()["metrics"].asDict()
            info[variant] = {"files_before": before["numFiles"],
                             "optimize_metrics": {k: metrics[k] for k in ("numFilesAdded", "numFilesRemoved") if k in metrics}}

    liquid = variant_paths["liquid_cluster"]
    if not table_exists(spark, liquid):
        source = spark.read.format("delta").load(variant_paths["baseline"])
        (DeltaTable.create(spark).location(liquid).addColumns(source.schema)
         .clusterBy(*CLUSTER_COLUMNS).execute())
        source.write.format("delta").mode("append").save(liquid)
        before = table_detail(spark, liquid)
        with temporary_conf(spark, optimize_conf):
            spark.sql(f"OPTIMIZE delta.`{liquid}` FULL")
        info["liquid_cluster"] = {"files_before": before["numFiles"], "prepared_by": "CLUSTER BY + OPTIMIZE FULL"}

    for variant in VARIANTS:
        detail = table_detail(spark, variant_paths[variant])
        info.setdefault(variant, {}).update({
            "path": variant_paths[variant], "files": detail["numFiles"],
            "size_mb": round(detail["sizeInBytes"] / 1024 / 1024, 1),
            "avg_file_mb": round(detail["sizeInBytes"] / max(detail["numFiles"], 1) / 1024 / 1024, 2),
            "clustering": detail.get("clusteringColumns"),
        })
    return info


# ----------------------------------------------------------------------------
# Measurement and reporting
# ----------------------------------------------------------------------------
def run_rounds(spark: SparkSession, variant_paths: Dict[str, str], rounds: int, fmt: str = "delta") -> List[Dict[str, Any]]:
    runs: List[Dict[str, Any]] = []
    for round_no in range(0, rounds + 1):  # round 0 = warm-up, discarded
        for query_name, (_, query) in QUERIES.items():
            for variant in variant_paths:  # interleaved 1,2,3,4,5
                measurement = time_query(spark, variant_paths[variant], query, fmt)
                runs.append({"round": round_no, "warmup": round_no == 0, "query": query_name,
                             "variant": variant, **measurement})
        print(f"[bench] round {round_no}{' (warm-up)' if round_no == 0 else ''} done")
    return runs


def summarize_runs(runs: List[Dict[str, Any]], variants: List[str]) -> Dict[str, Any]:
    measured = [r for r in runs if not r["warmup"]]
    summary: Dict[str, Any] = {}
    correctness: Dict[str, bool] = {}
    for query in QUERIES:
        per_variant = {}
        results = set()
        for variant in variants:
            sample = [r for r in measured if r["query"] == query and r["variant"] == variant]
            times = [r["ms"] for r in sample]
            per_variant[variant] = {
                "runs": len(times), "median_ms": round(statistics.median(times), 1),
                "min_ms": min(times), "max_ms": max(times),
                "files_scanned": int(statistics.median([r["files_scanned"] for r in sample])),
                "mb_read": round(statistics.median([r["bytes_read"] for r in sample]) / 1024 / 1024, 2),
            }
            results.add(json.dumps(sample[0]["result"], sort_keys=True))
        base = per_variant[variants[0]]["median_ms"]
        for variant in variants:
            per_variant[variant]["improvement_pct"] = round((base - per_variant[variant]["median_ms"]) / base * 100, 1)
        summary[query] = per_variant
        correctness[query] = len(results) == 1
    return {"per_query": summary, "results_identical_across_variants": correctness}


THREATS = """## Threats to validity

* **OS page cache.** Spark caches are cleared before every run, but the operating
  system's file cache cannot be dropped without root. After the warm-up round every
  variant is equally "warm", so the comparison is fair but absolute times are optimistic
  compared with cold cloud object storage. Files scanned and bytes read are unaffected
  by caching and are the primary evidence.
* **Local disk, one machine.** Object stores (S3/ADLS) add per-file request latency,
  which makes the small-file penalty *larger* than measured here.
* **Target file size.** OPTIMIZE's default 1 GB target would leave ~1 file for this
  dataset and make skipping impossible, so the target was scaled to the data size
  (`--target-file-mb`). Results depend on this choice.
* **Query selection.** Q1/Q2 filter on the clustering columns by design; Q3 is the
  control. Workloads filtering on other columns will not see these gains.
* **Snapshot/metadata caching.** Delta caches the transaction-log snapshot per JVM;
  planning time after the first run is lower for all variants alike.
* **Variance.** Medians over N interleaved rounds are reported with min/max; small
  differences (a few %) are within noise.
"""


def render_markdown(result: Dict[str, Any]) -> str:
    lines = ["# Optimization benchmark (D2)", "",
             f"Silver snapshot rows: {result['rows']:,} · rounds: {result['rounds']} (+1 warm-up) · "
             f"target file size: {result['target_file_mb']} MB · baseline files: {result['baseline_files']}", "",
             "## Table variants (files before/after)", "",
             "| Variant | Files before | Files after | Size MB | Avg file MB | How prepared |", "|---|---:|---:|---:|---:|---|"]
    for name, info in result["variants"].items():
        lines.append(f"| {name} | {info.get('files_before', info['files'])} | {info['files']} | {info['size_mb']} | "
                     f"{info['avg_file_mb']} | {info.get('prepared_by', 'OPTIMIZE')} |")
    for query, per_variant in result["summary"]["per_query"].items():
        lines += ["", f"## {query}: {QUERIES[query][0]}", "",
                  "| Variant | Median ms | Min | Max | Files scanned | MB read | Improvement vs baseline |",
                  "|---|---:|---:|---:|---:|---:|---:|"]
        for variant, m in per_variant.items():
            lines.append(f"| {variant} | {m['median_ms']} | {m['min_ms']} | {m['max_ms']} | {m['files_scanned']} | "
                         f"{m['mb_read']} | {m['improvement_pct']}% |")
    lines += ["", f"Results identical across variants: `{result['summary']['results_identical_across_variants']}`",
              "", THREATS]
    return "\n".join(lines) + "\n"


def run(spark: SparkSession, paths: Optional[LakehousePaths] = None, rounds: int = 5, baseline_files: int = 200,
        target_file_mb: int = 32, rebuild: bool = False) -> Dict[str, Any]:
    paths = paths or get_paths()
    if rounds < 3:
        raise ValueError("Use at least 3 measured rounds")
    variants = build_variants(spark, paths, baseline_files, target_file_mb, rebuild)
    variant_paths = {v: variants[v]["path"] for v in VARIANTS}
    with temporary_conf(spark, {"spark.sql.shuffle.partitions": "8"}):
        runs = run_rounds(spark, variant_paths, rounds)
    result = {
        "rows": spark.read.format("delta").load(variant_paths["baseline"]).count(),
        "rounds": rounds, "baseline_files": baseline_files, "target_file_mb": target_file_mb,
        "variants": variants, "summary": summarize_runs(runs, VARIANTS), "runs": runs,
    }
    out_dir = paths.docs_dir / "benchmark"
    write_json(out_dir / "benchmark_results.json", result)
    (out_dir / "benchmark_results.md").write_text(render_markdown(result), encoding="utf-8")
    return {k: result[k] for k in ("rows", "rounds", "variants", "summary")}


def main() -> None:
    from src.spark_session import create_spark

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--baseline-files", type=int, default=200)
    parser.add_argument("--target-file-mb", type=int, default=32)
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args()
    spark = create_spark("optimization-benchmark")
    try:
        result = run(spark, rounds=args.rounds, baseline_files=args.baseline_files,
                     target_file_mb=args.target_file_mb, rebuild=args.rebuild)
        print(json.dumps(result["summary"], indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
