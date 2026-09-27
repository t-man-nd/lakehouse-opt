"""Silver layer for the real TLC lane: typed, keyed, deduplicated, DQ-flagged.

Processing model (incremental, like a real ingestion service)
---------------------------------------------------------------
For every Bronze batch not yet in `ops/silver_batch_log`, in commit order:

1. Read the batch *at the Bronze version that committed it* (time travel).
   The batch therefore keeps its original schema: a 2024 batch has no
   cbd_congestion_fee even though today's Bronze schema does.
2. Classify with TLC-v2 (src/dq_rules.py): hard failures -> quarantine,
   soft issues -> dq_flags, in-batch duplicates -> quarantine.
3. Plan the MERGE by joining the batch keys to Silver: count expected
   inserts, real updates (content changed) and no-ops (identical resubmission).
4. MERGE INTO Silver ON (pickup_month, trip_key):
      WHEN MATCHED AND row_hash differs -> UPDATE *
      WHEN NOT MATCHED                  -> INSERT *
   with schema evolution enabled, then check Delta's operationMetrics against
   the plan.
5. Record counts in the batch log. Row conservation per batch:
      bronze_rows = rejected + inserted + updated + noop

Late arrivals (a February file containing January pickups) are inserted
into their true `pickup_month` partition; resubmissions of an existing trip
become updates or no-ops. That is CDC coming from the data itself.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.config import DQThresholds, LakehousePaths, get_paths
from src.delta_utils import (
    commit_metadata, find_schema_changes, last_operation_metrics, latest_version,
    table_exists, temporary_conf, write_json,
)
from src.dq_rules import HARD_REASONS, classify_trips, to_rejected_rows, to_silver_rows
from src.tlc_bronze import append_batch, find_batch_versions

BATCH_LOG_SCHEMA = (
    "batch_id string, source_month string, bronze_version bigint, bronze_rows bigint, "
    "accepted_rows bigint, rejected_rows bigint, expected_inserts bigint, expected_updates bigint, "
    "expected_noop bigint, inserted bigint, updated bigint, silver_version bigint, "
    "reject_reason_counts string, flag_counts string, processed_ts timestamp"
)
MERGE_CONDITION = "t.pickup_month = s.pickup_month AND t.trip_key = s.trip_key"


# ----------------------------------------------------------------------------
# Batch bookkeeping
# ----------------------------------------------------------------------------
def read_batch_log(spark: SparkSession, paths: LakehousePaths) -> List[Dict[str, Any]]:
    if not table_exists(spark, paths.silver_batch_log):
        return []
    rows = spark.read.format("delta").load(paths.silver_batch_log).orderBy("bronze_version").collect()
    return [r.asDict() for r in rows]


def list_pending_batches(spark: SparkSession, paths: LakehousePaths) -> List[Tuple[str, int]]:
    """Bronze batches not yet processed, ordered by the Bronze version that added them."""
    done = {r["batch_id"] for r in read_batch_log(spark, paths)}
    versions = find_batch_versions(spark, paths.bronze_trips)
    return sorted(((b, v) for b, v in versions.items() if b not in done), key=lambda item: item[1])


def append_batch_log(spark: SparkSession, paths: LakehousePaths, record: Dict[str, Any]) -> None:
    row = spark.createDataFrame([record], BATCH_LOG_SCHEMA)
    row.write.format("delta").mode("append").save(paths.silver_batch_log)


# ----------------------------------------------------------------------------
# MERGE planning and execution
# ----------------------------------------------------------------------------
def plan_merge(spark: SparkSession, accepted: DataFrame, silver_path: str) -> Dict[str, int]:
    """Count inserts / updates / no-ops *before* merging, from a plain join."""
    source = accepted.select("trip_key", "pickup_month", F.col("row_hash").alias("s_hash"))
    if not table_exists(spark, silver_path):
        return {"inserts": source.count(), "updates": 0, "noop": 0}
    target = spark.read.format("delta").load(silver_path).select(
        "trip_key", "pickup_month", F.col("row_hash").alias("t_hash"))
    joined = source.join(target, ["trip_key", "pickup_month"], "left")
    row = joined.agg(
        F.sum(F.when(F.col("t_hash").isNull(), 1).otherwise(0)).alias("inserts"),
        F.sum(F.when(F.col("t_hash").isNotNull() & (F.col("t_hash") != F.col("s_hash")), 1).otherwise(0)).alias("updates"),
        F.sum(F.when(F.col("t_hash") == F.col("s_hash"), 1).otherwise(0)).alias("noop"),
    ).first()
    return {k: int(row[k] or 0) for k in ("inserts", "updates", "noop")}


def merge_into_silver(spark: SparkSession, accepted: DataFrame, silver_path: str, meta: Dict[str, Any]) -> Dict[str, int]:
    """Create Silver on the first batch, MERGE afterwards. Returns inserted/updated counts."""
    from delta import DeltaTable

    with commit_metadata(spark, meta):
        if not table_exists(spark, silver_path):
            accepted.write.format("delta").partitionBy("pickup_month").save(silver_path)
            metrics = last_operation_metrics(spark, silver_path)
            return {"inserted": metrics.get("numOutputRows", accepted.count()), "updated": 0,
                    "version": metrics["version"]}
        builder = (
            DeltaTable.forPath(spark, silver_path).alias("t")
            .merge(accepted.alias("s"), MERGE_CONDITION)
            .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash")
            .whenNotMatchedInsertAll()
        )
        if hasattr(builder, "withSchemaEvolution"):  # Delta >= 3.2
            builder = builder.withSchemaEvolution()
        with temporary_conf(spark, {"spark.databricks.delta.schema.autoMerge.enabled": "true"}):
            builder.execute()
    metrics = last_operation_metrics(spark, silver_path)
    return {"inserted": metrics.get("numTargetRowsInserted", -1),
            "updated": metrics.get("numTargetRowsUpdated", -1),
            "version": metrics["version"]}


def write_rejected(spark: SparkSession, rejected: DataFrame, path: str, batch_id: str, meta: Dict[str, Any]) -> None:
    """Idempotent per batch: delete this batch's old quarantine rows, then append."""
    from delta import DeltaTable

    with commit_metadata(spark, meta):
        if table_exists(spark, path):
            DeltaTable.forPath(spark, path).delete(F.col("_batch_id") == batch_id)
        (rejected.write.format("delta").mode("append").option("mergeSchema", "true")
         .partitionBy("_source_month").save(path))


def count_by(df: DataFrame, column: str) -> Dict[str, int]:
    return {r[column]: r["count"] for r in df.groupBy(column).count().collect()}


def process_batch(
    spark: SparkSession, paths: LakehousePaths, batch_id: str, bronze_version: int, th: DQThresholds,
) -> Dict[str, Any]:
    bronze = (spark.read.format("delta").option("versionAsOf", bronze_version).load(paths.bronze_trips)
              .filter(F.col("_batch_id") == batch_id))
    classified = classify_trips(bronze, th).persist()
    accepted: Optional[DataFrame] = None
    try:
        bronze_rows = classified.count()
        accepted = to_silver_rows(classified).persist()
        rejected = to_rejected_rows(classified, bronze.columns)
        reason_counts = count_by(classified.filter("reject_reason IS NOT NULL"), "reject_reason")
        flag_counts = count_by(accepted.select(F.explode("dq_flags").alias("flag")), "flag")
        accepted_rows = accepted.count()
        rejected_rows = sum(reason_counts.values())
        if accepted_rows + rejected_rows != bronze_rows:
            raise RuntimeError(f"{batch_id}: accepted {accepted_rows} + rejected {rejected_rows} != bronze {bronze_rows}")
        source_month = bronze.select("_source_month").first()["_source_month"]
        plan = plan_merge(spark, accepted, paths.silver_trips)
        meta = {"layer": "silver", "batch_id": batch_id, "bronze_version": bronze_version}
        write_rejected(spark, rejected, paths.silver_rejected, batch_id, {**meta, "table": "rejected"})
        merged = merge_into_silver(spark, accepted, paths.silver_trips, meta)
    finally:
        classified.unpersist()
        if accepted is not None:
            accepted.unpersist()

    if merged["inserted"] >= 0 and merged["inserted"] != plan["inserts"]:
        raise RuntimeError(f"{batch_id}: MERGE inserted {merged['inserted']} but plan expected {plan['inserts']}")
    if merged["updated"] >= 0 and merged["updated"] != plan["updates"]:
        raise RuntimeError(f"{batch_id}: MERGE updated {merged['updated']} but plan expected {plan['updates']}")
    record = {
        "batch_id": batch_id, "source_month": source_month, "bronze_version": bronze_version,
        "bronze_rows": bronze_rows, "accepted_rows": accepted_rows, "rejected_rows": rejected_rows,
        "expected_inserts": plan["inserts"], "expected_updates": plan["updates"], "expected_noop": plan["noop"],
        "inserted": merged["inserted"], "updated": merged["updated"], "silver_version": merged["version"],
        "reject_reason_counts": json.dumps(dict(sorted(reason_counts.items()))),
        "flag_counts": json.dumps(dict(sorted(flag_counts.items()))),
        "processed_ts": datetime.now(timezone.utc),
    }
    append_batch_log(spark, paths, record)
    print(f"[silver] {batch_id}: bronze={bronze_rows:,} rejected={rejected_rows:,} "
          f"insert={plan['inserts']:,} update={plan['updates']:,} noop={plan['noop']:,} -> v{merged['version']}")
    return record


def summarize(spark: SparkSession, paths: LakehousePaths) -> Dict[str, Any]:
    log = read_batch_log(spark, paths)
    silver = spark.read.format("delta").load(paths.silver_trips)
    rejected = spark.read.format("delta").load(paths.silver_rejected)
    reasons: Dict[str, int] = {r: 0 for r in HARD_REASONS}
    for entry in log:
        for reason, count in json.loads(entry["reject_reason_counts"]).items():
            reasons[reason] = reasons.get(reason, 0) + count
    totals = {k: sum(int(e[k]) for e in log) for k in
              ("bronze_rows", "accepted_rows", "rejected_rows", "expected_inserts", "expected_updates", "expected_noop")}
    silver_count = silver.count()
    rejected_count = rejected.count()
    bronze_count = spark.read.format("delta").load(paths.bronze_trips).count()
    conservation = {
        "bronze_rows_processed == bronze_count": totals["bronze_rows"] == bronze_count,
        "bronze == rejected + inserts + updates + noop": totals["bronze_rows"] == (
            totals["rejected_rows"] + totals["expected_inserts"] + totals["expected_updates"] + totals["expected_noop"]),
        "silver_count == total inserts": silver_count == totals["expected_inserts"],
        "rejected_count == total rejected": rejected_count == totals["rejected_rows"],
        "trip_key unique in silver": silver.select("trip_key").distinct().count() == silver_count,
        "every rejected row has a reason": rejected.filter("reject_reason IS NULL OR reject_reason = ''").count() == 0,
    }
    return {
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "rejected_count": rejected_count,
        "totals": totals,
        "reject_reason_counts": {k: v for k, v in reasons.items() if v},
        "late_arrivals_in_silver": silver.filter("is_late_arrival").count(),
        "silver_version": latest_version(spark, paths.silver_trips),
        "batches": [{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in e.items()} for e in log],
        "conservation": conservation,
        "conservation_ok": all(conservation.values()),
    }


def build(
    spark: SparkSession,
    paths: Optional[LakehousePaths] = None,
    thresholds: DQThresholds = DQThresholds(),
    rebuild: bool = False,
) -> Dict[str, Any]:
    """Process every pending Bronze batch into Silver. Safe to call repeatedly."""
    paths = paths or get_paths()
    if rebuild:
        for path in (paths.silver_trips, paths.silver_rejected, paths.silver_batch_log):
            if Path(path).exists():
                shutil.rmtree(path)
    pending = list_pending_batches(spark, paths)
    if not pending:
        print("[silver] no pending batches")
    processed = [process_batch(spark, paths, batch_id, version, thresholds)["batch_id"] for batch_id, version in pending]
    summary = summarize(spark, paths)
    summary["processed_this_run"] = processed
    if not summary["conservation_ok"]:
        raise RuntimeError(f"Silver conservation checks failed: {summary['conservation']}")
    return summary


# ----------------------------------------------------------------------------
# C1: CDC correction feed (labelled synthetic, built from real Silver keys)
# ----------------------------------------------------------------------------
LANDING_CAST = {
    "VendorID": "bigint", "passenger_count": "bigint", "RatecodeID": "bigint",
    "PULocationID": "bigint", "DOLocationID": "bigint", "payment_type": "bigint",
}


def build_correction_feed(silver: DataFrame, seed: int, n_updates: int, n_inserts: int, target_month: str) -> DataFrame:
    """Deterministic vendor-adjustment feed.

    * n_updates real trips (same trip_key): tip +2.00; every second one also
      gets fare +1.50; total_amount moves by the same delta. Only non-key
      columns change, so these must MERGE as UPDATEs.
    * n_inserts late-arriving trips: copies of other real trips shifted by
      +3 minutes (new identity), so they must MERGE as INSERTs.
    """
    base_cols = [c for c in (
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
        "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount",
        "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
        "congestion_surcharge", "Airport_fee", "cbd_congestion_fee",
    ) if c in silver.columns]
    eligible = (
        silver.filter((F.col("pickup_month") == target_month) & (F.col("payment_type") == 1)
                      & (F.size("dq_flags") == 0) & (F.dayofmonth("tpep_pickup_datetime") <= 27))
        .withColumn("_rank_key", F.sha2(F.concat_ws("|", "trip_key", F.lit(str(seed))), 256))
        .orderBy("_rank_key")
        .limit(n_updates + n_inserts)
        .withColumn("_pos", F.row_number().over(Window.orderBy("_rank_key")))
    )
    updates = eligible.filter(F.col("_pos") <= n_updates)
    fare_delta = F.when(F.col("_pos") % 2 == 0, F.lit(1.50)).otherwise(F.lit(0.0))
    tip_delta = F.lit(2.00)
    updates = (
        updates.withColumn("fare_amount", F.round(F.col("fare_amount") + fare_delta, 2))
        .withColumn("tip_amount", F.round(F.col("tip_amount") + tip_delta, 2))
        .withColumn("total_amount", F.round(F.col("total_amount") + fare_delta + tip_delta, 2))
    )
    inserts = (
        eligible.filter(F.col("_pos") > n_updates)
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime") + F.expr("INTERVAL 3 MINUTES"))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime") + F.expr("INTERVAL 3 MINUTES"))
    )
    feed = updates.unionByName(inserts).select(*base_cols)
    for name, dtype in LANDING_CAST.items():
        if name in feed.columns:
            feed = feed.withColumn(name, F.col(name).cast(dtype))
    return feed


def apply_cdc(
    spark: SparkSession,
    paths: Optional[LakehousePaths] = None,
    seed: int = 42,
    n_updates: int = 25,
    n_inserts: int = 5,
    target_month: Optional[str] = None,
    thresholds: DQThresholds = DQThresholds(),
) -> Dict[str, Any]:
    """Land a correction feed in Bronze and let the normal Silver MERGE apply it."""
    paths = paths or get_paths()
    batch_id = f"cdc_corrections_seed{seed}"
    log_before = {e["batch_id"]: e for e in read_batch_log(spark, paths)}
    silver_version_before = latest_version(spark, paths.silver_trips)
    silver_before = spark.read.format("delta").option("versionAsOf", silver_version_before).load(paths.silver_trips)
    count_before = silver_before.count()

    if batch_id in log_before:
        entry = log_before[batch_id]
        print(f"[cdc] {batch_id} already applied at silver v{entry['silver_version']}; nothing to do")
        return {"batch_id": batch_id, "already_applied": True, "silver_version": entry["silver_version"],
                "updated": entry["updated"], "inserted": entry["inserted"]}

    if target_month is None:
        # The latest *source* month, not the latest pickup month: files contain a few trips that
        # start just after month end (flagged AFTER_SOURCE_MONTH), e.g. 2025-04-01 00:05 in March.
        months = [e["source_month"] for e in log_before.values() if e["batch_id"].startswith("yellow_")]
        target_month = max(months) if months else silver_before.agg(F.max("pickup_month")).first()[0]
    feed = build_correction_feed(silver_before, seed, n_updates, n_inserts, target_month)
    feed_rows = feed.count()
    if feed_rows != n_updates + n_inserts:
        raise RuntimeError(f"Correction feed has {feed_rows} rows, expected {n_updates + n_inserts}; "
                           f"not enough eligible trips in pickup_month {target_month}")
    feed_path = paths.raw_cdc_dir / f"{batch_id}.parquet"
    if feed_path.exists():
        shutil.rmtree(feed_path) if feed_path.is_dir() else feed_path.unlink()
    feed.coalesce(1).write.mode("overwrite").parquet(str(feed_path))
    digest = hashlib.sha256()
    for part in sorted(Path(feed_path).glob("*.parquet")):
        digest.update(part.read_bytes())
    feed_df = spark.read.parquet(str(feed_path))
    if batch_id not in find_batch_versions(spark, paths.bronze_trips):
        append_batch(spark, feed_df, paths.bronze_trips, batch_id, target_month,
                     f"synthetic://{feed_path.name}", digest.hexdigest(), n_updates + n_inserts)

    build(spark, paths, thresholds)
    entry = {e["batch_id"]: e for e in read_batch_log(spark, paths)}[batch_id]
    count_after = spark.read.format("delta").load(paths.silver_trips).count()
    checks = {
        "matched rows were updated (updated > 0)": entry["updated"] > 0,
        "updated == n_updates": entry["updated"] == n_updates,
        "inserted == n_inserts": entry["inserted"] == n_inserts,
        "after == before + inserted": count_after == count_before + entry["inserted"],
    }

    # Before/after evidence for a few corrected trips (time travel on Silver).
    keys = [r["trip_key"] for r in spark.read.format("delta").load(paths.silver_trips)
            .filter(F.col("_batch_id") == batch_id).select("trip_key").limit(5).collect()]
    cols = ["trip_key", "fare_amount", "tip_amount", "total_amount", "_batch_id"]
    before_rows = {r["trip_key"]: r.asDict() for r in silver_before.filter(F.col("trip_key").isin(keys)).select(cols).collect()}
    after_rows = {r["trip_key"]: r.asDict() for r in spark.read.format("delta").load(paths.silver_trips)
                  .filter(F.col("trip_key").isin(keys)).select(cols).collect()}
    samples = [{"trip_key": k, "before": before_rows.get(k), "after": after_rows.get(k)} for k in keys]

    monthly = [e for e in read_batch_log(spark, paths) if e["batch_id"].startswith("yellow_")]
    result = {
        "batch_id": batch_id, "seed": seed, "target_month": target_month, "feed_file": str(feed_path),
        "silver_version_before": silver_version_before, "silver_version_after": entry["silver_version"],
        "count_before": count_before, "count_after": count_after,
        "planned": {"inserts": entry["expected_inserts"], "updates": entry["expected_updates"], "noop": entry["expected_noop"]},
        "merge_metrics": {"inserted": entry["inserted"], "updated": entry["updated"]},
        "checks": checks, "checks_ok": all(checks.values()), "samples": samples,
        "real_cdc_from_monthly_files": {
            "cross_file_updates": sum(int(e["expected_updates"]) for e in monthly),
            "cross_file_noop_resubmissions": sum(int(e["expected_noop"]) for e in monthly),
            "late_arrivals_inserted": spark.read.format("delta").load(paths.silver_trips).filter("is_late_arrival").count(),
        },
    }
    write_json(paths.evidence_dir / "cdc_merge.json", result)
    if not result["checks_ok"]:
        raise RuntimeError(f"CDC checks failed: {checks}")
    return result


# ----------------------------------------------------------------------------
# C2: schema evolution evidence (the evolution itself happens during ingest)
# ----------------------------------------------------------------------------
def apply_evolution(spark: SparkSession, paths: Optional[LakehousePaths] = None,
                    column: str = "cbd_congestion_fee") -> Dict[str, Any]:
    """Verify that `column` was added by evolution, not by rebuilding tables."""
    paths = paths or get_paths()
    result: Dict[str, Any] = {"column": column}
    for name, path in (("bronze", paths.bronze_trips), ("silver", paths.silver_trips),
                       ("silver_rejected", paths.silver_rejected)):
        changes = find_schema_changes(path)
        event = next((c for c in changes if c["event"] == "SCHEMA_CHANGE" and column in c.get("added", [])), None)
        entry: Dict[str, Any] = {"schema_changes": changes, "evolution_event": event}
        if event:
            version = event["version"]
            before = spark.read.format("delta").option("versionAsOf", version - 1).load(path)
            after = spark.read.format("delta").option("versionAsOf", version).load(path)
            entry["column_absent_before"] = column not in before.columns
            entry["column_present_after"] = column in after.columns
            entry["operation"] = event["operation"]
            entry["not_a_rebuild"] = event["operation"] not in ("CREATE OR REPLACE TABLE AS SELECT", "REPLACE TABLE AS SELECT") and not any(
                c["event"] == "CREATE" and c["version"] > 0 for c in changes)
        result[name] = entry

    if Path(paths.silver_trips).exists():
        silver = spark.read.format("delta").load(paths.silver_trips)
        if column in silver.columns:
            result["silver_null_check"] = {
                r["_source_month"]: {"rows": r["rows"], "null": r["nulls"]}
                for r in silver.groupBy("_source_month").agg(
                    F.count(F.lit(1)).alias("rows"),
                    F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias("nulls"),
                ).collect()
            }
            old_rows = silver.filter(F.col("_source_month") < "2025-01")
            result["old_rows_null"] = old_rows.filter(F.col(column).isNotNull()).count() == 0
    ok = all(result.get(n, {}).get("evolution_event") for n in ("bronze", "silver"))
    result["status"] = "SCHEMA_EVOLVED" if ok else "NO_SCHEMA_CHANGE_OBSERVED"
    write_json(paths.evidence_dir / "schema_evolution.json", result)
    return result
