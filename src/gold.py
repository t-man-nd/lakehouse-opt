"""Gold layer: question-driven marts rebuilt from Silver in one command.

Tables
------
zone_hourly_metrics   pickup zone x date x hour: trips, fares, tip %, driver earnings (rubric D1)
cbd_flow_monthly      trips by CBD flow (into / out of / within / non-CBD), split at the
                      congestion-pricing start date, with fee coverage
cbd_flow_yoy          2025 vs 2024 per calendar day, per flow, and relative to non-CBD trips
dq_monthly            reject and flag rates per source month (data quality as a product)

Metric definitions (agreed in the task plan)
--------------------------------------------
tip %            AVG(tip_amount / fare_amount) per trip, only where fare_amount > 0.
                 Reported twice: over all trips (plan definition) and over card trips
                 only, because TLC records tips for credit-card payments only and cash
                 tips are absent from the data.
driver_earnings  fare_amount + tip_amount + extra. Tolls, MTA tax, improvement and
                 congestion surcharges, airport and CBD fees are pass-through charges.
Exclusions       rows flagged REVERSED, IMPLAUSIBLE_SPEED or EXTREME_DISTANCE are kept
                 in Silver but left out of Gold (counted in dq_monthly).

All timestamps are NYC local wall-clock values (session time zone pinned to UTC
so Spark never shifts them); `pickup_hour` is therefore the local hour.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as F

from src.config import CBD_POLICY_START, DEFAULT_MONTHS, GOLD_EXCLUDED_FLAGS, LakehousePaths, get_paths
from src.delta_utils import commit_metadata, latest_version, write_json


# ----------------------------------------------------------------------------
# Pure transformations (unit-tested without Delta)
# ----------------------------------------------------------------------------
def prepare_gold_base(
    silver: DataFrame,
    dim_zone: DataFrame,
    months: Iterable[str],
    excluded_flags: Iterable[str] = GOLD_EXCLUDED_FLAGS,
    policy_start: str = CBD_POLICY_START,
) -> DataFrame:
    """Silver rows in scope for analysis, enriched with zones, CBD flow and metrics."""
    excluded = list(excluded_flags)
    availability = dim_zone.agg(F.max(F.col("cbd_reference_available").cast("int")).alias("a")).first()["a"]
    cbd_available = bool(availability)
    pu = dim_zone.select(
        F.col("LocationID").alias("PULocationID"), F.col("zone").alias("pu_zone"),
        F.col("borough").alias("pu_borough"), F.col("is_cbd").alias("pu_is_cbd"),
        F.col("is_unknown").alias("pu_is_unknown"))
    do = dim_zone.select(
        F.col("LocationID").alias("DOLocationID"), F.col("is_cbd").alias("do_is_cbd"),
        F.col("is_unknown").alias("do_is_unknown"))
    base = (
        silver.filter(F.col("pickup_month").isin(list(months)))
        .filter(~F.arrays_overlap("dq_flags", F.array(*[F.lit(f) for f in excluded])))
        .join(F.broadcast(pu), "PULocationID", "left")
        .join(F.broadcast(do), "DOLocationID", "left")
    )
    unknown = (F.coalesce("pu_is_unknown", F.lit(True)) | F.coalesce("do_is_unknown", F.lit(True)))
    pu_cbd, do_cbd = F.coalesce("pu_is_cbd", F.lit(False)), F.coalesce("do_is_cbd", F.lit(False))
    cbd_fee = F.col("cbd_congestion_fee") if "cbd_congestion_fee" in silver.columns else F.lit(None).cast("double")
    return (
        base.withColumn(
            "cbd_flow",
            F.when(F.lit(not cbd_available), "CBD_UNAVAILABLE")
            .when(unknown, "UNKNOWN_ZONE")
            .when(pu_cbd & do_cbd, "WITHIN_CBD")
            .when(~pu_cbd & do_cbd, "INTO_CBD")
            .when(pu_cbd & ~do_cbd, "OUT_OF_CBD")
            .otherwise("NON_CBD"))
        .withColumn("policy_period",
                    F.when(F.col("pickup_date") >= F.to_date(F.lit(policy_start)), "post_policy").otherwise("pre_policy"))
        .withColumn("tip_ratio", F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount")))
        .withColumn("card_tip_ratio", F.when((F.col("payment_type") == 1) & (F.col("fare_amount") > 0),
                                             F.col("tip_amount") / F.col("fare_amount")))
        .withColumn("driver_earnings", F.col("fare_amount") + F.coalesce("tip_amount", F.lit(0.0))
                    + F.coalesce("extra", F.lit(0.0)))
        .withColumn("cbd_fee", cbd_fee)
        .withColumn("duration_min", F.col("trip_duration_sec") / 60.0)
    )


def build_zone_hourly(base: DataFrame) -> DataFrame:
    return (
        base.groupBy("pickup_month", "pickup_date", "pickup_hour", "PULocationID", "pu_zone", "pu_borough")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.sum(F.when(F.col("payment_type") == 1, 1).otherwise(0)).alias("card_trips"),
            F.round(F.avg("fare_amount"), 4).alias("avg_fare"),
            F.round(F.avg("tip_ratio") * 100, 4).alias("avg_tip_pct"),
            F.round(F.avg("card_tip_ratio") * 100, 4).alias("avg_tip_pct_card"),
            F.round(F.sum("driver_earnings"), 2).alias("total_driver_earnings"),
            F.round(F.avg("driver_earnings"), 4).alias("avg_driver_earnings"),
            F.round(F.avg("duration_min"), 4).alias("avg_duration_min"),
            F.round(F.avg("trip_distance"), 4).alias("avg_distance_mi"),
            F.round(F.sum(F.coalesce("cbd_fee", F.lit(0.0))), 2).alias("total_cbd_fee"),
        )
    )


def period_days_expr(policy_start: str = CBD_POLICY_START) -> F.Column:
    """Calendar days covered by (pickup_month, policy_period); normalises trips per day."""
    month_start = F.to_date(F.concat(F.col("pickup_month"), F.lit("-01")))
    month_end = F.last_day(month_start)
    policy = F.to_date(F.lit(policy_start))
    post_start = F.greatest(month_start, policy)
    pre_end = F.least(month_end, F.date_sub(policy, 1))
    days = F.when(F.col("policy_period") == "post_policy", F.datediff(month_end, post_start) + 1) \
            .otherwise(F.datediff(pre_end, month_start) + 1)
    return F.greatest(days, F.lit(0))


def build_cbd_monthly(base: DataFrame, policy_start: str = CBD_POLICY_START) -> DataFrame:
    grouped = base.groupBy("pickup_month", "policy_period", "cbd_flow").agg(
        F.count(F.lit(1)).alias("trips"),
        F.round(F.avg("fare_amount"), 4).alias("avg_fare"),
        F.round(F.avg("duration_min"), 4).alias("avg_duration_min"),
        F.round(F.sum("trip_distance") / (F.sum("trip_duration_sec") / 3600.0), 4).alias("avg_speed_mph"),
        F.round(F.avg(F.when(F.col("cbd_fee") > 0, 1.0).otherwise(0.0)), 6).alias("share_with_cbd_fee"),
        F.round(F.avg(F.when(F.col("cbd_fee") > 0, F.col("cbd_fee"))), 4).alias("avg_cbd_fee_when_charged"),
        F.round(F.sum(F.coalesce("cbd_fee", F.lit(0.0))), 2).alias("total_cbd_fee"),
    )
    return (
        grouped.withColumn("period_days", period_days_expr(policy_start))
        .withColumn("trips_per_day", F.round(F.col("trips") / F.col("period_days"), 4))
        .withColumn("year", F.substring("pickup_month", 1, 4).cast("int"))
        .withColumn("month_of_year", F.substring("pickup_month", 6, 2).cast("int"))
    )


def build_cbd_yoy(cbd_monthly: DataFrame) -> DataFrame:
    """Year-over-year per calendar day (handles Feb 2024 = 29 days vs Feb 2025 = 28)."""
    monthly = cbd_monthly.groupBy("year", "month_of_year", "cbd_flow").agg(
        F.sum("trips").alias("trips"), F.sum("period_days").alias("days"),
        (F.sum(F.col("avg_duration_min") * F.col("trips")) / F.sum("trips")).alias("avg_duration_min"),
    ).withColumn("trips_per_day", F.col("trips") / F.col("days"))
    prev = monthly.select(
        (F.col("year") + 1).alias("year"), "month_of_year", "cbd_flow",
        F.col("trips_per_day").alias("trips_per_day_prev"),
        F.col("avg_duration_min").alias("avg_duration_min_prev"))
    yoy = (
        monthly.join(prev, ["year", "month_of_year", "cbd_flow"], "inner")
        .withColumn("yoy_trips_per_day_pct", F.round((F.col("trips_per_day") / F.col("trips_per_day_prev") - 1) * 100, 3))
        .withColumn("yoy_duration_pct", F.round((F.col("avg_duration_min") / F.col("avg_duration_min_prev") - 1) * 100, 3))
    )
    baseline = yoy.filter(F.col("cbd_flow") == "NON_CBD").select(
        "year", "month_of_year", F.col("yoy_trips_per_day_pct").alias("non_cbd_yoy_pct"))
    return (
        yoy.join(baseline, ["year", "month_of_year"], "left")
        .withColumn("relative_to_non_cbd_pp", F.round(F.col("yoy_trips_per_day_pct") - F.col("non_cbd_yoy_pct"), 3))
        .select("year", "month_of_year", "cbd_flow", F.round("trips_per_day_prev", 2).alias("trips_per_day_prev"),
                F.round("trips_per_day", 2).alias("trips_per_day"), "yoy_trips_per_day_pct",
                "non_cbd_yoy_pct", "relative_to_non_cbd_pp",
                F.round("avg_duration_min_prev", 3).alias("avg_duration_min_prev"),
                F.round("avg_duration_min", 3).alias("avg_duration_min"), "yoy_duration_pct")
    )


def build_dq_monthly(bronze: DataFrame, silver: DataFrame, rejected: DataFrame,
                     excluded_flags: Iterable[str] = GOLD_EXCLUDED_FLAGS) -> DataFrame:
    denominators = bronze.groupBy("_source_month").agg(F.count(F.lit(1)).alias("bronze_rows"))
    rejected_rows = rejected.groupBy("_source_month", F.col("reject_reason").alias("rule")).count() \
        .withColumn("outcome", F.lit("rejected"))
    flagged_rows = silver.select("_source_month", F.explode("dq_flags").alias("rule")) \
        .groupBy("_source_month", "rule").count().withColumn("outcome", F.lit("flagged_kept_in_silver"))
    excluded = silver.filter(F.arrays_overlap("dq_flags", F.array(*[F.lit(f) for f in excluded_flags]))) \
        .groupBy("_source_month").count().withColumn("rule", F.lit("ANY_GOLD_EXCLUDED_FLAG")) \
        .withColumn("outcome", F.lit("excluded_from_gold"))
    combined = rejected_rows.unionByName(flagged_rows).unionByName(excluded.select("_source_month", "rule", "count", "outcome"))
    return (
        combined.join(denominators, "_source_month", "left")
        .withColumn("rate_pct", F.round(F.col("count") / F.col("bronze_rows") * 100, 4))
        .withColumnRenamed("count", "rows")
        .select("_source_month", "outcome", "rule", "rows", "bronze_rows", "rate_pct")
    )


# ----------------------------------------------------------------------------
# CBD zone list validated against the fees actually charged
# ----------------------------------------------------------------------------
def validate_cbd_zones(silver: DataFrame, dim_zone: DataFrame, policy_start: str = CBD_POLICY_START,
                       min_pickups: int = 200, in_threshold: float = 0.90, out_threshold: float = 0.95) -> DataFrame:
    """Every trip that *starts* inside the CBD after the policy start owes the congestion fee.

    So for each pickup zone, the share of post-policy pickups carrying cbd_congestion_fee > 0
    should be close to 1 for CBD zones and clearly lower elsewhere (only trips heading into or
    through the zone pay). Verdicts:
      CONFIRMED_BY_FEES     listed as CBD and share >= in_threshold
      LISTED_BUT_LOW_SHARE  listed as CBD but share < in_threshold  -> check the list
      NOT_LISTED_HIGH_SHARE not listed but share >= out_threshold   -> probably missing from the list
      CONSISTENT_NON_CBD    not listed and share < out_threshold
      TOO_FEW_TRIPS         fewer than min_pickups post-policy pickups
    """
    if "cbd_congestion_fee" not in silver.columns:
        return silver.sparkSession.createDataFrame([], "PULocationID int, verdict string")
    per_zone = (
        silver.filter(F.col("pickup_date") >= F.to_date(F.lit(policy_start)))
        .filter(F.col("cbd_congestion_fee").isNotNull())
        .groupBy("PULocationID")
        .agg(F.count(F.lit(1)).alias("pickups"),
             F.avg(F.when(F.col("cbd_congestion_fee") > 0, 1.0).otherwise(0.0)).alias("fee_share"))
    )
    zones = dim_zone.select(F.col("LocationID").alias("PULocationID"), "zone", "borough", "is_cbd",
                            F.col("cbd_source") if "cbd_source" in dim_zone.columns else F.lit(None).alias("cbd_source"))
    joined = per_zone.join(zones, "PULocationID", "left").withColumn("is_cbd", F.coalesce("is_cbd", F.lit(False)))
    return joined.withColumn(
        "verdict",
        F.when(F.col("pickups") < min_pickups, "TOO_FEW_TRIPS")
        .when(F.col("is_cbd") & (F.col("fee_share") >= in_threshold), "CONFIRMED_BY_FEES")
        .when(F.col("is_cbd"), "LISTED_BUT_LOW_SHARE")
        .when(F.col("fee_share") >= out_threshold, "NOT_LISTED_HIGH_SHARE")
        .otherwise("CONSISTENT_NON_CBD"),
    ).withColumn("fee_share", F.round("fee_share", 4))


def render_cbd_validation(rows: List[Dict[str, Any]]) -> str:
    order = {"NOT_LISTED_HIGH_SHARE": 0, "LISTED_BUT_LOW_SHARE": 1, "CONFIRMED_BY_FEES": 2,
             "TOO_FEW_TRIPS": 3, "CONSISTENT_NON_CBD": 4}
    counts: Dict[str, int] = {}
    for r in rows:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    lines = ["# CBD zone list vs. fees charged in the data", "",
             "Trips starting inside the Congestion Relief Zone after 2025-01-05 always owe the CBD fee, so the "
             "share of fee-paying pickups per zone tests the zone list independently of its source.", "",
             "Verdict counts: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items(), key=lambda kv: order.get(kv[0], 9))), "",
             "| Zone | Name | Listed as CBD | List source | Post-policy pickups | Fee share | Verdict |",
             "|---:|---|---|---|---:|---:|---|"]
    shown = [r for r in rows if r["verdict"] != "CONSISTENT_NON_CBD" or (r["fee_share"] or 0) >= 0.5]
    for r in sorted(shown, key=lambda r: (order.get(r["verdict"], 9), -(r["fee_share"] or 0))):
        lines.append(f"| {r['PULocationID']} | {r.get('zone') or ''} | {r['is_cbd']} | {r.get('cbd_source') or ''} | "
                     f"{r['pickups']:,} | {r['fee_share']} | {r['verdict']} |")
    lines += ["", "Non-CBD zones with a fee share below 0.5 are omitted from the table."]
    return "\n".join(lines) + "\n"


# ----------------------------------------------------------------------------
# Verification against Silver with independent SQL
# ----------------------------------------------------------------------------
def verify_against_silver(spark: SparkSession, zone_hourly: DataFrame, silver_view: str, months: List[str],
                          excluded_flags: Iterable[str] = GOLD_EXCLUDED_FLAGS, samples: int = 3) -> Dict[str, Any]:
    """Recompute a few Gold cells and the global totals straight from Silver in SQL."""
    month_list = ", ".join(f"'{m}'" for m in months)
    flag_list = ", ".join(f"'{f}'" for f in excluded_flags)
    where = f"pickup_month IN ({month_list}) AND NOT arrays_overlap(dq_flags, array({flag_list}))"
    picks = zone_hourly.orderBy(F.desc("trips"), "pickup_date", "pickup_hour", "PULocationID").limit(samples).collect()
    checks = []
    for p in picks:
        manual = spark.sql(f"""
            SELECT COUNT(*) AS trips,
                   ROUND(AVG(fare_amount), 4) AS avg_fare,
                   ROUND(AVG(CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount END) * 100, 4) AS avg_tip_pct,
                   ROUND(SUM(fare_amount + COALESCE(tip_amount, 0) + COALESCE(extra, 0)), 2) AS total_driver_earnings
            FROM {silver_view}
            WHERE {where} AND pickup_date = DATE'{p['pickup_date']}' AND pickup_hour = {p['pickup_hour']}
                  AND PULocationID = {p['PULocationID']}
        """).first().asDict()
        gold = {k: p[k] for k in manual}
        checks.append({"cell": {"pickup_date": str(p["pickup_date"]), "pickup_hour": p["pickup_hour"],
                                "PULocationID": p["PULocationID"]},
                       "gold": gold, "manual_sql": manual,
                       "match": all(abs((gold[k] or 0) - (manual[k] or 0)) < 1e-6 for k in manual)})
    total_manual = spark.sql(f"SELECT COUNT(*) AS n, ROUND(SUM(fare_amount + COALESCE(tip_amount,0) + COALESCE(extra,0)), 2) AS e "
                             f"FROM {silver_view} WHERE {where}").first()
    totals = zone_hourly.agg(F.sum("trips").alias("n"), F.round(F.sum("total_driver_earnings"), 2).alias("e")).first()
    totals_match = totals["n"] == total_manual["n"] and abs((totals["e"] or 0) - (total_manual["e"] or 0)) < 1.0
    return {"cells": checks, "totals": {"gold_trips": totals["n"], "manual_trips": total_manual["n"],
                                        "gold_earnings": totals["e"], "manual_earnings": total_manual["e"],
                                        "match": totals_match},
            "all_match": totals_match and all(c["match"] for c in checks)}


# ----------------------------------------------------------------------------
# Optional external reconciliation with TLC's published aggregates
# ----------------------------------------------------------------------------
def reconcile_external(spark: SparkSession, paths: LakehousePaths, months: List[str]) -> Dict[str, Any]:
    """Compare Bronze pickups per zone-month with TLC's own monthly aggregate, if downloaded.

    The aggregate's column names were not verifiable in advance, so they are
    detected by keyword; if detection fails the step reports SKIPPED with the
    header so the mapping can be fixed, rather than guessing.
    """
    path = paths.raw_ref_dir / "tlc_zone_monthly_raw.csv"
    if not path.exists():
        return {"status": "SKIPPED", "reason": "tlc_zone_monthly_raw.csv not downloaded (--with-tlc-aggregates)"}
    with open(path, newline="", encoding="utf-8-sig") as handle:
        header = next(csv.reader(handle))
    lower = {h.lower(): h for h in header}

    def find(*keywords: str) -> Optional[str]:
        for low, original in lower.items():
            if all(k in low for k in keywords):
                return original
        return None

    month_col = find("month") or find("date")
    industry_col = find("industry") or find("license")
    zone_col = find("location", "id") or find("zone", "id") or find("locationid")
    pickups_col = find("pickup")
    if not all((month_col, industry_col, zone_col, pickups_col)):
        return {"status": "SKIPPED", "reason": "could not detect columns", "header": header}

    ext = (spark.read.option("header", "true").csv(str(path))
           .filter(F.lower(F.col(f"`{industry_col}`")).contains("yellow"))
           .select(F.date_format(F.to_date(F.substring(F.col(f"`{month_col}`"), 1, 10)), "yyyy-MM").alias("pickup_month"),
                   F.col(f"`{zone_col}`").try_cast("int").alias("PULocationID"),
                   F.regexp_replace(F.col(f"`{pickups_col}`"), ",", "").try_cast("long").alias("tlc_pickups"))
           .filter(F.col("pickup_month").isin(months)))
    bronze = spark.read.format("delta").load(paths.bronze_trips).filter(F.col("_batch_id").startswith("yellow_"))
    ours = (bronze.withColumn("pickup_month", F.date_format("tpep_pickup_datetime", "yyyy-MM"))
            .filter(F.col("pickup_month").isin(months))
            .groupBy("pickup_month", F.col("PULocationID").cast("int").alias("PULocationID"))
            .agg(F.count(F.lit(1)).alias("our_pickups")))
    joined = ours.join(ext, ["pickup_month", "PULocationID"], "full_outer").fillna(0, ["our_pickups", "tlc_pickups"])
    per_month = joined.groupBy("pickup_month").agg(
        F.sum("our_pickups").alias("our_pickups"), F.sum("tlc_pickups").alias("tlc_pickups"),
    ).withColumn("diff_pct", F.round((F.col("our_pickups") / F.col("tlc_pickups") - 1) * 100, 3)).orderBy("pickup_month")
    rows = [r.asDict() for r in per_month.collect()]
    return {"status": "DONE", "columns_used": [month_col, industry_col, zone_col, pickups_col], "per_month": rows,
            "within_1pct": all(r["tlc_pickups"] and abs(r["diff_pct"]) <= 1.0 for r in rows)}


# ----------------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------------
def overwrite_table(spark: SparkSession, df: DataFrame, path: str, partition_by: Optional[str] = None) -> int:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    with commit_metadata(spark, {"layer": "gold", "table": Path(path).name}):
        writer.save(path)
    return latest_version(spark, path)


def run(spark: SparkSession, paths: Optional[LakehousePaths] = None,
        months: Optional[List[str]] = None) -> Dict[str, Any]:
    """Rebuild every Gold table from the current Silver snapshot."""
    paths = paths or get_paths()
    months = sorted(months or DEFAULT_MONTHS)
    from src.delta_utils import temporary_conf

    silver_version = latest_version(spark, paths.silver_trips)
    silver = spark.read.format("delta").option("versionAsOf", silver_version).load(paths.silver_trips)
    dim_zone = spark.read.format("delta").load(paths.silver_dim_zone)
    # 20 M enriched rows do not belong in the JVM heap: cache to disk so the two marts below
    # reuse the work without an OutOfMemoryError on a laptop.
    base = prepare_gold_base(silver, dim_zone, months).persist(StorageLevel.DISK_ONLY)
    try:
        # More, smaller shuffle partitions keep each task's memory small on one machine.
        current = int(spark.conf.get("spark.sql.shuffle.partitions", "200"))
        with temporary_conf(spark, {"spark.sql.shuffle.partitions": str(max(current, 64))}):
            zone_hourly = build_zone_hourly(base)
            cbd_monthly = build_cbd_monthly(base)
            versions = {
                "zone_hourly_metrics": overwrite_table(spark, zone_hourly, paths.gold_zone_hourly, "pickup_month"),
                "cbd_flow_monthly": overwrite_table(spark, cbd_monthly, paths.gold_cbd_monthly),
            }
    finally:
        base.unpersist()
    cbd_monthly_saved = spark.read.format("delta").load(paths.gold_cbd_monthly)
    versions["cbd_flow_yoy"] = overwrite_table(spark, build_cbd_yoy(cbd_monthly_saved), paths.gold_cbd_yoy)
    bronze = spark.read.format("delta").load(paths.bronze_trips)
    rejected = spark.read.format("delta").load(paths.silver_rejected)
    versions["dq_monthly"] = overwrite_table(spark, build_dq_monthly(bronze, silver, rejected), paths.gold_dq_monthly)

    silver.createOrReplaceTempView("silver_snapshot")
    zone_hourly_saved = spark.read.format("delta").load(paths.gold_zone_hourly)
    verification = verify_against_silver(spark, zone_hourly_saved, "silver_snapshot", months)
    external = reconcile_external(spark, paths, months)
    cbd_rows = [r.asDict() for r in validate_cbd_zones(silver, dim_zone).collect()]
    write_json(paths.evidence_dir / "cbd_zone_validation.json", cbd_rows)
    (paths.evidence_dir / "cbd_zone_validation.md").write_text(render_cbd_validation(cbd_rows), encoding="utf-8")
    cbd_summary: Dict[str, int] = {}
    for r in cbd_rows:
        cbd_summary[r["verdict"]] = cbd_summary.get(r["verdict"], 0) + 1
    yoy_rows = [r.asDict() for r in spark.read.format("delta").load(paths.gold_cbd_yoy)
                .orderBy("month_of_year", "cbd_flow").collect()]
    result = {
        "silver_version_used": silver_version,
        "table_versions": versions,
        "zone_hourly_rows": zone_hourly_saved.count(),
        "gold_trips": verification["totals"]["gold_trips"],
        "verification": verification,
        "external_reconciliation": external,
        "cbd_zone_validation": cbd_summary,
        "cbd_yoy": yoy_rows,
    }
    write_json(paths.evidence_dir / "gold_verification.json", result)
    if not verification["all_match"]:
        raise RuntimeError("Gold does not match the manual SQL recomputation on Silver")
    return result
