"""Profile Bronze before cleaning it.

This is the "explore the data" step: Silver rules are justified by what this
report finds, not assumed in advance. Every metric is computed per source
month so 2024 vs 2025 differences (e.g. new payment_type 0 / Flex Fare,
cbd_congestion_fee) are visible.

Outputs
-------
docs/profile/bronze_profile.json   machine-readable metrics
docs/profile/bronze_profile.md     tables for the report

Sections
--------
1. Volume and completeness      rows, NULL counts per column
2. Time sanity                  pickups outside the file month, durations
3. Value sanity                 distance, fare, totals, passengers, rate codes, zones
4. Categorical distributions    payment_type, VendorID, RatecodeID
5. Key analysis                 trip_key uniqueness, exact duplicates, collisions,
                                keys shared across files (late arrivals / resubmissions)
6. Reversal hypothesis          do negative-fare rows mirror a positive trip?
7. Rule preview                 what TLC-v2 would reject/flag, per month
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import DQThresholds, LakehousePaths, get_paths
from src.delta_utils import latest_version, write_json
from src.dq_rules import TYPED, add_typed_struct, classify_trips, row_hash_expr, trip_key_expr, typed_columns


def month_bounds() -> tuple:
    start = F.to_timestamp(F.concat(F.col("_source_month"), F.lit("-01 00:00:00")), "yyyy-MM-dd HH:mm:ss")
    end = F.add_months(start, 1).cast("timestamp")
    return start, end


def profile_values(bronze: DataFrame, th: DQThresholds) -> Dict[str, Dict[str, Any]]:
    """One aggregation pass: numeric sanity metrics per source month."""
    start, end = month_bounds()
    pickup, dropoff = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    duration_min = (F.unix_timestamp(dropoff) - F.unix_timestamp(pickup)) / 60.0
    speed = F.try_divide(F.col("trip_distance"), duration_min / 60.0)
    has = set(bronze.columns)

    def count_if(cond) -> F.Column:
        return F.sum(F.when(F.coalesce(cond, F.lit(False)), 1).otherwise(0))

    null_columns = [c for c in (
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
        "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount",
        "tip_amount", "total_amount", "congestion_surcharge", "Airport_fee", "cbd_congestion_fee",
    ) if c in has]
    aggs = [F.count(F.lit(1)).alias("rows")]
    aggs += [count_if(F.col(c).isNull()).alias(f"null__{c}") for c in null_columns]
    aggs += [
        F.min(pickup).cast("string").alias("min_pickup"),
        F.max(pickup).cast("string").alias("max_pickup"),
        count_if(pickup < start).alias("pickup_before_month"),
        count_if(pickup < start - F.expr(f"INTERVAL {th.late_window_days} DAYS")).alias("pickup_before_late_window"),
        count_if(pickup >= end).alias("pickup_after_month"),
        count_if(pickup >= end + F.expr(f"INTERVAL {th.lead_tolerance_hours} HOURS")).alias("pickup_after_tolerance"),
        count_if(dropoff < pickup).alias("dropoff_before_pickup"),
        count_if(dropoff == pickup).alias("zero_duration"),
        count_if(duration_min > th.max_duration_hours * 60).alias("duration_over_max"),
        F.percentile_approx(duration_min, [0.5, 0.99, 0.999], 1000).alias("duration_min_p50_p99_p999"),
        F.max(duration_min).alias("duration_min_max"),
        count_if(F.col("trip_distance") <= 0).alias("distance_zero_or_negative"),
        count_if(F.col("trip_distance") > th.max_distance_miles).alias("distance_over_max"),
        F.percentile_approx("trip_distance", [0.5, 0.99, 0.999], 1000).alias("distance_p50_p99_p999"),
        F.max("trip_distance").alias("distance_max"),
        count_if((duration_min >= 1) & (speed > th.max_speed_mph)).alias("speed_over_max"),
        count_if(F.col("fare_amount") < 0).alias("fare_negative"),
        count_if(F.col("fare_amount") == 0).alias("fare_zero"),
        F.percentile_approx("fare_amount", [0.01, 0.5, 0.99], 1000).alias("fare_p01_p50_p99"),
        F.max("fare_amount").alias("fare_max"),
        count_if(F.col("total_amount") < 0).alias("total_negative"),
        count_if(F.col("passenger_count") == 0).alias("passengers_zero"),
        count_if(F.col("RatecodeID") == 99).alias("ratecode_99"),
        count_if(F.col("PULocationID").isin(*th.unknown_zone_ids)
                 | F.col("DOLocationID").isin(*th.unknown_zone_ids)).alias("unknown_zone_264_265"),
        count_if((F.col("payment_type") == 1) & (F.col("tip_amount") > 0)).alias("card_trips_with_tip"),
        count_if(F.col("payment_type") == 1).alias("card_trips"),
        count_if((F.col("payment_type") == 2) & (F.col("tip_amount") > 0)).alias("cash_trips_with_recorded_tip"),
    ]
    if "cbd_congestion_fee" in has:
        aggs += [
            count_if(F.col("cbd_congestion_fee") > 0).alias("cbd_fee_positive"),
            F.expr("percentile_approx(CASE WHEN cbd_congestion_fee > 0 THEN cbd_congestion_fee END, 0.5)")
            .alias("cbd_fee_median_when_positive"),
        ]
    rows = bronze.groupBy("_source_month").agg(*aggs).orderBy("_source_month").collect()
    return {r["_source_month"]: {k: v for k, v in r.asDict().items() if k != "_source_month"} for r in rows}


def profile_distributions(bronze: DataFrame, column: str) -> Dict[str, Dict[str, int]]:
    rows = bronze.groupBy("_source_month", column).count().collect()
    result: Dict[str, Dict[str, int]] = {}
    for r in rows:
        result.setdefault(r["_source_month"], {})[str(r[column])] = r["count"]
    return {m: dict(sorted(v.items(), key=lambda kv: -kv[1])) for m, v in sorted(result.items())}


def profile_keys(bronze: DataFrame) -> Dict[str, Any]:
    """Is trip_key a usable business key? Measure before relying on it for MERGE."""
    typed = add_typed_struct(bronze)
    business = typed_columns(bronze)
    keyed = typed.select(
        "_source_month",
        trip_key_expr().alias("trip_key"),
        row_hash_expr(business).alias("row_hash"),
        F.col(TYPED)["fare_amount"].alias("fare"),
    ).persist()
    try:
        per_key_month = keyed.groupBy("_source_month", "trip_key").agg(
            F.count(F.lit(1)).alias("n"), F.countDistinct("row_hash").alias("n_hash"))
        within = per_key_month.groupBy("_source_month").agg(
            F.sum("n").alias("rows"),
            F.count(F.lit(1)).alias("distinct_keys"),
            F.sum(F.when((F.col("n") > 1) & (F.col("n_hash") == 1), F.col("n") - 1).otherwise(0)).alias("exact_duplicate_excess_rows"),
            F.sum(F.when(F.col("n_hash") > 1, 1).otherwise(0)).alias("keys_with_conflicting_content"),
        ).orderBy("_source_month").collect()
        across = keyed.groupBy("trip_key").agg(F.countDistinct("_source_month").alias("files"))
        cross_file_keys = across.filter("files > 1").count()

        negatives = keyed.filter("fare < 0").select("_source_month", "trip_key", F.abs("fare").alias("neg_abs"))
        positives = keyed.filter("fare > 0").select("trip_key", F.col("fare").alias("pos_fare"))
        paired = (negatives.join(positives, "trip_key", "left")
                  .groupBy("_source_month", "trip_key", "neg_abs")
                  .agg(F.max(F.when(F.abs(F.col("pos_fare") - F.col("neg_abs")) < 0.005, 1).otherwise(0)).alias("paired")))
        reversal = paired.groupBy("_source_month").agg(
            F.count(F.lit(1)).alias("negative_fare_rows"),
            F.sum("paired").alias("with_matching_positive_trip"),
        ).orderBy("_source_month").collect()
    finally:
        keyed.unpersist()
    return {
        "within_file": {r["_source_month"]: {k: r[k] for k in r.asDict() if k != "_source_month"} for r in within},
        "keys_seen_in_more_than_one_file": cross_file_keys,
        "reversal_hypothesis": {r["_source_month"]: {"negative_fare_rows": r["negative_fare_rows"],
                                                    "with_matching_positive_trip": r["with_matching_positive_trip"]}
                                for r in reversal},
    }


def preview_rules(bronze: DataFrame, th: DQThresholds) -> Dict[str, Any]:
    """Apply TLC-v2 one source month at a time (as Silver does), keeping each job small.

    Duplicates are detected within each file; a trip resubmitted in a later file is not
    counted here (in Silver it becomes a MERGE update/no-op).
    """
    out: Dict[str, Dict[str, Dict[str, int]]] = {"reject_reason": {}, "dq_flag": {}}
    months = sorted(r["_source_month"] for r in bronze.select("_source_month").distinct().collect())
    for month in months:
        classified = classify_trips(bronze.filter(F.col("_source_month") == month), th).select(
            "reject_reason", "dq_flags").persist()
        try:
            for r in classified.filter("reject_reason IS NOT NULL").groupBy("reject_reason").count().collect():
                out["reject_reason"].setdefault(month, {})[r["reject_reason"]] = r["count"]
            flags = (classified.filter("reject_reason IS NULL").select(F.explode("dq_flags").alias("flag"))
                     .groupBy("flag").count().collect())
            for r in flags:
                out["dq_flag"].setdefault(month, {})[r["flag"]] = r["count"]
        finally:
            classified.unpersist()
    return out


def render_markdown(profile: Dict[str, Any]) -> str:
    months: List[str] = sorted(profile["values"])
    header = "| Metric | " + " | ".join(months) + " |\n|---|" + "---:|" * len(months)

    def fmt(value: Any) -> str:
        if isinstance(value, float):
            return f"{value:,.2f}"
        if isinstance(value, int):
            return f"{value:,}"
        if isinstance(value, list):
            return " / ".join(fmt(v) for v in value)
        return "" if value is None else str(value)

    lines = ["# Bronze profile", "",
             f"Bronze version profiled: v{profile['bronze_version']}. Thresholds: `{profile['thresholds']}`.", "",
             "## 1–3. Completeness, time and value sanity", "", header]
    metrics = sorted({k for m in months for k in profile["values"][m]})
    for metric in metrics:
        lines.append(f"| {metric} | " + " | ".join(fmt(profile["values"][m].get(metric)) for m in months) + " |")

    for name, dist in profile["distributions"].items():
        lines += ["", f"## 4. Distribution of `{name}`", "", header]
        values = sorted({k for m in dist for k in dist[m]})
        for value in values:
            lines.append(f"| {value} | " + " | ".join(fmt(dist.get(m, {}).get(value, 0)) for m in months) + " |")

    keys = profile["keys"]
    lines += ["", "## 5. Key analysis (`trip_key` = vendor + pickup + dropoff + PU + DO)", "", header]
    kmetrics = sorted({k for m in keys["within_file"].values() for k in m})
    for metric in kmetrics:
        lines.append(f"| {metric} | " + " | ".join(fmt(keys["within_file"].get(m, {}).get(metric)) for m in months) + " |")
    lines += ["", f"Keys appearing in more than one file: **{keys['keys_seen_in_more_than_one_file']:,}** "
              "(late arrivals / resubmissions; these become MERGE matches in Silver).", "",
              "## 6. Reversal hypothesis", "",
              "Negative-fare rows that share `trip_key` with a positive trip of the same absolute fare.", "", header]
    for metric in ("negative_fare_rows", "with_matching_positive_trip"):
        lines.append(f"| {metric} | " + " | ".join(
            fmt(keys["reversal_hypothesis"].get(m, {}).get(metric, 0)) for m in months) + " |")

    for section, title in (("reject_reason", "7a. Rule preview: hard rejects"), ("dq_flag", "7b. Rule preview: soft flags")):
        data = profile["rule_preview"][section]
        lines += ["", f"## {title}", "", header]
        for value in sorted({k for m in data.values() for k in m}):
            lines.append(f"| {value} | " + " | ".join(fmt(data.get(m, {}).get(value, 0)) for m in months) + " |")
    lines += ["", "## How to use this report", "",
              "For each rule in docs/DQ_RULES.md, cite the row above that justifies keeping, "
              "tightening or relaxing its threshold. Record any change in `DQThresholds` (src/config.py)."]
    return "\n".join(lines) + "\n"


def run(spark: SparkSession, paths: Optional[LakehousePaths] = None,
        thresholds: DQThresholds = DQThresholds()) -> Dict[str, Any]:
    paths = paths or get_paths()
    version = latest_version(spark, paths.bronze_trips)
    bronze = spark.read.format("delta").option("versionAsOf", version).load(paths.bronze_trips)
    profile = {
        "bronze_version": version,
        "thresholds": thresholds.__dict__,
        "values": profile_values(bronze, thresholds),
        "distributions": {c: profile_distributions(bronze, c) for c in ("payment_type", "VendorID", "RatecodeID")},
        "keys": profile_keys(bronze),
        "rule_preview": preview_rules(bronze, thresholds),
    }
    write_json(paths.profile_dir / "bronze_profile.json", profile)
    (paths.profile_dir / "bronze_profile.md").write_text(render_markdown(profile), encoding="utf-8")
    return {
        "bronze_version": version,
        "months": sorted(profile["values"]),
        "report": str(paths.profile_dir / "bronze_profile.md"),
        "keys_seen_in_more_than_one_file": profile["keys"]["keys_seen_in_more_than_one_file"],
    }
