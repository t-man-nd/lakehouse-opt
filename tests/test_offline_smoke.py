"""Pure-Spark pieces of the Delta modules, exercised without Delta.

These catch logic bugs in profiling, the CDC feed builder, the Gold DQ mart,
Gold's SQL self-check and the results renderer on machines without Delta.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from lakehouse_pipeline import render_results
from src.config import DQThresholds, get_paths
from src.dq_rules import classify_trips, to_rejected_rows, to_silver_rows
from src.gold import build_dq_monthly, build_zone_hourly, prepare_gold_base, verify_against_silver
from src.tlc_profile import preview_rules, profile_keys, profile_values, render_markdown
from src.tlc_silver import build_correction_feed
from tests.test_dq_rules import bronze_like
from tests.tlc_like import build_dataset


def _bronze_and_silver(spark, root, months):
    bronze = None
    for m in months:
        b = bronze_like(spark, root, m)
        bronze = b if bronze is None else bronze.unionByName(b, allowMissingColumns=True)
    classified = classify_trips(bronze)
    return bronze, to_silver_rows(classified), to_rejected_rows(classified, bronze.columns)


def test_profile_sections_and_markdown(spark, tmp_path):
    root = tmp_path
    build_dataset(root, ["2024-01", "2024-02"], clean=60)
    bronze, _, _ = _bronze_and_silver(spark, root, ["2024-01", "2024-02"])
    th = DQThresholds()
    profile = {"bronze_version": 0, "thresholds": th.__dict__, "values": profile_values(bronze, th),
               "distributions": {}, "keys": profile_keys(bronze), "rule_preview": preview_rules(bronze, th)}
    values = profile["values"]["2024-01"]
    assert values["null__tpep_pickup_datetime"] == 2
    assert values["pickup_before_month"] == 3                           # 2 late arrivals + the 2009 row
    assert values["pickup_before_late_window"] == 1                     # the 2009 row
    keys = profile["keys"]
    assert keys["keys_seen_in_more_than_one_file"] == 1                 # the resubmitted trip
    assert keys["reversal_hypothesis"]["2024-01"] == {"negative_fare_rows": 1, "with_matching_positive_trip": 1}
    assert keys["within_file"]["2024-01"]["exact_duplicate_excess_rows"] == 2
    md = render_markdown(profile)
    assert "## 6. Reversal hypothesis" in md and "| 2024-01 | 2024-02 |" in md


def test_correction_feed_changes_only_money_columns(spark, tmp_path):
    build_dataset(tmp_path, ["2025-01"], clean=80)
    _, silver, _ = _bronze_and_silver(spark, tmp_path, ["2025-01"])
    feed = build_correction_feed(silver, seed=42, n_updates=6, n_inserts=2, target_month="2025-01")
    again = build_correction_feed(silver, seed=42, n_updates=6, n_inserts=2, target_month="2025-01")
    assert feed.count() == 8
    assert sorted(map(str, feed.collect())) == sorted(map(str, again.collect()))      # fixed seed -> same feed
    reclassified = classify_trips(feed.withColumn("_source_month", F.lit("2025-01")).withColumn("_source_file", F.lit("cdc")))
    assert reclassified.filter("reject_reason IS NOT NULL").count() == 0
    keys = {r["trip_key"] for r in reclassified.collect()}
    existing = {r["trip_key"] for r in silver.select("trip_key").collect()}
    assert len(keys & existing) == 6 and len(keys - existing) == 2                    # 6 updates, 2 inserts
    joined = reclassified.select("trip_key", F.col("_t.tip_amount").alias("new_tip"), F.col("_t.fare_amount").alias("new_fare")) \
        .join(silver.select("trip_key", "tip_amount", "fare_amount"), "trip_key")
    for r in joined.collect():
        assert abs(r["new_tip"] - r["tip_amount"] - 2.0) < 1e-9
        assert abs(r["new_fare"] - r["fare_amount"]) in (0.0, 1.5) or abs(abs(r["new_fare"] - r["fare_amount"]) - 1.5) < 1e-9


def test_gold_dq_mart_and_sql_self_check(spark, tmp_path):
    months = ["2025-01"]
    build_dataset(tmp_path, months, clean=80)
    bronze, silver, rejected = _bronze_and_silver(spark, tmp_path, months)
    dim = spark.read.option("header", "true").csv(str(tmp_path / "data/raw/ref/taxi_zone_lookup.csv")).select(
        F.col("LocationID").cast("int").alias("LocationID"), F.col("Borough").alias("borough"), F.col("Zone").alias("zone"),
        F.col("LocationID").cast("int").isin(161, 162, 163, 164, 230).alias("is_cbd"),
        F.col("LocationID").cast("int").isin(264, 265).alias("is_unknown"), F.lit(True).alias("cbd_reference_available"))
    zone_hourly = build_zone_hourly(prepare_gold_base(silver, dim, months))
    silver.createOrReplaceTempView("silver_smoke")
    check = verify_against_silver(spark, zone_hourly, "silver_smoke", months)
    assert check["all_match"], check
    dq = {(r["outcome"], r["rule"]): r["rows"] for r in build_dq_monthly(bronze, silver, rejected).collect()}
    assert dq[("rejected", "DUPLICATE_TRIP")] == 2
    assert dq[("flagged_kept_in_silver", "REVERSED")] == 1
    assert dq[("excluded_from_gold", "ANY_GOLD_EXCLUDED_FLAG")] == 1


def test_results_renderer_handles_partial_runs():
    run = {"started_at": "t", "status": "FAILED", "source": "tlc", "months": ["2025-01"],
           "steps": [{"step": "bronze", "status": "OK", "seconds": 1.0, "result": {"bronze_count": 10, "bronze_version": 0}},
                     {"step": "silver", "status": "FAILED", "seconds": 0.5, "error": "RuntimeError: boom"}]}
    text = render_results(run, get_paths())
    assert "bronze_count=10" in text and "RuntimeError: boom" in text


def test_findings_diagnostics(spark, tmp_path):
    """The diagnostic must attribute a constant gap, a vendor's zero-duration trips and
    negative fares correctly on data where the answer is known."""
    from scripts.investigate_findings import add_gap, q1_total_mismatch, q2_zero_duration, q3_negative_fares, render

    rows = []
    for i in range(20):        # clean rows: components add up exactly
        rows.append(("2025-03", 2, 10.0, 1.0, 0.5, 2.0, 0.0, 1.0, 2.5, 0.0, 0.75, 17.75, 1, 1,
                     "2025-03-01 08:00:00", "2025-03-01 08:20:00", "N"))
    for i in range(10):        # rows where total is exactly 1.50 higher than the sum
        rows.append(("2025-03", 2, 10.0, 1.0, 0.5, 2.0, 0.0, 1.0, 2.5, 0.0, 0.75, 19.25, 1, 1,
                     "2025-03-01 09:00:00", "2025-03-01 09:15:00", "N"))
    for i in range(7):         # vendor 7: zero duration
        rows.append(("2025-03", 7, 8.0, 0.0, 0.5, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 9.5, 0, None,
                     "2025-03-02 10:00:00", "2025-03-02 10:00:00", None))
    for i in range(4):         # negative fares from vendor 2
        rows.append(("2025-03", 2, -10.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -10.0, 1, 1,
                     "2025-03-03 11:00:00", "2025-03-03 11:10:00", "N"))
    schema = ("_source_month string, VendorID int, fare_amount double, extra double, mta_tax double, "
              "tip_amount double, tolls_amount double, improvement_surcharge double, congestion_surcharge double, "
              "Airport_fee double, cbd_congestion_fee double, total_amount double, payment_type int, "
              "RatecodeID int, tpep_pickup_datetime string, tpep_dropoff_datetime string, store_and_fwd_flag string")
    bronze = spark.createDataFrame(rows, schema).withColumn(
        "passenger_count", F.when(F.col("VendorID") == 7, None).otherwise(F.lit(1)))

    gaps = {r["gap"]: r["count"] for r in add_gap(bronze).groupBy("gap").count().collect()}
    assert gaps[1.5] == 10 and gaps[0.0] == 31

    mismatch = q1_total_mismatch(bronze)
    assert mismatch["per_month"][0]["mismatched_rows"] == 10
    assert mismatch["most_common_gaps"][0]["gap"] == "1.5"
    assert mismatch["per_month"][0]["with_null_block"] == 0          # the gap is not the NULL-block rows

    zero = {(r["VendorID"]): r["count"] for r in q2_zero_duration(bronze)}
    assert zero == {7: 7}
    negatives = q3_negative_fares(bronze)
    assert negatives[0]["rows"] == 4 and negatives[0]["VendorID"] == 2
    assert "## 2. Zero-duration trips by vendor" in render(
        {"total_mismatch": mismatch, "zero_duration": q2_zero_duration(bronze),
         "negative_fares": negatives, "null_block": []})
