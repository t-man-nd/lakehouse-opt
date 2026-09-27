"""D1: rebuild zone/hour-of-day Gold metrics from a pinned Silver snapshot.

avg_tip_pct is 100 * AVG(tip_amount / fare_amount) over trips with fare > 0;
SQL AVG excludes null ratios. driver_earnings follows the assignment's gross
fare + tip + extra definition (null components count as zero), not net income.
Other receipt charges are excluded by this project metric. total_revenue is
SUM(total_amount); discrepancies against sums of individual components require
separate reconciliation and are not attributed to any omitted fee here.

Gold describes the selected B1 sample, not representative NYC-wide demand.
The profile reports its dates, hours and sparse aggregation buckets.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import statistics
from uuid import uuid4

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, functions as F

# Assignment-defined gross earnings components; operating expenses are unknown.
DRIVER_COLUMNS = ("fare_amount", "tip_amount", "extra")

GRAINS = {
    # Optional detailed grain. One bucket per zone per calendar hour.
    "zone_hour": ["PULocationID", "pickup_hour"],
    # Assignment grain: pools the same hour-of-day across calendar dates.
    "zone_hourofday": ["PULocationID", "hour_of_day"],
    # Coarsest. Survives sparse data best.
    "zone_date": ["PULocationID", "pickup_date"],
}


def with_derived_columns(silver: DataFrame) -> DataFrame:
    """Add the per-trip quantities the aggregation needs.

    Kept separate from the aggregation so tests can check a single row's
    tip_pct and driver_earnings without grouping anything.
    """
    silver.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")
    return (
        silver
        .withColumn("pickup_hour", F.date_trunc("hour", "tpep_pickup_datetime"))
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("hour_of_day", F.hour("tpep_pickup_datetime"))
        .withColumn(
            "driver_earnings",
            sum(F.coalesce(F.col(c), F.lit(0.0)) for c in DRIVER_COLUMNS))
        # Guard the denominator explicitly. Silver already rejects fare <= 0,
        # but Gold must not depend silently on an upstream rule: relax that
        # threshold later and an unguarded division emits infinities.
        .withColumn(
            "tip_pct",
            F.when(F.col("fare_amount") > 0,
                   100 * F.col("tip_amount") / F.col("fare_amount")))
    )


def compute(silver: DataFrame, grain: str = "zone_hour") -> DataFrame:
    """Pure transformation, no I/O, so tests can run it on a five-row frame."""
    if grain not in GRAINS:
        raise ValueError(f"grain must be one of {sorted(GRAINS)}")
    keys = GRAINS[grain]

    gold = (
        with_derived_columns(silver)
        .groupBy(*keys)
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 4).alias("avg_fare"),
            # AVG of the per-trip ratio. Not SUM(tip)/SUM(fare).
            F.round(F.avg("tip_pct"), 4).alias("avg_tip_pct"),
            F.round(F.sum("driver_earnings"), 2).alias("driver_earnings"),
            F.round(F.avg("trip_distance"), 4).alias("avg_distance"),
            # Basis: total_amount, the authoritative field. See module docstring.
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("passenger_count"), 4).alias("avg_passengers"),
        )
    )
    return gold.orderBy(*keys)


def profile(silver: DataFrame, gold: DataFrame, grain: str) -> dict:
    """Measure whether the aggregation is interpretable at this grain.

    A bucket holding one trip is not an aggregate; its 'average' is that
    trip's value. Reporting the share of singleton buckets keeps the report
    honest about what the numbers mean.
    """
    derived = with_derived_columns(silver)
    counts = [r["trip_count"] for r in gold.select("trip_count").collect()]
    counts.sort()
    n = len(counts)

    dates = [r["pickup_date"] for r in
             derived.select("pickup_date").distinct().orderBy("pickup_date").collect()]
    hours = {r["hour_of_day"]: r["count"] for r in
             derived.groupBy("hour_of_day").count().orderBy("hour_of_day").collect()}

    return {
        "grain": grain,
        "silver_rows": silver.count(),
        "gold_buckets": n,
        "trips_per_bucket_median": statistics.median(counts) if n else 0,
        "trips_per_bucket_max": counts[-1] if n else 0,
        "singleton_buckets": counts.count(1),
        "singleton_pct": round(100 * counts.count(1) / n, 2) if n else 0.0,
        "distinct_zones": derived.select("PULocationID").distinct().count(),
        "distinct_dates": len(dates),
        "dates": [str(d) for d in dates],
        "hour_of_day_distribution": {str(k): v for k, v in hours.items()},
    }


def verify(silver: DataFrame, gold: DataFrame, grain: str = "zone_hour") -> dict:
    """Compare every bucket against SQL written independently of compute().

    Weighting rounded bucket averages by total trip count is invalid when a
    bucket contains null tip ratios. Checking SQL AVG per bucket preserves its
    null semantics and also handles empty/all-null inputs.
    """
    if grain not in GRAINS:
        raise ValueError(f"grain must be one of {sorted(GRAINS)}")
    spark = silver.sparkSession
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    view = "gold_verification_" + uuid4().hex
    silver.createOrReplaceTempView(view)
    dimensions = {
        "zone_hour": "PULocationID, date_trunc('hour', tpep_pickup_datetime) AS pickup_hour",
        "zone_hourofday": "PULocationID, hour(tpep_pickup_datetime) AS hour_of_day",
        "zone_date": "PULocationID, to_date(tpep_pickup_datetime) AS pickup_date",
    }
    keys = GRAINS[grain]
    try:
        expected = spark.sql(f"""
            SELECT {dimensions[grain]}, COUNT(*) AS trip_count,
                   ROUND(AVG(fare_amount), 4) AS avg_fare,
                   ROUND(AVG(CASE WHEN fare_amount > 0
                                  THEN 100 * tip_amount / fare_amount END), 4) AS avg_tip_pct,
                   ROUND(SUM(COALESCE(fare_amount, 0.0) + COALESCE(tip_amount, 0.0)
                             + COALESCE(extra, 0.0)), 2) AS driver_earnings,
                   ROUND(AVG(trip_distance), 4) AS avg_distance,
                   ROUND(SUM(total_amount), 2) AS total_revenue,
                   ROUND(AVG(passenger_count), 4) AS avg_passengers
            FROM {view} GROUP BY {', '.join(keys)}
        """)
        if set(expected.columns) != set(gold.columns):
            raise RuntimeError("Gold columns do not match the independent SQL contract")
        if gold.groupBy(*keys).count().filter("count > 1").limit(1).count():
            raise RuntimeError("Gold contains duplicate aggregation buckets")
        actual = gold.withColumn("__present", F.lit(True)).alias("actual")
        wanted = expected.withColumn("__present", F.lit(True)).alias("expected")
        equal_keys = F.lit(True)
        for name in keys:
            equal_keys = equal_keys & F.col(f"actual.{name}").eqNullSafe(F.col(f"expected.{name}"))
        joined = actual.join(wanted, equal_keys, "full")
        mismatch = F.col("actual.__present").isNull() | F.col("expected.__present").isNull()
        for name in set(expected.columns) - set(keys):
            left, right = F.col(f"actual.{name}"), F.col(f"expected.{name}")
            tolerance = 0 if name == "trip_count" else 0.00011
            mismatch = mismatch | (left.isNull() != right.isNull()) | (F.abs(left - right) > tolerance)
        if joined.filter(mismatch).limit(1).count():
            raise RuntimeError("Gold differs from independent SQL: per-trip average, earnings or bucket metrics")
        summary = spark.sql(f"""
            SELECT COUNT(*) AS silver_rows,
                   AVG(CASE WHEN fare_amount > 0
                            THEN 100 * tip_amount / fare_amount END) AS per_trip,
                   CASE WHEN SUM(fare_amount) > 0
                        THEN 100 * SUM(tip_amount) / SUM(fare_amount) END AS ratio_of_sums,
                   SUM(total_amount) AS revenue,
                   SUM(COALESCE(fare_amount, 0.0) + COALESCE(tip_amount, 0.0)
                       + COALESCE(extra, 0.0)) AS earnings
            FROM {view}
        """).first()
        gold_trips = gold.agg(F.sum("trip_count")).first()[0] or 0
        if gold_trips != summary.silver_rows:
            raise RuntimeError("Gold trip count does not reconcile with Silver")
        def rounded(value, digits):
            return None if value is None else round(value, digits)
        gap = None if summary.per_trip is None or summary.ratio_of_sums is None else summary.per_trip - summary.ratio_of_sums
        return {
            "independent_sql_passed": True,
            "sql_checked_buckets": expected.count(),
            "silver_rows": summary.silver_rows,
            "gold_trip_count_sum": int(gold_trips),
            "tip_pct_per_trip_mean": rounded(summary.per_trip, 4),
            "tip_pct_ratio_of_sums": rounded(summary.ratio_of_sums, 4),
            "tip_pct_definition_gap_pp": rounded(gap, 4),
            "total_revenue": rounded(summary.revenue, 2),
            "driver_earnings": rounded(summary.earnings, 2),
        }
    finally:
        spark.catalog.dropTempView(view)


def build(
    spark: SparkSession,
    silver_dir: str = "data/silver/taxi_trips",
    gold_dir: str = "data/gold/taxi_hourly_metrics",
    *,
    grain: str = "zone_hour",
    source_version: int | None = None,
) -> dict:
    """Rebuild Gold from Silver. Overwrite is correct here: Gold is derived."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    source, destination = Path(silver_dir).resolve(), Path(gold_dir).resolve()
    if source == destination or source in destination.parents or destination in source.parents:
        raise ValueError("Silver and Gold directories must not overlap")
    if source_version is None:
        source_version = int(DeltaTable.forPath(spark, silver_dir).history(1).first()["version"])
    silver = spark.read.format("delta").option("versionAsOf", source_version).load(silver_dir).persist()
    try:
        gold = compute(silver, grain).persist()
        try:
            checks = verify(silver, gold, grain)
            stats = profile(silver, gold, grain)

            (gold.write.format("delta").mode("overwrite")
                 .option("overwriteSchema", "true").save(gold_dir))

            written = spark.read.format("delta").load(gold_dir)
            return {
                "grain": grain,
                "gold_dir": str(destination),
                "source_silver_version": source_version,
                "timezone": "UTC",
                "rows_written": written.count(),
                "files": len(written.inputFiles()),
                "checks": checks,
                "profile": stats,
                "definitions": {
                    "avg_tip_pct": "100 * AVG(CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount END)",
                    "driver_earnings": "SUM(COALESCE(fare_amount, 0) + COALESCE(tip_amount, 0) + COALESCE(extra, 0))",
                    "total_revenue": "SUM(total_amount), not the sum of components",
                },
            }
        finally:
            gold.unpersist()
    finally:
        silver.unpersist()


def run(
    spark: SparkSession, silver_dir: str = "data/silver/taxi_trips",
    gold_dir: str = "data/gold/taxi_hourly_metrics", *, grain: str = "zone_hourofday",
    overwrite: bool = False, source_version: int | None = None,
) -> dict:
    """E1 entry point; replacing an existing derived table must be explicit."""
    if Path(gold_dir).exists() and not overwrite:
        raise FileExistsError("Gold destination already exists; choose a fresh directory or overwrite=True")
    return build(spark, silver_dir, gold_dir, grain=grain, source_version=source_version)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--silver-dir", default="data/silver/taxi_trips")
    p.add_argument("--gold-dir", default="data/gold/taxi_hourly_metrics")
    p.add_argument("--grain", default="zone_hourofday", choices=sorted(GRAINS))
    p.add_argument("--show", type=int, default=10,
                   help="Print the busiest N buckets")
    p.add_argument("--output", help="Write the summary as JSON")
    p.add_argument("--master", default="local[2]")
    args = p.parse_args()

    builder = (
        SparkSession.builder.appName("GoldAggregate").master(args.master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "true")
        # Buckets are derived from pickup time. Without a pinned timezone the
        # same code produces different buckets in Hanoi and in New York --
        # a silent wrong answer, not a crash.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        result = build(spark, args.silver_dir, args.gold_dir, grain=args.grain)

        if args.show:
            print(f"\nBusiest {args.show} buckets:")
            (spark.read.format("delta").load(args.gold_dir)
             .orderBy(F.col("trip_count").desc())
             .show(args.show, truncate=False))

        text = json.dumps(result, indent=2, ensure_ascii=False, default=str) + "\n"
        if args.output:
            out = Path(args.output)
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(text, encoding="utf-8")
        print(text)

        pr = result["profile"]
        if pr["singleton_pct"] > 20:
            print(f"NOTE: {pr['singleton_pct']}% of buckets hold a single trip. "
                  f"Their 'averages' are that trip's values. Consider "
                  f"--grain zone_date, and state this in the report.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
