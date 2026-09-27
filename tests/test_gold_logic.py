"""Gold metric definitions on hand-computed examples (plain Spark, no Delta)."""

from __future__ import annotations

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from src.gold import build_cbd_monthly, build_cbd_yoy, build_zone_hourly, prepare_gold_base

SILVER_SCHEMA = (
    "trip_key string, PULocationID int, DOLocationID int, payment_type int, fare_amount double, "
    "tip_amount double, extra double, tolls_amount double, mta_tax double, congestion_surcharge double, "
    "trip_distance double, trip_duration_sec bigint, dq_flags array<string>, pickup_date date, "
    "pickup_hour int, pickup_month string, tpep_pickup_datetime timestamp, cbd_congestion_fee double"
)


def trip(key, pu=100, do=101, pay=1, fare=10.0, tip=2.0, extra=1.0, tolls=6.94, flags=(), day=date(2025, 2, 3),
         hour=8, cbd=0.0, dist=2.0, dur=600):
    return (key, pu, do, pay, fare, tip, extra, tolls, 0.5, 2.5, dist, dur, list(flags), day, hour,
            day.strftime("%Y-%m"), datetime(day.year, day.month, day.day, hour), cbd)


@pytest.fixture()
def dim_zone(spark):
    rows = [(i, "Manhattan" if i in (161, 230) else "Queens", f"Zone {i}", i in (161, 230), i in (264, 265), True)
            for i in list(range(1, 264)) + [264, 265]]
    return spark.createDataFrame(rows, "LocationID int, borough string, zone string, is_cbd boolean, "
                                       "is_unknown boolean, cbd_reference_available boolean")


def test_tip_pct_is_average_of_per_trip_ratios_with_zero_fare_guard(spark, dim_zone):
    silver = spark.createDataFrame([
        trip("a", fare=10.0, tip=1.0),            # 10 %
        trip("b", fare=40.0, tip=20.0),           # 50 %
        trip("c", fare=0.0, tip=5.0),             # excluded from the average (no division by zero)
        trip("d", pay=2, fare=20.0, tip=0.0),     # cash: 0 % in all-trips metric, absent from card metric
    ], SILVER_SCHEMA)
    base = prepare_gold_base(silver, dim_zone, ["2025-02"])
    row = build_zone_hourly(base).first()
    assert row["trips"] == 4
    assert row["avg_tip_pct"] == pytest.approx((10 + 50 + 0) / 3, abs=1e-4)      # AVG of ratios, not SUM/SUM
    assert row["avg_tip_pct"] != pytest.approx(21.0 / 70.0 * 100, abs=1e-2)
    assert row["avg_tip_pct_card"] == pytest.approx((10 + 50) / 2, abs=1e-4)


def test_driver_earnings_excludes_pass_through_charges(spark, dim_zone):
    silver = spark.createDataFrame([trip("a", fare=10.0, tip=2.0, extra=1.0, tolls=6.94)], SILVER_SCHEMA)
    row = build_zone_hourly(prepare_gold_base(silver, dim_zone, ["2025-02"])).first()
    assert row["total_driver_earnings"] == pytest.approx(13.0)   # tolls, MTA tax, congestion not included


def test_excluded_flags_and_out_of_scope_months_are_dropped(spark, dim_zone):
    silver = spark.createDataFrame([
        trip("ok", flags=("ZERO_DISTANCE",)),                      # soft flag kept
        trip("rev", flags=("REVERSED",)),
        trip("speed", flags=("IMPLAUSIBLE_SPEED", "LATE_ARRIVAL")),
        trip("late_dec", day=date(2024, 12, 30)),                  # late arrival outside analysis months
    ], SILVER_SCHEMA)
    keys = {r["trip_key"] for r in prepare_gold_base(silver, dim_zone, ["2025-02"]).collect()}
    assert keys == {"ok"}


def test_cbd_flow_classification(spark, dim_zone):
    silver = spark.createDataFrame([
        trip("within", pu=161, do=230), trip("into", pu=100, do=161), trip("out", pu=230, do=100),
        trip("non", pu=100, do=101), trip("unknown", pu=264, do=161),
    ], SILVER_SCHEMA)
    flows = {r["trip_key"]: r["cbd_flow"] for r in prepare_gold_base(silver, dim_zone, ["2025-02"]).collect()}
    assert flows == {"within": "WITHIN_CBD", "into": "INTO_CBD", "out": "OUT_OF_CBD",
                     "non": "NON_CBD", "unknown": "UNKNOWN_ZONE"}


def test_cbd_flow_unavailable_without_reference(spark, dim_zone):
    no_ref = dim_zone.withColumn("cbd_reference_available", F.lit(False)).withColumn("is_cbd", F.lit(False))
    silver = spark.createDataFrame([trip("x", pu=161, do=230)], SILVER_SCHEMA)
    assert prepare_gold_base(silver, no_ref, ["2025-02"]).first()["cbd_flow"] == "CBD_UNAVAILABLE"


def test_period_days_split_at_policy_start_and_leap_year(spark, dim_zone):
    silver = spark.createDataFrame([
        trip("jan_pre", day=date(2025, 1, 2)), trip("jan_post", day=date(2025, 1, 20)),
        trip("feb24", day=date(2024, 2, 10)), trip("feb25", day=date(2025, 2, 10)),
    ], SILVER_SCHEMA)
    monthly = build_cbd_monthly(prepare_gold_base(silver, dim_zone, ["2024-02", "2025-01", "2025-02"]))
    days = {(r["pickup_month"], r["policy_period"]): r["period_days"] for r in monthly.collect()}
    assert days == {("2025-01", "pre_policy"): 4, ("2025-01", "post_policy"): 27,
                    ("2024-02", "pre_policy"): 29, ("2025-02", "post_policy"): 28}


def test_yoy_uses_trips_per_calendar_day(spark, dim_zone):
    feb24 = [trip(f"a{i}", day=date(2024, 2, 1 + i % 29)) for i in range(58)]    # 2.0 trips/day over 29 days
    feb25 = [trip(f"b{i}", day=date(2025, 2, 1 + i % 28)) for i in range(56)]    # 2.0 trips/day over 28 days
    silver = spark.createDataFrame(feb24 + feb25, SILVER_SCHEMA)
    monthly = build_cbd_monthly(prepare_gold_base(silver, dim_zone, ["2024-02", "2025-02"]))
    yoy = build_cbd_yoy(monthly).filter("cbd_flow = 'NON_CBD'").first()
    assert yoy["trips_per_day_prev"] == pytest.approx(2.0)
    assert yoy["trips_per_day"] == pytest.approx(2.0)
    assert yoy["yoy_trips_per_day_pct"] == pytest.approx(0.0)       # raw counts would wrongly show -3.4 %
    assert yoy["relative_to_non_cbd_pp"] == pytest.approx(0.0)


def test_cbd_zone_validation_verdicts(spark, dim_zone):
    from src.gold import validate_cbd_zones

    dim = dim_zone.withColumn("cbd_source", F.when(F.col("is_cbd"), "mta").otherwise("not_cbd"))
    post = date(2025, 2, 3)
    rows = (
        [trip(f"a{i}", pu=161, cbd=0.75, day=post) for i in range(10)]                                   # listed, all pay
        + [trip(f"b{i}", pu=230, cbd=0.75 if i < 5 else 0.0, day=post) for i in range(10)]               # listed, half pay
        + [trip(f"c{i}", pu=100, cbd=0.75, day=post) for i in range(10)]                                  # not listed, all pay
        + [trip(f"d{i}", pu=101, cbd=0.75 if i < 3 else 0.0, day=post) for i in range(10)]                # not listed, some pay
        + [trip(f"e{i}", pu=102, cbd=0.75, day=post) for i in range(3)]                                   # too few trips
        + [trip(f"f{i}", pu=101, cbd=0.0, day=date(2025, 1, 2)) for i in range(10)]                       # pre-policy: ignored
    )
    silver = spark.createDataFrame(rows, SILVER_SCHEMA)
    verdicts = {r["PULocationID"]: r["verdict"] for r in validate_cbd_zones(silver, dim, min_pickups=5).collect()}
    assert verdicts == {161: "CONFIRMED_BY_FEES", 230: "LISTED_BUT_LOW_SHARE", 100: "NOT_LISTED_HIGH_SHARE",
                        101: "CONSISTENT_NON_CBD", 102: "TOO_FEW_TRIPS"}
