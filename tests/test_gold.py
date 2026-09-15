"""D1 Gold checks. Real Spark/Delta, data isolated in tmp_path."""

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

from src.gold import (DRIVER_COLUMNS, build, compute, profile,
                      verify, with_derived_columns)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    builder = (
        SparkSession.builder.master("local[2]").appName("D1Tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("wh")))
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


SCHEMA = ("trip_id string, tpep_pickup_datetime timestamp, PULocationID int, "
          "passenger_count int, trip_distance double, fare_amount double, "
          "tip_amount double, extra double, total_amount double")


def _df(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def test_tip_pct_is_per_trip_average_not_ratio_of_sums(spark):
    """The definition trap, pinned.

    Two trips: (fare 10, tip 2) -> 20%, (fare 20, tip 2) -> 10%.
    Per-trip average = 15.0.  SUM(tip)/SUM(fare) = 4/30 = 13.33.
    """
    from datetime import datetime
    t = datetime(2025, 1, 1, 0, 30)
    df = _df(spark, [("a", t, 79, 1, 1.0, 10.0, 2.0, 0.0, 12.0),
                     ("b", t, 79, 1, 1.0, 20.0, 2.0, 0.0, 22.0)])
    row = compute(df).first()
    assert row["trip_count"] == 2
    assert row["avg_tip_pct"] == pytest.approx(15.0, abs=1e-6)
    assert row["avg_tip_pct"] != pytest.approx(100 * 4 / 30, abs=0.01)


def test_driver_earnings_excludes_pass_through_charges(spark):
    from datetime import datetime
    t = datetime(2025, 1, 1, 0, 30)
    df = _df(spark, [("a", t, 79, 1, 2.0, 20.0, 3.0, 1.0, 40.0)])
    row = compute(df).first()
    # fare 20 + tip 3 + extra 1 = 24, NOT total_amount 40
    assert row["driver_earnings"] == pytest.approx(24.0)
    assert row["total_revenue"] == pytest.approx(40.0)
    assert set(DRIVER_COLUMNS) == {"fare_amount", "tip_amount", "extra"}


def test_null_extra_does_not_null_the_sum(spark):
    """coalesce matters: fare + tip + NULL would be NULL without it."""
    from datetime import datetime
    t = datetime(2025, 1, 1, 0, 30)
    df = _df(spark, [("a", t, 79, 1, 2.0, 20.0, 3.0, None, 25.0)])
    assert compute(df).first()["driver_earnings"] == pytest.approx(23.0)


def test_zero_fare_yields_null_tip_pct_not_infinity(spark):
    """Silver rejects fare <= 0, but Gold must not depend on that silently."""
    from datetime import datetime
    t = datetime(2025, 1, 1, 0, 30)
    df = _df(spark, [("a", t, 79, 1, 1.0, 0.0, 2.0, 0.0, 2.0)])
    assert with_derived_columns(df).first()["tip_pct"] is None
    assert compute(df).first()["avg_tip_pct"] is None


def test_hour_bucketing_is_utc_and_truncates(spark):
    from datetime import datetime
    df = _df(spark, [("a", datetime(2025, 1, 1, 0, 5), 79, 1, 1.0, 10.0, 1.0, 0.0, 11.0),
                     ("b", datetime(2025, 1, 1, 0, 55), 79, 1, 1.0, 10.0, 1.0, 0.0, 11.0),
                     ("c", datetime(2025, 1, 1, 1, 5), 79, 1, 1.0, 10.0, 1.0, 0.0, 11.0)])
    rows = compute(df).collect()
    assert len(rows) == 2                     # 00:xx pooled, 01:xx separate
    assert sorted(r["trip_count"] for r in rows) == [1, 2]


def test_every_silver_row_lands_in_exactly_one_bucket(spark):
    from datetime import datetime
    rows = [(f"t{i}", datetime(2025, 1, 1, i % 3), 79 + i % 4, 1, 1.0,
             10.0 + i, 2.0, 1.0, 15.0 + i) for i in range(50)]
    df = _df(spark, rows)
    gold = compute(df)
    assert gold.agg(F.sum("trip_count")).first()[0] == 50
    verify(df, gold)                          # raises if conservation breaks


def test_verify_rejects_aggregate_of_averages(spark):
    """A gold table whose avg_tip_pct is an unweighted bucket mean must fail."""
    from datetime import datetime
    df = _df(spark, [("a", datetime(2025, 1, 1, 0), 79, 1, 1.0, 10.0, 2.0, 0.0, 12.0),
                     ("b", datetime(2025, 1, 1, 0), 80, 1, 1.0, 20.0, 2.0, 0.0, 22.0)])
    gold = compute(df).withColumn("avg_tip_pct", F.lit(99.0))
    with pytest.raises(RuntimeError, match="per-trip average"):
        verify(df, gold)


def test_build_round_trip(spark, tmp_path):
    from datetime import datetime
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    rows = [(f"t{i}", datetime(2025, 1, 1, 0, i % 60), 79 + i % 5, 1, 1.5,
             10.0 + i, 2.0, 1.0, 16.0 + i) for i in range(30)]
    _df(spark, rows).write.format("delta").mode("overwrite").save(str(silver))

    res = build(spark, str(silver), str(gold))
    assert res["checks"]["silver_rows"] == 30
    assert res["checks"]["gold_trip_count_sum"] == 30
    assert res["rows_written"] == res["profile"]["gold_buckets"]
    # rebuilding is idempotent
    again = build(spark, str(silver), str(gold))
    assert again["rows_written"] == res["rows_written"]


def test_profile_reports_singleton_buckets(spark):
    from datetime import datetime
    df = _df(spark, [(f"t{i}", datetime(2025, 1, 1, i), 79 + i, 1, 1.0,
                      10.0, 2.0, 0.0, 12.0) for i in range(5)])
    gold = compute(df)
    stats = profile(df, gold, "zone_hour")
    assert stats["gold_buckets"] == 5
    assert stats["singleton_pct"] == 100.0
