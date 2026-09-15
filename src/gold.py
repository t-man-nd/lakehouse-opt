"""D1: Hourly Gold aggregation from the Silver taxi trips table."""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    avg,
    sum,
    count,
    date_trunc
)

try:
    from delta import configure_spark_with_delta_pip
except ImportError:  # pragma: no cover
    configure_spark_with_delta_pip = None  # type: ignore


def run_gold(
    spark: SparkSession,
    silver_path: str = "data/silver/taxi_trips",
    gold_path: str = "data/gold/taxi_hourly_metrics",
) -> dict:
    """
    Đọc Silver table, tính toán các chỉ số theo giờ và ghi vào Gold Delta table.

    Tham số:
        spark (SparkSession): Phiên làm việc Spark đang chạy.
        silver_path (str): Đường dẫn bảng Delta Silver.
        gold_path (str): Đường dẫn lưu trữ bảng Delta Gold.

    Trả về:
        dict: Tóm tắt kết quả {"silver_input_count": int, "gold_output_count": int}
    """
    print("=== [TASK D1] BẮT ĐẦU TIẾN TRÌNH GOLD AGGREGATION ===")

    # ============================================================
    # 1. Read Silver
    # ============================================================

    silver_df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    silver_input_count = silver_df.count()
    print(f"--> Đọc Silver table: {silver_input_count} bản ghi")

    # ============================================================
    # 2. Create hourly timestamp
    # ============================================================

    silver_df = silver_df.withColumn(
        "pickup_hour",
        date_trunc("hour", col("tpep_pickup_datetime"))
    )

    # ============================================================
    # 3. Calculate trip-level metrics
    # ============================================================

    silver_df = silver_df.withColumn(
        "tip_pct_trip",
        when(
            col("fare_amount") > 0,
            col("tip_amount") / col("fare_amount")
        )
    )

    silver_df = silver_df.withColumn(
        "driver_earnings_trip",
        col("fare_amount")
        + col("tip_amount")
        + col("extra")
    )

    # ============================================================
    # 4. Aggregate to hourly Gold
    # ============================================================

    gold_df = (
        silver_df
        .groupBy("pickup_hour")
        .agg(
            count("*").alias("total_trips"),

            sum("fare_amount").alias("total_fare"),

            sum("tip_amount").alias("total_tip"),

            avg("tip_pct_trip").alias("tip_pct"),

            sum("driver_earnings_trip").alias("driver_earnings")
        )
    )

    # ============================================================
    # 5. Write Gold
    # ============================================================

    (
        gold_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path)
    )

    gold_output_count = spark.read.format("delta").load(gold_path).count()
    print(f"--> Gold table: {gold_output_count} nhóm giờ")
    print("\n[SUCCESS] Gold table rebuilt successfully.")

    return {
        "silver_input_count": silver_input_count,
        "gold_output_count": gold_output_count,
    }


if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.pop("SPARK_HOME", None)

    builder = (
        SparkSession.builder
        .appName("GoldAggregation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
    )

    if configure_spark_with_delta_pip is not None:
        spark = configure_spark_with_delta_pip(
            builder,
            extra_packages=["io.delta:delta-spark_2.12:3.2.0"]
        ).getOrCreate()
    else:
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        result = run_gold(spark)
        print(f"\nKết quả: {result}")
    finally:
        spark.stop()