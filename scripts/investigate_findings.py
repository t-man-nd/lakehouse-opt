#!/usr/bin/env python3
"""Investigate the questions raised by the first full run.

Profiling told us *what* is unusual; this script asks *why*, so the report can
explain each finding instead of just reporting a count.

Questions
---------
1. TOTAL_MISMATCH flags 25-32 % of all rows. Is the rule wrong, or the data?
   Prints the distribution of gap = total_amount - sum(components) and the most
   common gap values, split by year and by whether the row has the NULL block.
2. Zero-duration trips grew 30x in 2025. Is it the new VendorID 7?
3. Negative fares quadrupled in 2025, and most are now unpaired. Which vendor,
   payment type and rate code do they come from?
4. Roughly a fifth of 2025 rows have NULL RatecodeID/passenger_count/
   congestion_surcharge/Airport_fee and payment_type 0. Which vendor sends them?

Usage
-----
    python scripts/investigate_findings.py                 # writes docs/evidence/findings.md
    python scripts/investigate_findings.py --months 2025-03
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from src.config import TOTAL_COMPONENT_COLUMNS, get_paths  # noqa: E402
from src.delta_utils import write_json  # noqa: E402

NULL_BLOCK_COLUMNS = ("RatecodeID", "passenger_count", "congestion_surcharge", "store_and_fwd_flag")


def add_gap(bronze: DataFrame) -> DataFrame:
    """gap = total_amount - sum of the itemised components (NULLs treated as 0)."""
    present = [c for c in TOTAL_COMPONENT_COLUMNS if c in bronze.columns]
    component_sum = F.coalesce(F.col(present[0]), F.lit(0.0))
    for name in present[1:]:
        component_sum = component_sum + F.coalesce(F.col(name), F.lit(0.0))
    null_block = F.lit(True)
    for name in NULL_BLOCK_COLUMNS:
        if name in bronze.columns:
            null_block = null_block & F.col(name).isNull()
    return (bronze.withColumn("component_sum", F.round(component_sum, 2))
            .withColumn("gap", F.round(F.col("total_amount") - component_sum, 2))
            .withColumn("has_null_block", null_block))


def q1_total_mismatch(bronze: DataFrame, tolerance: float = 0.05) -> Dict[str, Any]:
    df = add_gap(bronze)
    mismatched = df.filter(F.abs("gap") > tolerance)
    per_month = mismatched.groupBy("_source_month").agg(
        F.count(F.lit(1)).alias("mismatched_rows"),
        F.transform(F.percentile_approx("gap", [0.01, 0.25, 0.5, 0.75, 0.99], 1000),
                    lambda x: F.round(x, 2)).alias("gap_p01_p25_p50_p75_p99"),
        F.round(F.min("gap"), 2).alias("gap_min"), F.round(F.max("gap"), 2).alias("gap_max"),
        F.sum(F.when(F.col("gap") > 0, 1).otherwise(0)).alias("total_exceeds_components"),
        F.sum(F.when(F.col("has_null_block"), 1).otherwise(0)).alias("with_null_block"),
    ).orderBy("_source_month")
    common = (mismatched.groupBy("gap").count().orderBy(F.desc("count")).limit(15)
              .withColumn("gap", F.col("gap").cast("string")))
    by_year = mismatched.groupBy(F.substring("_source_month", 1, 4).alias("year"), "gap").count()
    top_by_year = (by_year.withColumn("rank", F.row_number().over(
        Window.partitionBy("year").orderBy(F.desc("count"))))
        .filter("rank <= 5").drop("rank").orderBy("year", F.desc("count")))
    return {
        "per_month": [r.asDict() for r in per_month.collect()],
        "most_common_gaps": [r.asDict() for r in common.collect()],
        "most_common_gaps_by_year": [r.asDict() for r in top_by_year.collect()],
    }


def q2_zero_duration(bronze: DataFrame) -> List[Dict[str, Any]]:
    zero = bronze.filter(F.col("tpep_dropoff_datetime") == F.col("tpep_pickup_datetime"))
    return [r.asDict() for r in zero.groupBy("_source_month", "VendorID").count()
            .orderBy("_source_month", F.desc("count")).collect()]


def q3_negative_fares(bronze: DataFrame) -> List[Dict[str, Any]]:
    negative = bronze.filter(F.col("fare_amount") < 0)
    return [r.asDict() for r in negative.groupBy("_source_month", "VendorID", "payment_type").agg(
        F.count(F.lit(1)).alias("rows"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.sum(F.when(F.col("RatecodeID").isNull(), 1).otherwise(0)).alias("null_ratecode"),
    ).orderBy("_source_month", F.desc("rows")).collect()]


def q4_null_block(bronze: DataFrame) -> List[Dict[str, Any]]:
    df = add_gap(bronze)
    return [r.asDict() for r in df.filter("has_null_block").groupBy("_source_month", "VendorID", "payment_type")
            .agg(F.count(F.lit(1)).alias("rows"),
                 F.round(F.avg("total_amount"), 2).alias("avg_total"),
                 F.round(F.avg("fare_amount"), 2).alias("avg_fare"))
            .orderBy("_source_month", F.desc("rows")).collect()]


def render(results: Dict[str, Any]) -> str:
    def table(rows: List[Dict[str, Any]], limit: int = 40) -> List[str]:
        if not rows:
            return ["_(no rows)_", ""]
        headers = list(rows[0])
        out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
        for r in rows[:limit]:
            out.append("| " + " | ".join(f"{r[h]:,}" if isinstance(r[h], int) else str(r[h]) for h in headers) + " |")
        if len(rows) > limit:
            out.append(f"_… {len(rows) - limit} more rows in findings.json_")
        return out + [""]

    lines = ["# Findings: why the unusual numbers look the way they do", "",
             "Generated by `scripts/investigate_findings.py` from Bronze (source-faithful rows).", "",
             "## 1. TOTAL_MISMATCH: total_amount vs the itemised components", "",
             "`gap = total_amount - sum(components)`. A gap concentrated on a few repeated values means a "
             "charge the files do not itemise; gaps spread continuously would mean our component list is wrong.", ""]
    lines += table(results["total_mismatch"]["per_month"])
    lines += ["### Most common gap values (all months)", ""]
    lines += table(results["total_mismatch"]["most_common_gaps"])
    lines += ["### Most common gap values per year", ""]
    lines += table(results["total_mismatch"]["most_common_gaps_by_year"])
    lines += ["## 2. Zero-duration trips by vendor", "",
              "Zero-duration trips grew about 30x during 2025 Q1; this shows which vendor reports them.", ""]
    lines += table(results["zero_duration"])
    lines += ["## 3. Negative fares by vendor and payment type", "",
              "Negative fares drive almost every hard reject (INVALID_FARE).", ""]
    lines += table(results["negative_fares"])
    lines += ["## 4. Rows with the NULL block (RatecodeID, passenger_count, congestion_surcharge, store_and_fwd_flag)", ""]
    lines += table(results["null_block"])
    lines += ["## How to use this", "",
              "Quote the gap distribution when justifying the TOTAL_MISMATCH threshold in docs/DQ_RULES.md, and "
              "name the vendor behind the zero-duration and negative-fare growth in the report's findings section.", ""]
    return "\n".join(lines) + "\n"


def main() -> None:
    from src.spark_session import create_spark

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--months", nargs="+", default=None)
    args = parser.parse_args()
    paths = get_paths()
    spark = create_spark("investigate-findings")
    try:
        bronze = spark.read.format("delta").load(paths.bronze_trips).filter(F.col("_batch_id").startswith("yellow_"))
        if args.months:
            bronze = bronze.filter(F.col("_source_month").isin(args.months))
        results = {
            "total_mismatch": q1_total_mismatch(bronze),
            "zero_duration": q2_zero_duration(bronze),
            "negative_fares": q3_negative_fares(bronze),
            "null_block": q4_null_block(bronze),
        }
        write_json(paths.evidence_dir / "findings.json", results)
        out = paths.evidence_dir / "findings.md"
        out.write_text(render(results), encoding="utf-8")
        print(f"Wrote {out}")
    finally:
        try:
            spark.stop()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
