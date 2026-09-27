"""TLC-v2 data-quality contract, implemented as pure DataFrame functions.

Nothing here reads or writes Delta, so every rule is unit-testable with plain
Spark. Two outcomes exist for a row:

* Hard failure -> quarantined with exactly one `reject_reason` (first rule that fires).
  The row cannot be placed or priced reliably.
* Soft issue   -> kept in Silver, listed in the `dq_flags` array.
  The row is a real trip with a questionable attribute; each Gold metric
  decides whether to exclude it. This avoids over-cleaning.

Rule order and thresholds are documented in docs/DQ_RULES.md and should be
confirmed against docs/profile/bronze_profile.md before the final run.

The B1-v1.0 contract (4 rules, used by the synthetic fixture lane) lives in
src/silver.py and is intentionally left untouched.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, TimestampNTZType, TimestampType

from src.config import (
    CANONICAL_LANDING_TYPES, DATE_FORMAT, METADATA_COLUMNS, SILVER_TYPES,
    STANDARD_SURCHARGE_AMOUNTS, TOTAL_COMPONENT_COLUMNS, UNCERTAIN_COMPONENT_COLUMNS, DQThresholds,
)

HARD_REASONS: Tuple[str, ...] = (
    "INVALID_PICKUP_DATETIME",
    "INVALID_DROPOFF_DATETIME",
    "MISSING_LOCATION",
    "INVALID_FARE",
    "NEGATIVE_DURATION",
    "OUT_OF_PERIOD",
    "DUPLICATE_TRIP",
    "KEY_COLLISION",
)

SOFT_FLAGS: Tuple[str, ...] = (
    "LATE_ARRIVAL",
    "AFTER_SOURCE_MONTH",
    "ZERO_DURATION",
    "LONG_DURATION",
    "ZERO_DISTANCE",
    "EXTREME_DISTANCE",
    "IMPLAUSIBLE_SPEED",
    "PASSENGER_COUNT_MISSING_OR_ZERO",
    "UNKNOWN_RATECODE",
    "UNKNOWN_ZONE",
    "NEGATIVE_COMPONENT",
    "TOTAL_MISMATCH",
    "REVERSED",
)

TYPED = "_t"  # struct column holding the ANSI-safe typed values


def quote(name: str) -> Column:
    return F.col("`" + name.replace("`", "``") + "`")


# ----------------------------------------------------------------------------
# Bronze helpers
# ----------------------------------------------------------------------------
def harmonize_bronze_columns(df: DataFrame) -> Tuple[DataFrame, Dict[str, List[str]]]:
    """Align one source file to the canonical Bronze landing schema.

    * Renames case variants to the canonical name (airport_fee -> Airport_fee).
    * Widens canonical columns to the landing type (e.g. int32 -> bigint,
      timestamp_ntz -> timestamp). Widening never drops information.
    * Leaves unknown columns untouched so genuinely new source columns reach
      Bronze and trigger schema evolution there.
    """
    canonical_by_lower = {name.lower(): name for name in CANONICAL_LANDING_TYPES}
    renamed: List[str] = []
    casted: List[str] = []
    for column in df.columns:
        target = canonical_by_lower.get(column.lower())
        if target and target != column:
            df = df.withColumnRenamed(column, target)
            renamed.append(f"{column}->{target}")
    for field in df.schema.fields:
        target_type = CANONICAL_LANDING_TYPES.get(field.name)
        if target_type and field.dataType.simpleString() != target_type:
            df = df.withColumn(field.name, quote(field.name).cast(target_type))
            casted.append(f"{field.name}:{field.dataType.simpleString()}->{target_type}")
    return df, {"renamed": renamed, "casted": casted}


# ----------------------------------------------------------------------------
# Typing and keys
# ----------------------------------------------------------------------------
def parse_timestamp(df: DataFrame, name: str) -> Column:
    """Timestamp columns pass through; strings must match yyyy-MM-dd HH:mm:ss exactly."""
    dtype = df.schema[name].dataType
    if isinstance(dtype, (TimestampType, TimestampNTZType, DateType)):
        return quote(name).cast("timestamp")
    if isinstance(dtype, StringType):
        return F.try_to_timestamp(quote(name), F.lit(DATE_FORMAT))
    return F.try_to_timestamp(quote(name).try_cast("string"), F.lit(DATE_FORMAT))


def typed_columns(df: DataFrame) -> List[str]:
    """Canonical columns present in this batch, in canonical order.

    Only present columns are typed: a batch whose source file had no
    cbd_congestion_fee must not invent that column in Silver.
    """
    return [name for name in SILVER_TYPES if name in df.columns]


def extra_columns(df: DataFrame) -> List[str]:
    """Source columns that are neither canonical nor lineage metadata (future evolution)."""
    ignore = set(SILVER_TYPES) | set(METADATA_COLUMNS) | {TYPED}
    return [c for c in df.columns if c not in ignore]


def add_typed_struct(df: DataFrame) -> DataFrame:
    exprs = []
    for name in typed_columns(df):
        target = SILVER_TYPES[name]
        expr = parse_timestamp(df, name) if target == "timestamp" else quote(name).try_cast(target)
        exprs.append(expr.alias(name))
    return df.withColumn(TYPED, F.struct(*exprs))


def t(name: str) -> Column:
    return F.col(TYPED)[name]


def trip_key_expr() -> Column:
    """SHA-256 of the immutable trip identity: vendor, both timestamps, both zones."""
    def part(expr: Column) -> Column:
        return F.coalesce(expr, F.lit("NULL"))

    return F.sha2(F.concat_ws(
        "|",
        part(t("VendorID").cast("string")),
        part(F.date_format(t("tpep_pickup_datetime"), DATE_FORMAT)),
        part(F.date_format(t("tpep_dropoff_datetime"), DATE_FORMAT)),
        part(t("PULocationID").cast("string")),
        part(t("DOLocationID").cast("string")),
    ), 256)


def row_hash_expr(columns: List[str]) -> Column:
    """Content fingerprint of the business columns; used to detect real changes in MERGE."""
    return F.sha2(F.to_json(F.struct(*[t(c).alias(c) for c in columns])), 256)


# ----------------------------------------------------------------------------
# Classification
# ----------------------------------------------------------------------------
def _hard_reason(th: DQThresholds, month_start: Column, month_end: Column) -> Column:
    pickup, dropoff = t("tpep_pickup_datetime"), t("tpep_dropoff_datetime")
    earliest = month_start - F.expr(f"INTERVAL {th.late_window_days} DAYS")
    latest = month_end + F.expr(f"INTERVAL {th.lead_tolerance_hours} HOURS")
    return (
        F.when(pickup.isNull(), "INVALID_PICKUP_DATETIME")
        .when(dropoff.isNull(), "INVALID_DROPOFF_DATETIME")
        .when(t("PULocationID").isNull() | t("DOLocationID").isNull(), "MISSING_LOCATION")
        .when(t("fare_amount").isNull() | (t("fare_amount") <= 0), "INVALID_FARE")
        .when(dropoff < pickup, "NEGATIVE_DURATION")
        .when((pickup < earliest) | (pickup >= latest), "OUT_OF_PERIOD")
    )


def _soft_flags(df: DataFrame, th: DQThresholds, month_start: Column, month_end: Column) -> Column:
    present = set(typed_columns(df))

    def col_or_null(name: str) -> Column:
        return t(name) if name in present else F.lit(None)

    duration = F.col("trip_duration_sec")
    distance = col_or_null("trip_distance")
    speed = F.try_divide(distance, duration / F.lit(3600.0))
    zones = [col_or_null("PULocationID"), col_or_null("DOLocationID")]
    unknown_zone = F.lit(False)
    for zone in zones:
        unknown_zone = unknown_zone | zone.isin(*th.unknown_zone_ids) | (zone > th.max_valid_zone_id) | (zone < 1)
    # total_amount is compared against a *range*, not a single sum, because TLC records the
    # congestion surcharges inconsistently (see docs/evidence/findings.md):
    #   low  = the charges we are certain are in total_amount
    #   high = low + the uncertain surcharges (their value, or the standard amount when NULL)
    # TOTAL_MISMATCH fires only when total_amount falls outside [low, high] beyond the tolerance.
    certain = [c for c in TOTAL_COMPONENT_COLUMNS if c in present and c not in UNCERTAIN_COMPONENT_COLUMNS]
    low = F.coalesce(t(certain[0]), F.lit(0.0)) if certain else F.lit(0.0)
    for name in certain[1:]:
        low = low + F.coalesce(t(name), F.lit(0.0))
    high = low
    for name in UNCERTAIN_COMPONENT_COLUMNS:
        if name in present:
            high = high + F.coalesce(t(name), F.lit(STANDARD_SURCHARGE_AMOUNTS[name]))
    total = col_or_null("total_amount")
    negative_parts = [c for c in ("extra", "mta_tax", "tip_amount", "tolls_amount",
                                  "improvement_surcharge", "congestion_surcharge", "Airport_fee",
                                  "cbd_congestion_fee") if c in present]
    negative_component = F.lit(False)
    for name in negative_parts:
        negative_component = negative_component | (t(name) < 0)
    ratecode = col_or_null("RatecodeID")
    passengers = col_or_null("passenger_count")

    candidates = [
        ("LATE_ARRIVAL", t("tpep_pickup_datetime") < month_start),
        ("AFTER_SOURCE_MONTH", t("tpep_pickup_datetime") >= month_end),
        ("ZERO_DURATION", duration == 0),
        ("LONG_DURATION", duration > th.max_duration_hours * 3600),
        ("ZERO_DISTANCE", distance.isNull() | (distance <= 0)),
        ("EXTREME_DISTANCE", distance > th.max_distance_miles),
        ("IMPLAUSIBLE_SPEED", (duration >= 60) & (speed > th.max_speed_mph)),
        ("PASSENGER_COUNT_MISSING_OR_ZERO", passengers.isNull() | (passengers <= 0)),
        ("UNKNOWN_RATECODE", ratecode.isNull() | ~ratecode.isin(*th.valid_ratecodes)),
        ("UNKNOWN_ZONE", unknown_zone),
        ("NEGATIVE_COMPONENT", negative_component),
        ("TOTAL_MISMATCH", (total < low - th.total_mismatch_tolerance) | (total > high + th.total_mismatch_tolerance)),
    ]
    return F.array_compact(F.array(*[
        F.when(F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in candidates
    ]))


def classify_trips(bronze: DataFrame, thresholds: DQThresholds = DQThresholds()) -> DataFrame:
    """Classify one Bronze batch.

    Returns the Bronze columns unchanged plus:
      _t                typed struct (ANSI-safe values)
      trip_key          immutable identity hash
      row_hash          content hash of typed business columns
      trip_duration_sec dropoff - pickup in seconds
      reject_reason     first hard failure, or NULL when the row is accepted
      reject_detail     extra context (e.g. REVERSAL_OF_ACCEPTED_TRIP)
      dq_flags          array of soft flags (only meaningful for accepted rows)

    Row-order independence: duplicates keep the row with the smallest
    row_hash, so the result is identical on every run.
    """
    required = {"tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID",
                "DOLocationID", "fare_amount", "_source_month"}
    missing = required.difference(bronze.columns)
    if missing:
        raise ValueError(f"Bronze batch is missing required columns: {sorted(missing)}")
    if {TYPED, "reject_reason", "trip_key"}.intersection(bronze.columns):
        raise ValueError("Bronze batch contains reserved column names")

    th = thresholds
    df = add_typed_struct(bronze)
    business = typed_columns(bronze)
    month_start = F.to_timestamp(F.concat(F.col("_source_month"), F.lit("-01 00:00:00")), DATE_FORMAT)
    month_end = F.add_months(month_start, 1).cast("timestamp")

    df = (
        df.withColumn("trip_key", trip_key_expr())
        .withColumn("row_hash", row_hash_expr(business))
        .withColumn("trip_duration_sec",
                    F.unix_timestamp(t("tpep_dropoff_datetime")) - F.unix_timestamp(t("tpep_pickup_datetime")))
        .withColumn("reject_reason", _hard_reason(th, month_start, month_end))
        .withColumn("reject_detail", F.lit(None).cast("string"))
    )
    df = df.withColumn("dq_flags", _soft_flags(df, th, month_start, month_end))

    # ---- in-batch deduplication by trip_key (accepted candidates only) ----
    order_cols = ["row_hash"] + (["_source_file"] if "_source_file" in bronze.columns else [])
    window = Window.partitionBy("trip_key").orderBy(*order_cols)
    candidates = df.filter(F.col("reject_reason").isNull())
    ranked = (
        candidates.withColumn("_rn", F.row_number().over(window))
        .withColumn("_first_hash", F.first("row_hash").over(window))
        .withColumn(
            "reject_reason",
            F.when(F.col("_rn") == 1, F.lit(None).cast("string"))
            .when(F.col("row_hash") == F.col("_first_hash"), "DUPLICATE_TRIP")
            .otherwise("KEY_COLLISION"),
        )
        .drop("_rn", "_first_hash")
    )
    hard_rejects = df.filter(F.col("reject_reason").isNotNull())
    combined = ranked.unionByName(hard_rejects)

    # ---- reversal matching: negative-fare row with the same identity as an accepted trip ----
    accepted_fares = combined.filter(F.col("reject_reason").isNull()).select(
        "trip_key", t("fare_amount").alias("_acc_fare"))
    negatives = combined.filter(
        (F.col("reject_reason") == "INVALID_FARE") & (t("fare_amount") < 0)
    ).select("trip_key", F.abs(t("fare_amount")).alias("_neg_abs"))
    reversed_keys = (
        accepted_fares.join(negatives, "trip_key")
        .filter(F.abs(F.col("_acc_fare") - F.col("_neg_abs")) < 0.005)
        .select("trip_key").distinct().withColumn("_is_reversal_pair", F.lit(True))
    )
    combined = combined.join(reversed_keys, "trip_key", "left")
    pair = F.coalesce(F.col("_is_reversal_pair"), F.lit(False))
    combined = (
        combined.withColumn(
            "dq_flags",
            F.when(F.col("reject_reason").isNull() & pair, F.array_union(F.col("dq_flags"), F.array(F.lit("REVERSED"))))
            .otherwise(F.col("dq_flags")),
        )
        .withColumn(
            "reject_detail",
            F.when((F.col("reject_reason") == "INVALID_FARE") & (t("fare_amount") < 0) & pair,
                   F.lit("REVERSAL_OF_ACCEPTED_TRIP"))
            .when((F.col("reject_reason") == "INVALID_FARE") & (t("fare_amount") < 0), F.lit("NEGATIVE_FARE_UNPAIRED"))
            .when((F.col("reject_reason") == "INVALID_FARE") & t("fare_amount").isNull(), F.lit("NULL_FARE"))
            .when(F.col("reject_reason") == "INVALID_FARE", F.lit("ZERO_FARE"))
            .otherwise(F.col("reject_detail")),
        )
        .drop("_is_reversal_pair")
    )
    return combined


def to_silver_rows(classified: DataFrame) -> DataFrame:
    """Accepted rows in Silver shape: typed values + derived analytics columns + lineage."""
    typed_names = [f.name for f in classified.schema[TYPED].dataType.fields]
    extras = extra_columns(classified.drop("trip_key", "row_hash", "trip_duration_sec",
                                           "reject_reason", "reject_detail", "dq_flags"))
    pickup = F.col("tpep_pickup_datetime")
    return (
        classified.filter(F.col("reject_reason").isNull())
        .select(
            "trip_key",
            *[t(name).alias(name) for name in typed_names],
            *[quote(c) for c in extras],
            "trip_duration_sec", "dq_flags", "row_hash",
            *[quote(c) for c in METADATA_COLUMNS if c in classified.columns],
        )
        .withColumn("pickup_date", F.to_date(pickup))
        .withColumn("pickup_hour", F.hour(pickup))
        .withColumn("pickup_month", F.date_format(pickup, "yyyy-MM"))
        .withColumn("is_late_arrival", F.array_contains("dq_flags", "LATE_ARRIVAL"))
        .withColumn("_silver_ts", F.current_timestamp())
    )


def to_rejected_rows(classified: DataFrame, bronze_columns: List[str]) -> DataFrame:
    """Rejected rows keep their original Bronze values for audit, plus the reason."""
    return (
        classified.filter(F.col("reject_reason").isNotNull())
        .select(*[quote(c) for c in bronze_columns], "trip_key", "reject_reason", "reject_detail")
        .withColumn("_rejected_ts", F.current_timestamp())
    )
