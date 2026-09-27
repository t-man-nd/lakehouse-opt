"""Central configuration for the real-data (TLC) lane of the lakehouse.

Every path, source URL, month and data-quality threshold lives here so the
pipeline, tests and report generator agree on one definition.

Environment overrides:
    LAKEHOUSE_ROOT   project root used for data/ and docs/ (default: repo root)
    LAKEHOUSE_DIR    where Delta tables are written (default: <root>/data/lakehouse)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]

# --------------------------------------------------------------------------
# Scope: two comparable quarters, one year apart.
# 2024 files have 19 columns; 2025 files add cbd_congestion_fee, so loading
# 2024 first produces a *real* schema-evolution event.
# --------------------------------------------------------------------------
DEFAULT_MONTHS: List[str] = [
    "2024-01", "2024-02", "2024-03",
    "2025-01", "2025-02", "2025-03",
]

TRIP_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
# MTA dataset listing the taxi zones inside the Congestion Relief Zone (CBD).
CBD_ZONES_URL = "https://data.ny.gov/api/views/yfdc-w5jh/rows.csv?accessType=DOWNLOAD"
# TLC monthly pickups/drop-offs by taxi zone and industry (external reconciliation).
TLC_ZONE_MONTHLY_URL = "https://data.cityofnewyork.us/api/views/c5iv-bn4s/rows.csv?accessType=DOWNLOAD"

# Congestion pricing (CBD tolling) start date; trips before it cannot carry the fee.
CBD_POLICY_START = "2025-01-05"

# Canonical column names and the *landing* types used in Bronze.
# Bronze keeps every row and every source column; it only harmonises names
# (e.g. airport_fee -> Airport_fee) and widens integer/decimal types so that
# files from different years can be appended to one Delta table.
CANONICAL_LANDING_TYPES: Dict[str, str] = {
    "VendorID": "bigint",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "bigint",
    "trip_distance": "double",
    "RatecodeID": "bigint",
    "store_and_fwd_flag": "string",
    "PULocationID": "bigint",
    "DOLocationID": "bigint",
    "payment_type": "bigint",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "Airport_fee": "double",
    "cbd_congestion_fee": "double",  # only present in 2025+ files
}

# Silver target types (ANSI-safe try_cast from Bronze).
SILVER_TYPES: Dict[str, str] = {
    "VendorID": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int",
    "trip_distance": "double",
    "RatecodeID": "int",
    "store_and_fwd_flag": "string",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "Airport_fee": "double",
    "cbd_congestion_fee": "double",
}

# Columns that identify a trip event. Money columns are deliberately excluded
# so a fare/tip correction updates the same key instead of inserting a new one.
TRIP_KEY_COLUMNS: Tuple[str, ...] = (
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "PULocationID", "DOLocationID",
)

# Money columns summed to check total_amount.
TOTAL_COMPONENT_COLUMNS: Tuple[str, ...] = (
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "congestion_surcharge", "Airport_fee",
    "cbd_congestion_fee",
)

# Charges that TLC records inconsistently: sometimes itemised and included in total_amount,
# sometimes itemised but excluded from it, sometimes charged without being itemised at all
# (the payment_type 0 rows, where congestion_surcharge is NULL). Evidence:
# docs/evidence/findings.md - 90.7 % of TOTAL_MISMATCH gaps are exactly these amounts.
UNCERTAIN_COMPONENT_COLUMNS: Tuple[str, ...] = ("congestion_surcharge", "cbd_congestion_fee")
# Standard yellow-taxi amounts, used only to bound a row whose value is NULL.
STANDARD_SURCHARGE_AMOUNTS = {"congestion_surcharge": 2.50, "cbd_congestion_fee": 0.75}

METADATA_COLUMNS: Tuple[str, ...] = (
    "_ingest_ts", "_source_file", "_batch_id", "_source_month", "_file_sha256",
)

DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"


@dataclass(frozen=True)
class DQThresholds:
    """Initial thresholds for TLC-v2 rules.

    These are starting values. Run the profiling step first and adjust them
    using docs/profile/bronze_profile.md; record the reason for any change in
    docs/DQ_RULES.md so the report can cite the evidence.
    """

    late_window_days: int = 31          # pickups up to 31 days before the file month = late arrival
    lead_tolerance_hours: int = 24      # pickups up to 24h after month end tolerated
    max_distance_miles: float = 100.0   # soft flag above this
    max_duration_hours: float = 4.0     # soft flag above this
    max_speed_mph: float = 80.0         # soft flag above this (only when duration >= 60 s)
    total_mismatch_tolerance: float = 0.05
    unknown_zone_ids: Tuple[int, ...] = (264, 265)
    max_valid_zone_id: int = 263
    valid_ratecodes: Tuple[int, ...] = (1, 2, 3, 4, 5, 6)


# Soft flags that Gold excludes from averages (the rows stay in Silver).
GOLD_EXCLUDED_FLAGS: Tuple[str, ...] = ("REVERSED", "IMPLAUSIBLE_SPEED", "EXTREME_DISTANCE")


@dataclass(frozen=True)
class LakehousePaths:
    root: Path
    lake: Path

    # ---- raw landing (files, not Delta) ----
    @property
    def raw_tlc_dir(self) -> Path:
        return self.root / "data" / "raw" / "tlc"

    @property
    def raw_ref_dir(self) -> Path:
        return self.root / "data" / "raw" / "ref"

    @property
    def raw_cdc_dir(self) -> Path:
        return self.root / "data" / "raw" / "cdc"

    @property
    def source_manifest(self) -> Path:
        return self.root / "docs" / "source_data_manifest.json"

    # ---- Delta tables ----
    def table(self, layer: str, name: str) -> str:
        return str(self.lake / layer / name)

    @property
    def bronze_trips(self) -> str:
        return self.table("bronze", "yellow_trips")

    @property
    def bronze_zone_lookup(self) -> str:
        return self.table("bronze", "zone_lookup")

    @property
    def bronze_cbd_zones(self) -> str:
        return self.table("bronze", "cbd_zones")

    @property
    def silver_trips(self) -> str:
        return self.table("silver", "yellow_trips")

    @property
    def silver_rejected(self) -> str:
        return self.table("silver", "yellow_trips_rejected")

    @property
    def silver_dim_zone(self) -> str:
        return self.table("silver", "dim_zone")

    @property
    def silver_batch_log(self) -> str:
        return self.table("ops", "silver_batch_log")

    @property
    def gold_zone_hourly(self) -> str:
        return self.table("gold", "zone_hourly_metrics")

    @property
    def gold_cbd_monthly(self) -> str:
        return self.table("gold", "cbd_flow_monthly")

    @property
    def gold_cbd_yoy(self) -> str:
        return self.table("gold", "cbd_flow_yoy")

    @property
    def gold_dq_monthly(self) -> str:
        return self.table("gold", "dq_monthly")

    @property
    def benchmark_dir(self) -> Path:
        return self.lake / "benchmark"

    @property
    def scratch_dir(self) -> Path:
        return self.lake / "_scratch"

    # ---- evidence / docs ----
    @property
    def docs_dir(self) -> Path:
        return self.root / "docs"

    @property
    def evidence_dir(self) -> Path:
        return self.root / "docs" / "evidence"

    @property
    def profile_dir(self) -> Path:
        return self.root / "docs" / "profile"


def get_paths(root: Optional[os.PathLike] = None, lake: Optional[os.PathLike] = None) -> LakehousePaths:
    """Resolve project paths, honouring LAKEHOUSE_ROOT / LAKEHOUSE_DIR."""
    root_path = Path(root or os.environ.get("LAKEHOUSE_ROOT", REPO_ROOT)).resolve()
    lake_path = Path(lake or os.environ.get("LAKEHOUSE_DIR", root_path / "data" / "lakehouse")).resolve()
    return LakehousePaths(root=root_path, lake=lake_path)


def trip_file_name(month: str) -> str:
    return f"yellow_tripdata_{month}.parquet"
