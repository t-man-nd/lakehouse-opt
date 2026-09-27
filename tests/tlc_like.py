"""Synthetic TLC-shaped Parquet files for tests (NOT real data).

Each month gets `clean` realistic trips plus planted anomalies with exactly
known counts, and the files reproduce the real cross-year schema drift:

* 2024 files: `airport_fee` lower-case, `passenger_count` as double, no CBD fee
* 2025 files: `Airport_fee`, `passenger_count` as int64, plus `cbd_congestion_fee`
* timestamps are written without a time zone (Spark reads them as TIMESTAMP_NTZ)

Planted per month (all disjoint):
  hard: 2 null pickup, 1 null dropoff, 2 missing location, 1 zero fare,
        1 reversal (negative copy of a clean trip), 1 negative duration,
        1 out-of-period (2009), 2 exact duplicates, 1 key collision
  soft: 2 late arrivals (pickup in previous month), 1 rate code 99,
        1 unknown zone (264), 1 zero passengers, 1 REVERSED (the reversal's original)
  cross-file: when the previous month is also generated, one late arrival is
        an exact resubmission of a previous-month trip (MERGE no-op).
"""

from __future__ import annotations

import csv
import hashlib
import json
import random
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

CBD_ZONES = [161, 162, 163, 164, 230]
POLICY_START = datetime(2025, 1, 5)

EXPECTED_REJECTS = {
    "INVALID_PICKUP_DATETIME": 2, "INVALID_DROPOFF_DATETIME": 1, "MISSING_LOCATION": 2,
    "INVALID_FARE": 2, "NEGATIVE_DURATION": 1, "OUT_OF_PERIOD": 1,
    "DUPLICATE_TRIP": 2, "KEY_COLLISION": 1,
}
EXPECTED_FLAGS = {"LATE_ARRIVAL": 2, "UNKNOWN_RATECODE": 1, "UNKNOWN_ZONE": 1,
                  "PASSENGER_COUNT_MISSING_OR_ZERO": 1, "REVERSED": 1}


def month_start(month: str) -> datetime:
    return datetime.strptime(month + "-01", "%Y-%m-%d")


def previous_month(month: str) -> str:
    start = month_start(month)
    return (start - timedelta(days=1)).strftime("%Y-%m")


def price(trip: Dict, has_cbd: bool) -> Dict:
    distance = trip["trip_distance"]
    trip["fare_amount"] = round(3.0 + 2.5 * distance, 2)
    trip["tip_amount"] = round(trip["fare_amount"] * 0.2, 2) if trip["payment_type"] == 1 else 0.0
    trip["extra"], trip["mta_tax"], trip["improvement_surcharge"] = 1.0, 0.5, 1.0
    trip["tolls_amount"], trip["congestion_surcharge"], trip["airport"] = 0.0, 2.5, 0.0
    total = (trip["fare_amount"] + trip["tip_amount"] + trip["extra"] + trip["mta_tax"]
             + trip["improvement_surcharge"] + trip["tolls_amount"] + trip["congestion_surcharge"] + trip["airport"])
    if has_cbd:
        touches = trip["PULocationID"] in CBD_ZONES or trip["DOLocationID"] in CBD_ZONES
        trip["cbd_congestion_fee"] = 0.75 if touches and trip["tpep_pickup_datetime"] >= POLICY_START else 0.0
        total += trip["cbd_congestion_fee"]
    trip["total_amount"] = round(total, 2)
    return trip


def make_trip(rng: random.Random, pickup: datetime, has_cbd: bool, vendor: int) -> Dict:
    distance = round(rng.uniform(0.5, 12.0), 2)
    duration = timedelta(minutes=int(distance * 3) + 3, seconds=rng.randint(0, 59))
    pu = rng.choice(CBD_ZONES) if rng.random() < 0.4 else rng.randint(1, 263)
    do = rng.choice(CBD_ZONES) if rng.random() < 0.4 else rng.randint(1, 263)
    trip = {
        "VendorID": vendor, "tpep_pickup_datetime": pickup, "tpep_dropoff_datetime": pickup + duration,
        "passenger_count": rng.randint(1, 3), "trip_distance": distance, "RatecodeID": 1,
        "store_and_fwd_flag": "N", "PULocationID": pu, "DOLocationID": do,
        "payment_type": 1 if rng.random() < 0.7 else 2,
    }
    return price(trip, has_cbd)


def make_month(month: str, clean: int, seed: int, prev_clean: Optional[List[Dict]]) -> Dict:
    rng = random.Random(f"{seed}-{month}")
    has_cbd = month >= "2025-01"
    start = month_start(month)
    step = timedelta(minutes=(27 * 24 * 60) // (clean + 1))
    base = [make_trip(rng, start + step * (i + 1) + timedelta(seconds=i % 60), has_cbd, 1 + i % 2) for i in range(clean)]

    rows = deepcopy(base)
    planted = []

    def plant(row: Dict, label: str) -> None:
        row["_label"] = label
        planted.append(row)

    # ---- hard failures ----
    for i in range(2):
        r = deepcopy(base[10 + i]); r["tpep_pickup_datetime"] = None; r["VendorID"] = 1; plant(r, "null_pickup")
    r = deepcopy(base[12]); r["tpep_dropoff_datetime"] = None; plant(r, "null_dropoff")
    r = deepcopy(base[13]); r["PULocationID"] = None; plant(r, "missing_pu")
    r = deepcopy(base[14]); r["DOLocationID"] = None; plant(r, "missing_do")
    r = deepcopy(base[15]); r["tpep_pickup_datetime"] += timedelta(seconds=7); r["tpep_dropoff_datetime"] += timedelta(seconds=7)
    r["fare_amount"] = 0.0; plant(r, "zero_fare")
    r = deepcopy(base[16])  # reversal of an accepted trip: same identity, negative amounts
    for k in ("fare_amount", "tip_amount", "total_amount"):
        r[k] = -r[k]
    plant(r, "reversal")
    r = deepcopy(base[17]); r["tpep_pickup_datetime"] += timedelta(seconds=11)
    r["tpep_dropoff_datetime"] = r["tpep_pickup_datetime"] - timedelta(minutes=5); plant(r, "negative_duration")
    r = deepcopy(base[18]); r["tpep_pickup_datetime"] = datetime(2009, 1, 1, 12, 0, 0)
    r["tpep_dropoff_datetime"] = datetime(2009, 1, 1, 12, 20, 0); plant(r, "out_of_period")
    plant(deepcopy(base[19]), "exact_duplicate")
    plant(deepcopy(base[20]), "exact_duplicate")
    r = deepcopy(base[21]); r["trip_distance"] = round(r["trip_distance"] + 0.3, 2); r = price(r, has_cbd)
    plant(r, "key_collision")

    # ---- soft issues (accepted rows) ----
    late_start = start - timedelta(days=3)
    if prev_clean:
        plant(deepcopy(prev_clean[-1]), "late_resubmission")   # same trip already in previous file
    else:
        plant(make_trip(rng, late_start + timedelta(hours=5, seconds=17), has_cbd, 2), "late_new")
    plant(make_trip(rng, late_start + timedelta(hours=9, seconds=41), has_cbd, 1), "late_new")
    rows[30]["RatecodeID"] = 99
    rows[31]["PULocationID"] = 264
    rows[32]["passenger_count"] = 0
    rows.extend(planted)
    rng.shuffle(rows)
    return {"rows": rows, "base": base, "has_cbd": has_cbd, "resubmission": bool(prev_clean)}


def write_month_parquet(month_data: Dict, path: Path) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq

    has_cbd = month_data["has_cbd"]
    rows = month_data["rows"]
    airport_name = "Airport_fee" if has_cbd else "airport_fee"
    columns = {
        "VendorID": pa.array([r["VendorID"] for r in rows], pa.int32()),
        "tpep_pickup_datetime": pa.array([r["tpep_pickup_datetime"] for r in rows], pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array([r["tpep_dropoff_datetime"] for r in rows], pa.timestamp("us")),
        "passenger_count": (pa.array([r["passenger_count"] for r in rows], pa.int64()) if has_cbd
                            else pa.array([float(r["passenger_count"]) for r in rows], pa.float64())),
        "trip_distance": pa.array([r["trip_distance"] for r in rows], pa.float64()),
        "RatecodeID": pa.array([r["RatecodeID"] for r in rows], pa.int64()),
        "store_and_fwd_flag": pa.array([r["store_and_fwd_flag"] for r in rows], pa.string()),
        "PULocationID": pa.array([r["PULocationID"] for r in rows], pa.int32()),
        "DOLocationID": pa.array([r["DOLocationID"] for r in rows], pa.int32()),
        "payment_type": pa.array([r["payment_type"] for r in rows], pa.int64()),
    }
    for name in ("fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
                 "total_amount", "congestion_surcharge"):
        columns[name] = pa.array([r[name] for r in rows], pa.float64())
    columns[airport_name] = pa.array([r["airport"] for r in rows], pa.float64())
    if has_cbd:
        columns["cbd_congestion_fee"] = pa.array([r["cbd_congestion_fee"] for r in rows], pa.float64())
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns), path)
    return len(rows)


def write_reference_files(ref_dir: Path) -> None:
    ref_dir.mkdir(parents=True, exist_ok=True)
    with open(ref_dir / "taxi_zone_lookup.csv", "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["LocationID", "Borough", "Zone", "service_zone"])
        for i in range(1, 264):
            borough = "Manhattan" if i in CBD_ZONES or i % 3 == 0 else "Queens"
            writer.writerow([i, borough, f"Zone {i}", "Yellow Zone" if borough == "Manhattan" else "Boro Zone"])
        writer.writerow([264, "Unknown", "N/A", "N/A"])
        writer.writerow([265, "N/A", "Outside of NYC", "N/A"])
    with open(ref_dir / "cbd_zones.csv", "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["LocationID"])
        writer.writerows([[z] for z in CBD_ZONES])


def build_dataset(root: Path, months: List[str], clean: int = 120, seed: int = 7) -> Dict:
    """Write trip files, reference files and a source manifest under `root`.

    Returns expected counts per month for the tests.
    """
    raw = root / "data" / "raw" / "tlc"
    manifest: Dict[str, Dict] = {}
    expected: Dict[str, Dict] = {}
    generated: Dict[str, Dict] = {}
    for month in sorted(months):
        prev = generated.get(previous_month(month))
        data = make_month(month, clean, seed, prev["base"] if prev else None)
        generated[month] = data
        path = raw / f"yellow_tripdata_{month}.parquet"
        rows = write_month_parquet(data, path)
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        manifest[month] = {"file": path.name, "rows": rows, "sha256": sha, "bytes": path.stat().st_size}
        rejected = sum(EXPECTED_REJECTS.values())
        expected[month] = {
            "rows": rows, "rejected": rejected, "accepted": rows - rejected,
            "noop": 1 if data["resubmission"] else 0,
            "reject_reasons": dict(EXPECTED_REJECTS), "flags": dict(EXPECTED_FLAGS), "has_cbd": data["has_cbd"],
        }
    write_reference_files(root / "data" / "raw" / "ref")
    (root / "docs").mkdir(parents=True, exist_ok=True)
    (root / "docs" / "source_data_manifest.json").write_text(
        json.dumps({"months": sorted(months), "trip_files": manifest}, indent=2, default=str))
    return {"manifest": manifest, "expected": expected}
