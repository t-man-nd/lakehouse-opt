#!/usr/bin/env python3
"""
B1 - Data Contract & Dirty JSON Generator
Delta Lakehouse Architecture & Storage Optimization

Purpose
-------
Create 3 deterministic JSON Lines batches for the Bronze layer. Each batch
contains controlled data-quality defects that downstream B3 can detect and
reconcile exactly.

Default dirty-data profile per 1,000 clean base rows:
- 20 duplicate excess rows (2%)
- 10 malformed pickup datetimes (1%)
- 10 missing PULocationID rows (1%)
- 10 missing DOLocationID rows (1%)
- 10 fare_amount <= 0 rows (1%)

The 5 index sets are disjoint. Therefore each intentionally bad base row has
one primary business-quality error, while duplicated rows are copied only from
otherwise-valid rows.

The script samples real NYC Yellow Taxi Parquet files (Jan/Feb/Mar 2025)
located by default in `./data/` (or specified via `--source-files`).

No Bronze/Silver/Delta logic is implemented here; that belongs to B2/B3.
"""

from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_BASE_ROWS = 1000
DEFAULT_SEED = 42

# B1-controlled counts for each 1,000-row base batch.
DEFAULT_DUPLICATES = 20
DEFAULT_MALFORMED_DATES = 10
DEFAULT_MISSING_PU = 10
DEFAULT_MISSING_DO = 10
DEFAULT_INVALID_FARES = 10

DEFAULT_SOURCE_CANDIDATES = [
    # 1. Standard group repo root layout: data/raw/
    [
        Path("data/raw/yellow_tripdata_2025-01.parquet"),
        Path("data/raw/yellow_tripdata_2025-02.parquet"),
        Path("data/raw/yellow_tripdata_2025-03.parquet"),
    ],
    # 2. Executing from inside b1-data-generator/ subfolder: ../data/raw/
    [
        Path("../data/raw/yellow_tripdata_2025-01.parquet"),
        Path("../data/raw/yellow_tripdata_2025-02.parquet"),
        Path("../data/raw/yellow_tripdata_2025-03.parquet"),
    ],
    # 3. Flat data/ directory layout
    [
        Path("data/yellow_tripdata_2025-01.parquet"),
        Path("data/yellow_tripdata_2025-02.parquet"),
        Path("data/yellow_tripdata_2025-03.parquet"),
    ],
    # 4. data/source/ layout
    [
        Path("data/source/yellow_tripdata_2025-01.parquet"),
        Path("data/source/yellow_tripdata_2025-02.parquet"),
        Path("data/source/yellow_tripdata_2025-03.parquet"),
    ],
]

SOURCE_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
]


def _convert_to_json_safe(value: Any) -> Any:
    """Convert Arrow/Python scalar values into JSON-safe primitives."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime(DATE_FORMAT)
    # pyarrow/numpy scalars often expose item()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_datetime_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime(DATE_FORMAT)
    text = str(value)
    # Common Parquet/Pandas representation: 2025-01-01 00:00:00
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).strftime(DATE_FORMAT)
    except ValueError:
        try:
            return datetime.strptime(text[:19], DATE_FORMAT).strftime(DATE_FORMAT)
        except ValueError:
            return None


def normalize_source_record(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize one official-source row into the B1 raw contract.

    Only clean rows are accepted as the base population so every later defect
    is controlled by this generator rather than inherited accidentally.
    """
    pickup = _format_datetime_string(row.get("tpep_pickup_datetime"))
    dropoff = _format_datetime_string(row.get("tpep_dropoff_datetime"))
    pu = _parse_int(row.get("PULocationID"))
    do = _parse_int(row.get("DOLocationID"))
    fare = _parse_float(row.get("fare_amount"))

    if pickup is None or dropoff is None:
        return None
    if pu is None or do is None:
        return None
    if fare is None or fare <= 0:
        return None

    record = {
        "VendorID": _parse_int(row.get("VendorID")),
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": dropoff,
        "passenger_count": _parse_int(row.get("passenger_count")),
        "trip_distance": _parse_float(row.get("trip_distance")),
        "RatecodeID": _parse_int(row.get("RatecodeID")),
        "store_and_fwd_flag": _convert_to_json_safe(row.get("store_and_fwd_flag")),
        "PULocationID": pu,
        "DOLocationID": do,
        "payment_type": _parse_int(row.get("payment_type")),
        "fare_amount": fare,
        "extra": _parse_float(row.get("extra")),
        "mta_tax": _parse_float(row.get("mta_tax")),
        "tip_amount": _parse_float(row.get("tip_amount")),
        "tolls_amount": _parse_float(row.get("tolls_amount")),
        "improvement_surcharge": _parse_float(row.get("improvement_surcharge")),
        "total_amount": _parse_float(row.get("total_amount")),
        "congestion_surcharge": _parse_float(row.get("congestion_surcharge")),
        "Airport_fee": _parse_float(row.get("Airport_fee")),
    }
    return record


def load_clean_rows_from_parquet(path: Path, needed: int) -> List[Dict[str, Any]]:
    """
    Read only enough rows from a local NYC Parquet file.

    Requires pyarrow only when --source-files is used.
    """
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "pyarrow is required for --source-files mode. "
            "Install it with: pip install pyarrow"
        ) from exc

    parquet = pq.ParquetFile(path)
    available = set(parquet.schema_arrow.names)
    columns = [c for c in SOURCE_COLUMNS if c in available]

    required = {
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
    }
    missing_required = required - available
    if missing_required:
        raise ValueError(f"{path} is missing required columns: {sorted(missing_required)}")

    clean: List[Dict[str, Any]] = []
    for batch in parquet.iter_batches(batch_size=50_000, columns=columns):
        for row in batch.to_pylist():
            normalized = normalize_source_record(row)
            if normalized is not None:
                clean.append(normalized)
                if len(clean) >= needed:
                    return clean

    raise ValueError(
        f"Could only find {len(clean)} clean rows in {path}; need {needed}."
    )


def resolve_source_files(provided: Optional[Sequence[Path]]) -> List[Path]:
    """Resolve and validate the 3 required NYC 2025 Parquet files with smart auto-detection."""
    if provided:
        targets = [Path(p) for p in provided]
    else:
        targets = None
        for candidate_group in DEFAULT_SOURCE_CANDIDATES:
            if all(p.exists() for p in candidate_group):
                targets = candidate_group
                break
        if targets is None:
            targets = DEFAULT_SOURCE_CANDIDATES[0]

    for path in targets:
        if not path.exists():
            raise FileNotFoundError(
                f"Required Parquet source file not found: {path}\n"
                "Please ensure the official 2025 Jan/Feb/Mar NYC Yellow Taxi Parquet files "
                "are available in data/raw/ or specify their location via --source-files."
            )
    return targets


def assign_trip_ids(records: List[Dict[str, Any]], batch_no: int) -> None:
    """
    Assign an explicit stable synthetic key.

    IMPORTANT: trip_id does not depend on fare_amount or tip_amount.
    Downstream CDC can reuse the same trip_id when fare/tip changes.
    """
    for idx, record in enumerate(records, start=1):
        record["trip_id"] = f"trip_2025{batch_no:02d}_{idx:06d}"


def choose_disjoint_error_sets(
    n: int,
    rng: random.Random,
    malformed_dates: int,
    missing_pu: int,
    missing_do: int,
    invalid_fares: int,
    duplicates: int,
) -> Dict[str, List[int]]:
    total_needed = malformed_dates + missing_pu + missing_do + invalid_fares + duplicates
    if total_needed > n:
        raise ValueError(
            f"Need {total_needed} disjoint source rows for defects, but batch has only {n}."
        )

    indices = list(range(n))
    rng.shuffle(indices)

    cursor = 0
    result: Dict[str, List[int]] = {}
    for name, size in [
        ("malformed_date", malformed_dates),
        ("missing_pu", missing_pu),
        ("missing_do", missing_do),
        ("invalid_fare", invalid_fares),
        ("duplicate_source", duplicates),
    ]:
        result[name] = indices[cursor : cursor + size]
        cursor += size
    return result


def inject_defects(
    records: List[Dict[str, Any]],
    batch_no: int,
    seed: int,
    malformed_dates: int,
    missing_pu: int,
    missing_do: int,
    invalid_fares: int,
    duplicates: int,
) -> List[Dict[str, Any]]:
    rng = random.Random(seed + batch_no * 100_000)
    sets = choose_disjoint_error_sets(
        len(records),
        rng,
        malformed_dates,
        missing_pu,
        missing_do,
        invalid_fares,
        duplicates,
    )

    malformed_values = ["not-a-date", "2024-13-40 25:61:00"]
    for pos, idx in enumerate(sets["malformed_date"]):
        records[idx]["tpep_pickup_datetime"] = malformed_values[pos % len(malformed_values)]

    for idx in sets["missing_pu"]:
        records[idx]["PULocationID"] = None

    for idx in sets["missing_do"]:
        records[idx]["DOLocationID"] = None

    for pos, idx in enumerate(sets["invalid_fare"]):
        records[idx]["fare_amount"] = 0.0 if pos % 2 == 0 else -5.0

    duplicate_rows = [deepcopy(records[idx]) for idx in sets["duplicate_source"]]
    output = records + duplicate_rows

    # Keep file order deterministic but avoid placing every duplicate at the end.
    rng.shuffle(output)
    return output


def check_valid_datetime(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.strptime(value, DATE_FORMAT)
        return True
    except ValueError:
        return False


def calculate_manifest(records: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    counts = Counter(r["trip_id"] for r in records)
    duplicate_excess = sum(max(0, count - 1) for count in counts.values())

    malformed_date = sum(
        1 for r in records if not check_valid_datetime(r.get("tpep_pickup_datetime"))
    )
    missing_pu = sum(1 for r in records if r.get("PULocationID") is None)
    missing_do = sum(1 for r in records if r.get("DOLocationID") is None)
    invalid_fare = sum(
        1
        for r in records
        if r.get("fare_amount") is None or float(r["fare_amount"]) <= 0
    )

    # Duplicates are deliberately copied only from valid rows, so these counts
    # do not overlap with malformed/missing/invalid-fare rows.
    rejected = (
        duplicate_excess
        + malformed_date
        + missing_pu
        + missing_do
        + invalid_fare
    )

    return {
        "raw_rows": len(records),
        "unique_trip_ids": len(counts),
        "duplicate_excess_rows": duplicate_excess,
        "malformed_date_rows": malformed_date,
        "missing_pu_rows": missing_pu,
        "missing_do_rows": missing_do,
        "missing_location_rows": missing_pu + missing_do,
        "invalid_fare_rows": invalid_fare,
        "expected_rejected_rows": rejected,
        "expected_silver_rows": len(records) - rejected,
    }


def validate_manifest(
    manifest: Dict[str, int],
    base_rows: int,
    malformed_dates: int,
    missing_pu: int,
    missing_do: int,
    invalid_fares: int,
    duplicates: int,
) -> None:
    expected = {
        "raw_rows": base_rows + duplicates,
        "unique_trip_ids": base_rows,
        "duplicate_excess_rows": duplicates,
        "malformed_date_rows": malformed_dates,
        "missing_pu_rows": missing_pu,
        "missing_do_rows": missing_do,
        "missing_location_rows": missing_pu + missing_do,
        "invalid_fare_rows": invalid_fares,
        "expected_rejected_rows": (
            duplicates + malformed_dates + missing_pu + missing_do + invalid_fares
        ),
        "expected_silver_rows": (
            base_rows - malformed_dates - missing_pu - missing_do - invalid_fares
        ),
    }
    for key, value in expected.items():
        actual = manifest[key]
        assert actual == value, f"{key}: expected {value}, got {actual}"


def write_jsonl(path: Path, records: Iterable[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


def generate_one_batch(
    batch_no: int,
    output_dir: Path,
    source_file: Path,
    base_rows: int,
    seed: int,
    malformed_dates: int,
    missing_pu: int,
    missing_do: int,
    invalid_fares: int,
    duplicates: int,
) -> Dict[str, Any]:
    clean = load_clean_rows_from_parquet(source_file, base_rows)
    source_mode = source_file.as_posix()

    clean = [deepcopy(r) for r in clean[:base_rows]]
    assign_trip_ids(clean, batch_no)

    output = inject_defects(
        clean,
        batch_no=batch_no,
        seed=seed,
        malformed_dates=malformed_dates,
        missing_pu=missing_pu,
        missing_do=missing_do,
        invalid_fares=invalid_fares,
        duplicates=duplicates,
    )

    manifest = calculate_manifest(output)
    validate_manifest(
        manifest,
        base_rows=base_rows,
        malformed_dates=malformed_dates,
        missing_pu=missing_pu,
        missing_do=missing_do,
        invalid_fares=invalid_fares,
        duplicates=duplicates,
    )

    batch_path = output_dir / f"batch_{batch_no:02d}.json"
    write_jsonl(batch_path, output)

    return {
        "batch": f"batch_{batch_no:02d}",
        "source": source_mode,
        "file": batch_path.name,
        **manifest,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate B1 dirty JSON batches from real NYC Parquet data.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("."),
        help="Destination directory for batch_*.json and error_manifest.json",
    )
    parser.add_argument(
        "--source-files",
        type=Path,
        nargs=3,
        default=None,
        metavar=("JAN_PARQUET", "FEB_PARQUET", "MAR_PARQUET"),
        help=(
            "Optional custom paths to NYC Yellow Taxi 2025 Jan/Feb/Mar Parquet files. "
            "Defaults to auto-detecting data/raw/yellow_tripdata_2025-0*.parquet."
        ),
    )
    parser.add_argument("--base-rows", type=int, default=DEFAULT_BASE_ROWS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--duplicates", type=int, default=DEFAULT_DUPLICATES)
    parser.add_argument("--malformed-dates", type=int, default=DEFAULT_MALFORMED_DATES)
    parser.add_argument("--missing-pu", type=int, default=DEFAULT_MISSING_PU)
    parser.add_argument("--missing-do", type=int, default=DEFAULT_MISSING_DO)
    parser.add_argument("--invalid-fares", type=int, default=DEFAULT_INVALID_FARES)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    source_files = resolve_source_files(args.source_files)

    batches: List[Dict[str, Any]] = []
    for batch_no in range(1, 4):
        batches.append(
            generate_one_batch(
                batch_no=batch_no,
                output_dir=args.output_dir,
                source_file=source_files[batch_no - 1],
                base_rows=args.base_rows,
                seed=args.seed,
                malformed_dates=args.malformed_dates,
                missing_pu=args.missing_pu,
                missing_do=args.missing_do,
                invalid_fares=args.invalid_fares,
                duplicates=args.duplicates,
            )
        )

    totals = {
        key: sum(batch[key] for batch in batches)
        for key in [
            "raw_rows",
            "unique_trip_ids",
            "duplicate_excess_rows",
            "malformed_date_rows",
            "missing_pu_rows",
            "missing_do_rows",
            "missing_location_rows",
            "invalid_fare_rows",
            "expected_rejected_rows",
            "expected_silver_rows",
        ]
    }

    manifest = {
        "contract_version": "B1-v1.0",
        "seed": args.seed,
        "date_format": DATE_FORMAT,
        "definition": {
            "duplicate": "duplicate excess row; repeated trip_id beyond the first row",
            "malformed_date": "tpep_pickup_datetime cannot parse as yyyy-MM-dd HH:mm:ss",
            "missing_location": "PULocationID is null OR DOLocationID is null",
            "invalid_fare": "fare_amount <= 0",
        },
        "batches": batches,
        "totals": totals,
    }

    manifest_path = args.output_dir / "error_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print("B1 generation completed")
    print("-" * 72)
    for batch in batches:
        print(
            f"{batch['batch']}: raw={batch['raw_rows']}, "
            f"dup={batch['duplicate_excess_rows']}, "
            f"bad_date={batch['malformed_date_rows']}, "
            f"missing_location={batch['missing_location_rows']} "
            f"(PU={batch['missing_pu_rows']}, DO={batch['missing_do_rows']}), "
            f"fare<=0={batch['invalid_fare_rows']}, "
            f"rejected={batch['expected_rejected_rows']}, "
            f"silver={batch['expected_silver_rows']}"
        )
    print("-" * 72)
    print(
        f"TOTAL: bronze/raw={totals['raw_rows']}, "
        f"rejected={totals['expected_rejected_rows']}, "
        f"silver={totals['expected_silver_rows']}"
    )


if __name__ == "__main__":
    main()
