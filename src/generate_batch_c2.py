#!/usr/bin/env python3
"""
Nhiem vu C2: Sinh batch du lieu moi (batch_04.json) phuc vu Schema Evolution.
Ke thua cau truc B1-v1.0 (19 cot chuan + trip_id) va bo sung cot moi: surcharge_fee (double).
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def generate_batch_04(
    output_path: str = "data/raw/batch_04.json",
    num_records: int = 100
) -> str:
    """Sinh batch_04.json chua truong moi surcharge_fee."""
    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    records = []
    for i in range(1, num_records + 1):
        trip_id = f"trip_202504_{i:06d}"
        minute = (i * 7) % 60
        second = (i * 13) % 60
        dropoff_min = (minute + 12) % 60

        record = {
            "VendorID": 1 if i % 2 == 0 else 2,
            "tpep_pickup_datetime": f"2025-04-01 09:{minute:02d}:{second:02d}",
            "tpep_dropoff_datetime": f"2025-04-01 09:{dropoff_min:02d}:{second:02d}",
            "passenger_count": 1 + (i % 4),
            "trip_distance": round(1.25 * (i % 8 + 1), 2),
            "RatecodeID": 1,
            "store_and_fwd_flag": "N",
            "PULocationID": 100 + (i % 60),
            "DOLocationID": 140 + (i % 60),
            "payment_type": 1 if i % 3 != 0 else 2,
            "fare_amount": round(12.50 + (i % 25) * 1.5, 2),
            "extra": 1.0,
            "mta_tax": 0.5,
            "tip_amount": round(2.0 + (i % 6) * 0.75, 2),
            "tolls_amount": 0.0,
            "improvement_surcharge": 1.0,
            "total_amount": round(20.0 + (i % 25) * 1.5, 2),
            "congestion_surcharge": 2.5,
            "Airport_fee": 0.0,
            "trip_id": trip_id,
            # COT MOI PHUC VU SCHEMA EVOLUTION (C2):
            "surcharge_fee": round(2.75 + (i % 5) * 0.5, 2)
        }
        records.append(record)

    with open(out_file, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"[C2-DATA] Successfully generated {num_records} records with column 'surcharge_fee' at: {out_file}")
    return str(out_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate batch_04.json for Task C2")
    parser.add_argument("--output", default="data/raw/batch_04.json", help="Path to output file")
    parser.add_argument("--records", type=int, default=100, help="Number of records")
    args = parser.parse_args()
    generate_batch_04(args.output, args.records)
