# TLC source data manifest

Downloaded on 2026-09-09 from the official [TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), using the Yellow Taxi Parquet links for January, February and March 2025. TLC states that trip data is published monthly in Parquet format and that 2025 data includes `cbd_congestion_fee`.

| File | Rows | Bytes | SHA-256 |
|---|---:|---:|---|
| `data/raw/yellow_tripdata_2025-01.parquet` | 3,475,226 | 59,158,238 | `9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a` |
| `data/raw/yellow_tripdata_2025-02.parquet` | 3,577,543 | 60,343,086 | `037cba555a73663f3a51a2c27816e40e3feb364769942bdf122b9da31e377bd3` |
| `data/raw/yellow_tripdata_2025-03.parquet` | 4,145,257 | 69,964,745 | `20c4b77cce457b7cfdae77c5069ca7dc91c167f876a41467f4502ab69487a186` |

Each file ends with the Parquet `PAR1` marker and contains the TLC schema,
including `cbd_congestion_fee`. The generator reads only the contract columns,
selects 1,000 clean base rows per month, assigns stable `trip_id` values, and
injects the deterministic B1-v1.0 defects. The generated JSON and manifest have
these checksums (also recorded in `docs/checksums.txt`):

| File | Rows/entries | Bytes | SHA-256 |
|---|---:|---:|---|
| `data/raw/batch_01.json` | 1,020 | 456,074 | `a405d7b2bf0f07cc9a80e4af5bc72accf731f97e46edf4f1a0785d17260fdc51` |
| `data/raw/batch_02.json` | 1,020 | 456,353 | `9c7214c7f74a67388cd4d8009efc1a6d1229ce5c3ffe0f848b724f642a631410` |
| `data/raw/batch_03.json` | 1,020 | 456,402 | `2d5033f0175b7328cc56c8c34e4b2d6fedeeb0eca8a7cea5289b3084c89c35f2` |
| `data/raw/error_manifest.json` | manifest | 2,065 | `4f07f91d3d12ba179df11fc44af38fb4ac7b8ad47df148898eaa8259e073c6d1` |

The B2 Delta table has 3,060 rows and three non-null metadata columns. The B3
run is reconciled by `docs/b3_run.json`: 2,880 Silver rows and 180 quarantine
rows, with counts 30 invalid pickup datetimes, 60 missing locations, 30 invalid
fares and 60 duplicate excess rows.
