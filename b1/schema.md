# B1 — Data Contract & Dirty JSON Generator

**Project:** Delta Lakehouse Architecture & Storage Optimization  
**Dataset contract:** NYC Yellow Taxi Trip Records (2025-compatible field set)  
**Owner:** Ánh — B1  
**Contract version:** `B1-v1.0`

## 1. Scope

B1 owns only:

1. Define the raw data contract used between the B1 generator, B2 Bronze ingestion, B3 Silver cleaning, and the later CDC task.
2. Generate 3 raw JSON Lines batches containing controlled dirty-data cases.
3. Record the exact ground-truth error counts so B3 can reconcile its cleaning result.

B1 does **not** perform Delta ingestion, Silver cleaning, MERGE, Time Travel, OPTIMIZE, or Z-ORDER.

Data flow:

```text
NYC Yellow Taxi 2025 Parquet source (Jan/Feb/Mar)
                    |
                    v
          B1 generate dirty JSON
                    |
                    v
     batch_01.json / 02 / 03
                    |
                    v
          B2 Bronze append-only
                    |
                    v
          B3 Silver validation
```

Official source page:
`https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`

Team Google Drive storage (Raw Parquet Dataset):
`https://drive.google.com/drive/folders/1YPWHynsI4eq6emZMhWQXElc-GdX_9QPv`

The project's source files are expected in `data/raw/`:

```text
data/raw/yellow_tripdata_2025-01.parquet
data/raw/yellow_tripdata_2025-02.parquet
data/raw/yellow_tripdata_2025-03.parquet
```

> **Note on Data Storage:** Due to GitHub's file size limits and repository performance, raw Parquet files (~1.15 GB) are hosted on the Team Google Drive folder above instead of being committed directly to Git. To re-generate the JSON batches locally, download the 3 Parquet files above from Google Drive and place them into `data/raw/`.

The delivered JSON files in this package are generated directly from these official 2025 NYC Yellow Taxi Parquet files using `generate_batches.py`.

---

## 2. File format contract

Raw files use **JSON Lines / NDJSON**: one complete JSON object per line.

Example:

```json
{"trip_id":"trip_202501_000001","VendorID":2,"tpep_pickup_datetime":"2025-01-15 08:24:19","PULocationID":161,"DOLocationID":236,"fare_amount":18.4,"tip_amount":3.5}
{"trip_id":"trip_202501_000002","VendorID":1,"tpep_pickup_datetime":"not-a-date","PULocationID":142,"DOLocationID":163,"fare_amount":12.0,"tip_amount":2.0}
```

B2 can therefore use a normal Spark JSON reader without `multiLine=true`.

---

## 3. Source schema / data contract

The Bronze/raw contract deliberately keeps datetime fields as **string** so malformed
date values can survive ingestion and be validated in B3.

| Field | Raw JSON type | Silver target | Contract / meaning |
|---|---|---|---|
| `trip_id` | string | string | Project-generated stable trip key; required |
| `VendorID` | integer/null | integer | Source vendor identifier |
| `tpep_pickup_datetime` | string | timestamp | Must parse as `yyyy-MM-dd HH:mm:ss` in Silver |
| `tpep_dropoff_datetime` | string | timestamp | Must parse as `yyyy-MM-dd HH:mm:ss` in Silver |
| `passenger_count` | integer/null | integer | Passenger count |
| `trip_distance` | number/null | double | Trip distance in miles |
| `RatecodeID` | integer/null | integer | Rate code |
| `store_and_fwd_flag` | string/null | string | Store-and-forward flag |
| `PULocationID` | integer/null | integer | Pickup Taxi Zone; required in Silver |
| `DOLocationID` | integer/null | integer | Drop-off Taxi Zone; required in Silver |
| `payment_type` | integer/null | integer | Payment type |
| `fare_amount` | number/null | double | Must be `> 0` in Silver |
| `extra` | number/null | double | Extra charge |
| `mta_tax` | number/null | double | MTA tax |
| `tip_amount` | number/null | double | Recorded tip |
| `tolls_amount` | number/null | double | Toll amount |
| `improvement_surcharge` | number/null | double | Improvement surcharge |
| `total_amount` | number/null | double | Total passenger charge |
| `congestion_surcharge` | number/null | double | Congestion surcharge |
| `Airport_fee` | number/null | double | Airport fee |

### Fields deliberately excluded from B1

B1 does **not** create the following Bronze metadata because they belong to B2:

```text
_ingest_ts
_source_file
_batch_id
```

B1 also does **not** put a future schema-evolution column such as `surcharge_fee`
into the baseline three batches. That column should be introduced by the later
schema-evolution task so the class can observe the schema change.

---

## 4. `trip_id` contract

NYC Yellow Taxi trip data does not provide the project key required by the assignment's
`MERGE INTO ... ON trip_id`.

B1 therefore adds an explicit stable synthetic key:

```text
trip_202401_000001
trip_202402_000001
trip_202403_000001
```

Properties:

- unique for each original/base record;
- copied unchanged when creating a deliberate duplicate;
- can be reused unchanged by downstream CDC updates;
- **does not depend on `fare_amount` or `tip_amount`**.

This is important because the CDC task will later update fare/tip values. If those
mutable values were part of the key, an update could accidentally become a new insert.

---

## 5. Dirty-data contract

### Rule DQ-01 — Duplicate trip

**Injection rule**

```text
Copy an otherwise-valid record and preserve exactly the same trip_id.
```

**B3 detection rule**

```text
same trip_id appears more than once
```

**Count definition**

`duplicate_excess_rows` = number of rows beyond the first row for a `trip_id`.

Example:

```text
T001
T001
T001

duplicate groups       = 1
duplicate excess rows  = 2
```

Recommended B3 reject reason:

```text
DUPLICATE_TRIP
```

---

### Rule DQ-02 — Malformed pickup datetime

**Valid format**

```text
yyyy-MM-dd HH:mm:ss
```

**Injected examples**

```text
not-a-date
2024-13-40 25:61:00
```

The JSON itself remains syntactically valid. Only the date value is invalid.

**B3 detection**

Parse `tpep_pickup_datetime` using the agreed format. If parsing fails / returns
null, reject the row.

Recommended reject reason:

```text
INVALID_PICKUP_DATETIME
```

---

### Rule DQ-03 — Missing location

**Invalid condition**

```text
PULocationID IS NULL
OR
DOLocationID IS NULL
```

B1 writes explicit JSON `null`; it does not remove the field.

Recommended B3 reject reason:

```text
MISSING_LOCATION
```

For audit, B1 records pickup-null and dropoff-null counts separately, while
`missing_location_rows = missing_pu_rows + missing_do_rows`.

---

### Rule DQ-04 — Invalid fare

**Invalid condition**

```text
fare_amount <= 0
```

B1 includes both boundary and negative cases:

```text
0.0
-5.0
```

Recommended B3 reject reason:

```text
INVALID_FARE
```

---

## 6. Controlled error profile

For every **1,000 clean base rows**, B1 injects disjoint defects:

| Injection | Count per batch | Relative to 1,000 base rows |
|---|---:|---:|
| Duplicate excess rows | 20 | 2% |
| Malformed pickup datetime | 10 | 1% |
| Missing `PULocationID` | 10 | 1% |
| Missing `DOLocationID` | 10 | 1% |
| `fare_amount <= 0` | 10 | 1% |

The error source sets are disjoint. Duplicate copies come only from otherwise-valid
records.

Therefore each batch has:

```text
base rows                     1000
duplicate rows appended         20
-----------------------------------
raw/Bronze rows               1020

duplicate excess               20
malformed date                 10
missing PU                     10
missing DO                     10
invalid fare                   10
-----------------------------------
expected rejected rows         60

expected Silver rows          960
```

Across 3 batches:

| Metric | Batch 01 | Batch 02 | Batch 03 | TOTAL |
|---|---:|---:|---:|---:|
| Raw rows | 1020 | 1020 | 1020 | 3060 |
| Duplicate excess rows | 20 | 20 | 20 | 60 |
| Malformed date rows | 10 | 10 | 10 | 30 |
| Missing PU rows | 10 | 10 | 10 | 30 |
| Missing DO rows | 10 | 10 | 10 | 30 |
| Missing location rows | 20 | 20 | 20 | 60 |
| Invalid fare rows | 10 | 10 | 10 | 30 |
| Expected rejected rows | 60 | 60 | 60 | 180 |
| Expected Silver rows | 960 | 960 | 960 | 2880 |

Required reconciliation:

```text
silver_count + rejected_count = bronze_count
2880        + 180            = 3060
```

The machine-readable version of this table is `error_manifest.json`.

---

## 7. Generator usage

The script samples clean base records from the real NYC Yellow Taxi 2025 Parquet files and injects controlled defects.

### A. Run generator with default paths (auto-detects `data/raw/`)

```bash
python generate_batches.py --output-dir .
```

### B. Run generator with custom paths

```bash
python generate_batches.py \
  --source-files \
  data/raw/yellow_tripdata_2025-01.parquet \
  data/raw/yellow_tripdata_2025-02.parquet \
  data/raw/yellow_tripdata_2025-03.parquet \
  --output-dir .
```

The script first selects clean source rows and only then injects the controlled
defects. This prevents pre-existing source anomalies from corrupting B1's ground truth.

Default random seed:

```text
42
```

The same inputs + same seed produce the same controlled defect assignment.

---

## 8. Handoff to B2 — Bronze Ingest

B2 should receive:

```text
batch_01.json
batch_02.json
batch_03.json
schema.md
error_manifest.json
```

B2 requirements:

- read all raw JSON rows;
- do **not** filter bad dates, missing locations, invalid fares, or duplicates;
- append rows to the Bronze Delta table;
- add only B2-owned metadata such as `_ingest_ts`, `_source_file`, `_batch_id`;
- expected final Bronze row count for the included batch set: **3060**.

If B2 removes B1 errors, B3 can no longer demonstrate Silver cleaning.

---

## 9. Handoff to B3 — Silver Cleaning + Quarantine

B3 must use exactly the same predicates:

| B1 error | B3 detection |
|---|---|
| Duplicate | repeated `trip_id`; reject excess rows |
| Malformed date | pickup datetime fails `yyyy-MM-dd HH:mm:ss` parsing |
| Missing location | `PULocationID IS NULL OR DOLocationID IS NULL` |
| Invalid fare | `fare_amount <= 0` |

For the included batches, expected totals are:

```text
bronze_count   = 3060
rejected_count = 180
silver_count   = 2880
```

and:

```text
silver_count + rejected_count == bronze_count
```

---

## 10. Handoff to C1 — CDC / MERGE

B1 only provides the prerequisite:

```text
stable trip_id
```

C1 may later create a late-arriving update such as:

```text
trip_id = trip_202501_000123
old fare/tip -> new fare/tip
```

while keeping the same `trip_id`, enabling:

```text
WHEN MATCHED     -> UPDATE
WHEN NOT MATCHED -> INSERT
```

B1 intentionally does not generate or execute the MERGE itself.

---

## 11. B1 Definition of Done

- [x] `schema.md` defines raw types and Silver expectations.
- [x] Stable project-generated `trip_id` is defined.
- [x] Date fields can preserve malformed string values in raw JSON.
- [x] Duplicate rule is based on repeated `trip_id`.
- [x] Missing location is explicitly defined.
- [x] `fare_amount <= 0` is explicitly defined.
- [x] `generate_batches.py` is deterministic and reproducible.
- [x] `batch_01.json` contains all 4 required error classes.
- [x] `batch_02.json` contains all 4 required error classes.
- [x] `batch_03.json` contains all 4 required error classes.
- [x] Exact counts are stored in `error_manifest.json`.
- [x] Ground-truth counts are available for B3 reconciliation.
- [x] No downstream Bronze/Silver/CDC/optimization responsibility is implemented in B1.
