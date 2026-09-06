# B1 — Data Contract & Dirty JSON Generator

**Contract version:** `B1-v1.0` (frozen)
**Status:** complete and verified
**Source:** NYC Yellow Taxi 2025-01 / 02 / 03 Parquet

B1 defines the raw data contract, generates three JSON batches containing
controlled dirty-data cases, and records exact ground-truth counts so every
downstream task has a number to check against.

B1 does **not** perform Delta ingestion, Silver cleaning, MERGE, Time Travel,
OPTIMIZE or Z-ORDER.

---

## 1. Files

| File | Who reads it | What it is |
|---|---|---|
| `schema.md` | **everyone** | The contract. Field types, dirty-data rules, handoff terms. Read this first. |
| `batch_01.json` `batch_02.json` `batch_03.json` | **B2** | JSON Lines, 1020 rows each, 3060 total |
| `error_manifest.json` | **B3** | Ground-truth counts per defect class |
| `generate_batches.py` | reference | Deterministic generator, seed 42 |
| `bronze_schema.json` | **B2** | Explicit Spark `StructType` |
| `defect_index.json` | **B3, C1, E2** | Which `trip_id` carries which defect, plus sampling pools |
| `fixture_mini.json` | **B3, E2** | 18-row test fixture, all four defect classes |
| `fixture_mini_expected.json` | **B3, E2** | Ground truth for the fixture |
| `checksums.txt` | **everyone** | sha256 of every B1 file |
| `README.md` | **everyone** | This file |

The last six are **derived from the delivered batches**. The generator was not
re-run and the ground truth is unchanged — they are additional views over the
same data, so B1's verification still stands.

---

## 2. Verification

All 20 counts in `error_manifest.json` match the delivered JSON exactly.

| Check | Manifest | Measured |
|---|---|---|
| raw rows | 3060 | 3060 |
| unique `trip_id` | 3000 | 3000 |
| duplicate excess | 60 | 60 |
| malformed date | 30 | 30 |
| missing PU / DO | 30 / 30 | 30 / 30 |
| invalid fare | 30 | 30 |
| rejected / silver | 180 / 2880 | 180 / 2880 |

Two further properties, not claimed in the manifest but load-bearing:

- **No row carries more than one defect class.**
- **No duplicated row is itself dirty.**

Without that disjointness the four counts would double-count and
`silver + rejected = bronze` would not balance. The generator achieves it by
sampling clean source rows *before* injecting defects.

Independent cross-check: `survives_to_silver` in `defect_index.json` is
**2880**, derived by a different path than the manifest used.

Confirm your copy is identical:

```bash
cd b1 && sha256sum -c checksums.txt
```

---

## 3. The frozen contract

Twenty fields, JSON Lines, one object per line:

```
VendorID · tpep_pickup_datetime · tpep_dropoff_datetime · passenger_count
trip_distance · RatecodeID · store_and_fwd_flag · PULocationID · DOLocationID
payment_type · fare_amount · extra · mta_tax · tip_amount · tolls_amount
improvement_surcharge · total_amount · congestion_surcharge · Airport_fee · trip_id
```

| Fact | Consequence for downstream |
|---|---|
| `trip_id` already exists, `trip_YYYYMM_NNNNNN` | **B2 must not generate one.** No hashing, no UUID. |
| Datetimes are **strings** | They stay strings through Bronze. Only Silver parses. |
| Only `PULocationID` / `DOLocationID` are ever null | Every other field is always populated |
| `Airport_fee` has a **capital A** | Never rename or lowercase it |
| No `cbd_congestion_fee` | That absence is C2's schema-evolution baseline |
| Malformed dates are exactly `"not-a-date"` and `"2024-13-40 25:61:00"` | The second is syntactically valid — must be rejected on semantics |
| Invalid fares are exactly `0.0` and `-5.0` | The rule `fare <= 0` covers both |
| 3060 rows, 3000 unique ids | The number every downstream task checks |

### `trip_id` design

The key deliberately **excludes `fare_amount` and `tip_amount`**. C1 will later
update those values; if they were part of the key, an update would produce a
different key and the MERGE would insert a new row instead of updating the
existing one.

**Known limitation:** the id is assigned by `enumerate()` over B1's sample, so
it is a property of a row's *position*, not of the row. Deterministic given
identical inputs and seed. But regenerate the batches from a different sample
and `trip_202501_000123` points at a different trip. The JSON files on disk are
the source of truth — do not regenerate them.

---

## 4. Handoff — B2 (Bronze Ingest)

**Your number: 3060.**

### Read with the explicit schema

Reading the batches file-by-file makes Spark infer a schema **per file**. A
column can come back typed differently in each, and the union then fails or
silently nulls it. Pass the schema instead:

```python
import json
from pyspark.sql.types import StructType
from pyspark.sql import functions as F

schema = StructType.fromJson(json.load(open("b1/bronze_schema.json")))
df = spark.read.schema(schema).json("b1/batch_*.json")
```

`tpep_pickup_datetime` and `tpep_dropoff_datetime` are `StringType` **on
purpose**. Malformed values must survive Bronze so B3 can quarantine them.

### Add metadata

```python
df = (df
      .withColumn("_source_file", F.element_at(F.split(F.input_file_name(), "/"), -1))
      .withColumn("_batch_id",    F.regexp_extract(F.input_file_name(), r"(batch_\d+)", 1))
      .withColumn("_ingest_ts",   F.current_timestamp()))
```

### Do not

- **Do not filter anything.** Not bad dates, not null locations, not
  `fare <= 0`, not duplicates. Remove them and the 180 rejected rows vanish and
  B3 has nothing to demonstrate.
- **Do not parse timestamps.** That is B3's job.
- **Do not generate `trip_id`.**
- **Do not rename or lowercase any column.**
- **Do not deduplicate.** The 60 duplicate rows must reach Silver.

### Write

```python
(df.write.format("delta").mode("append")
   .option("mergeSchema", "true")     # so C2 can add a column later
   .save(BRONZE_PATH))
```

### Assert

```python
bronze = spark.read.format("delta").load(BRONZE_PATH)
assert bronze.count() == 3060
assert bronze.select("trip_id").distinct().count() == 3000
assert bronze.filter(
    "_ingest_ts IS NULL OR _source_file IS NULL OR _batch_id IS NULL").count() == 0
```

A count under 3060 means Spark parked lines in `_corrupt_record`. B1's JSON is
syntactically valid, so that column should never appear.

Do **not** assert "re-running does not duplicate" — an append-only re-run
doubles the table by design. Idempotency belongs to E2, against the whole
pipeline.

**Bronze output: 3060 rows × 23 columns** (20 from B1 + 3 metadata).

---

## 5. Handoff — B3 (Silver Cleaning + Quarantine)

**Your numbers: 2880 silver + 180 rejected, and per reason 60 / 30 / 60 / 30.**

### Exactly four rules — no more

| Reject reason | Condition |
|---|---|
| `DUPLICATE_TRIP` | `trip_id` repeated; reject rows beyond the first |
| `INVALID_PICKUP_DATETIME` | pickup fails `yyyy-MM-dd HH:mm:ss` |
| `MISSING_LOCATION` | `PULocationID IS NULL OR DOLocationID IS NULL` |
| `INVALID_FARE` | `fare_amount <= 0` |

B1 samples clean rows *before* injecting, so no other defect class exists in
the batches. Add a fifth rule (dropoff < pickup, distance ≤ 0, zone 264/265)
and it either fires on nothing or fires on rows B1 counted as clean — either
way `2880 + 180 = 3060` breaks with no obvious cause.

### Use `try_cast`, never `.cast()`

Spark 4 enables ANSI mode by default. `.cast("timestamp")` on `"not-a-date"`
raises `CAST_INVALID_INPUT` and kills the job on the first malformed row.
`try_cast` returns NULL, which `INVALID_PICKUP_DATETIME` then quarantines.

```python
F.expr("try_cast(`tpep_pickup_datetime` as timestamp)")   # NULL, quarantined
F.col("tpep_pickup_datetime").cast("timestamp")           # throws, job dies
```

### Pin the parser

`"2024-13-40 25:61:00"` is syntactically well-formed. Under `LEGACY` parsing,
Spark rolls month 13 into the next year instead of returning NULL:

```python
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
```

### Dedup tiebreak

B1's duplicates are byte-identical, so `_ingest_ts` ties and window ordering is
arbitrary between runs. Use `dropDuplicates(["trip_id"])`, or order by
`_ingest_ts DESC` plus a stable secondary column.

### Assert the right rows, not just the right count

Reconciling totals proves no rows were **lost**. It does not prove the right
rows were **rejected** — reject 30 valid rows and miss 30 invalid ones and
`2880 + 180 = 3060` still balances perfectly.

```python
idx = json.load(open("b1/defect_index.json"))
expected = set(idx["batches"]["batch_01"]["defect_ids"]["INVALID_PICKUP_DATETIME"])

actual = {r.trip_id for r in rejected
          .filter("reject_reason = 'INVALID_PICKUP_DATETIME' AND _batch_id = 'batch_01'")
          .select("trip_id").collect()}

assert actual == expected
```

This is the one thing this project can prove that a production pipeline never
can: at scale, nobody knows the true defect count. Not asserting it wastes B1
entirely.

### Fast iteration

Use the 18-row fixture instead of reloading 3060 rows each time:

```python
df  = spark.read.schema(schema).json("b1/fixture_mini.json")
exp = json.load(open("b1/fixture_mini_expected.json"))
# raw 18 -> silver 8 + rejected 10
```

---

## 6. Handoff — C1 (CDC / MERGE)

**Sample update ids from `clean_and_unique` only.**

```python
pool = json.load(open("b1/defect_index.json"))["pools"]["clean_and_unique"]
import random; random.seed(42)
update_ids = random.sample(pool, 500)     # guaranteed present in Silver
```

### Why it matters

If C1 draws ids from raw Bronze it can pick a row Silver rejected. That id is
absent from Silver, the MERGE falls to the **unmatched** branch, and a row is
*inserted* instead of *updated*. The statement succeeds, the count rises, the
log looks fine — and `whenMatchedUpdateAll` never fires. The demo fails while
appearing to work.

### Only change non-key columns

Modify `fare_amount`, `tip_amount` or `total_amount`. Never `trip_id`,
`VendorID`, the pickup/dropoff timestamps, or the location ids — those are what
`trip_id` was built from, and changing them turns an UPDATE into an INSERT.

### Prove both branches fired

```python
assert updated_count > 0
assert after_count == before_count + inserted_count
```

### Pools available

| Pool | Size | Use |
|---|---|---|
| `survives_to_silver` | 2880 | every id reaching Silver |
| `clean_and_unique` | 2820 | survives **and** not duplicated — **use this** |
| `duplicated_but_clean` | 60 | the duplicated ids; valid, only the excess copy is rejected |

---

## 7. Handoff — C2 (Schema Evolution)

B1's contract carries **no `cbd_congestion_fee`**. That is deliberate.

TLC added that column to the real feed for 2025 data onwards, to reflect
Manhattan congestion pricing. The generator excludes it, so the three baseline
batches carry the pre-2025 field set — which makes C2 a **real, documented,
citable schema change** rather than an invented `surcharge_fee`.

Evolve at **Bronze** with `mergeSchema` and let it flow to Silver. Do not write
directly to Silver; that breaks the layering the project exists to demonstrate.

---

## 8. Handoff — D2 (Performance Lab)

**B1's output is not suitable for the layout benchmark.**

3,060 rows is roughly 200 KB. `OPTIMIZE` targets ~1 GB files, so the table
compacts into a **single file** — and data skipping works by eliminating files
using per-file min/max statistics. One file means nothing to skip, and any
measured "improvement" is scheduling noise.

This is not a defect in B1. B1 was built for correctness and does that job
exactly. D2 needs volume instead: point it at the raw Parquet on `main`. Two
data paths, two purposes — a deliberate design choice worth writing up, not an
accident.

---

## 9. Regenerating the batches

Not normally needed. The delivered JSON is the source of truth, and
regenerating with a different sample repoints every `trip_id` at a different
trip.

The three source Parquet files (~1.15 GB) are on the team Google Drive rather
than in Git. Place them in `data/raw/` and:

```bash
python generate_batches.py --output-dir .
```

Same inputs plus seed 42 reproduce the same controlled defect assignment.
Verify with `sha256sum -c checksums.txt` afterwards.
