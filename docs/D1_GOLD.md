# D1 — Gold aggregates

**Depends on:** B3 (Silver). Re-run after C1 and C2.
**Module:** `src/gold.py` · **Tests:** `tests/test_gold.py`

```bash
python -m src.gold --output docs/d1_run.json
python -m src.gold --grain zone_date --show 10      # coarser alternative
```

---

## 1. Expected output — verify against these

Computed independently from B1's raw JSON, not from the pipeline. If your run
disagrees, the pipeline is wrong, not this table.

| Quantity | Expected |
|---|---:|
| Silver rows in | **2880** |
| Gold buckets, grain `zone_hour` | **210** |
| Median trips per bucket | 8.5 |
| Max trips in a bucket | 97 |
| Singleton buckets | 50 (23.8%) |
| Distinct zones | 78 |
| Distinct dates | 6 |
| `SUM(trip_count)` | **2880** |
| `SUM(total_revenue)` | **70,826.82** |
| `SUM(driver_earnings)` | **59,768.85** |
| Weighted tip% across buckets | **22.7141%** |

Grain `zone_hourofday` gives 92 buckets; `zone_date` gives fewer still.

---

## 2. The three definitions

### avg_tip_pct = AVG(tip / fare) **per trip**

Not `SUM(tip)/SUM(fare)`. On this dataset:

```
per-trip mean      22.7141 %     <- what Gold reports
ratio of sums      20.2264 %
gap                 +2.4876 pp
```

Two and a half percentage points is not a rounding difference. The per-trip
mean answers *"what does a typical passenger tip?"*; the ratio of sums is
pulled down by a few high-fare trips whose tips are proportionally smaller.

`verify()` pins this: the trip-count-weighted mean of `avg_tip_pct` across
buckets must equal the global per-trip mean. An unweighted average of bucket
averages would not — that is the classic aggregate-of-averages error, and
`test_verify_rejects_aggregate_of_averages` proves the check catches it.

### driver_earnings = fare + tip + extra

Tolls, `mta_tax`, `improvement_surcharge` and `congestion_surcharge` are
pass-through to the bridge authority, the state and the MTA. The driver does
not keep them.

`extra` is `coalesce`d to 0 — without that, one null makes the whole sum null.

### total_revenue = SUM(total_amount)

**Not** the sum of components. Those two disagree here:

```
SUM(total_amount)   70,826.82
SUM(components)     71,650.32      delta +823.50
823.50 / 0.75 = 1098 trips
```

B1's contract omits `cbd_congestion_fee` ($0.75 per CBD trip) while
`total_amount` still includes it, so the receipt no longer adds up on 1,098 of
2,880 rows. Each metric names its basis so the two are never mixed silently.

This is worth a paragraph in the report: a column-selection decision at B1
propagated into a broken accounting identity three layers downstream, and only
surfaced because Gold computes revenue two ways.

---

## 3. What these numbers describe — read before writing conclusions

B1's generator streams Parquet row groups in file order (`iter_batches`) and
keeps the first clean rows (`clean[:base_rows]`). TLC files are sorted by
pickup time, so each batch is the **first ~1.5 hours of its month**:

| Batch | Span | Rows |
|---|---|---:|
| batch_01 | 2024-12-31 23:30 → 2025-01-01 01:02 | 960 |
| batch_02 | 2025-01-31 23:39 → 2025-02-01 01:00 | 960 |
| batch_03 | 2025-02-28 23:43 → 2025-03-01 01:00 | 960 |

Consequences:

- **99.3% of trips are in hour 0.** Hour-of-day analysis is not available.
- **6 distinct dates, 78 zones** out of ~260.
- **batch_01 is New Year's Eve midnight** — the most atypical taxi night of the
  year.

The aggregates are arithmetically correct and demonstrate the Gold layer
properly. They are **not** a description of NYC taxi demand. `profile()`
measures all of this into `docs/d1_run.json` so the report states it rather
than implying otherwise.

Phrase findings as *"across the three sampled month-boundary midnights…"*,
never *"NYC taxi passengers tip 22.7%"*.

---

## 4. Grain

| Grain | Keys | Buckets | When |
|---|---|---:|---|
| `zone_hour` *(default)* | PULocationID × truncated hour | 210 | the specified grain |
| `zone_hourofday` | PULocationID × hour 0–23 | 92 | pointless here — almost everything is hour 0 |
| `zone_date` | PULocationID × date | fewer | most robust when sparse |

At `zone_hour`, 23.8% of buckets hold a single trip. A one-trip bucket is not
an aggregate — its "average" is that trip's value. The CLI warns above 20%.

`pickup_date` and `hour_of_day` are carried as columns, so a coarser view can
be derived without rebuilding.

---

## 5. Guarantees asserted

`verify()` raises rather than warns:

1. `SUM(trip_count) == silver_count` — no row lost to a null grouping key
2. trip-count-weighted `avg_tip_pct` equals the global per-trip mean
3. `SUM(total_revenue)` equals `SUM(total_amount)` in Silver

---

## 6. Ordering

Gold is derived and rebuilt with `overwrite`, which is correct — but it means
Gold is only as current as the last rebuild.

```
B2 → B3 → C1 (CDC) → C2 (schema evolution) → D1
```

Run D1 **after** C1, or Gold describes a pre-CDC Silver: 2,880 rows instead of
3,080, and the 500 corrected fares missing.

---

## 7. Evidence to capture

| Artifact | How |
|---|---|
| `docs/d1_run.json` | `python -m src.gold --output docs/d1_run.json` |
| busiest buckets | `--show 10` |
| tip% definition gap | `checks.tip_pct_definition_gap_pp` |
| sparsity | `profile.singleton_pct`, `trips_per_bucket_median` |
| temporal concentration | `profile.hour_of_day_distribution`, `profile.dates` |

The tip% gap and the hour-of-day distribution are the two worth putting in the
report. The first shows a definition choice changing a headline number by 2.5
points; the second shows you know what your data actually covers.
