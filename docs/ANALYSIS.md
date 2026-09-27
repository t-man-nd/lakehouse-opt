# Analysis of the full run (2024 Q1 + 2025 Q1, 20,752,834 Bronze rows)

Every number here comes from a generated evidence file; the source is named in each section so
the report can cite it. Nothing in this document is hand-computed.

---

## 1. Pipeline integrity

| Check | Result | Evidence |
|---|---|---|
| Row conservation, per batch and overall | Bronze 20,752,834 = rejected 680,112 + inserted 20,072,697 + updated 25 + no-op 0 | `docs/RESULTS.md` |
| Silver key uniqueness | `trip_key` unique across 20.07 M rows | `conservation_ok: true` |
| Gold vs independent SQL on Silver | `all_match: true` (3 sampled cells + global totals) | `docs/evidence/gold_verification.json` |
| Idempotency | Re-running Bronze skips all 6 batches; Silver processes none | `docs/pipeline_run.json` |
| Schema evolution | `cbd_congestion_fee` added by MERGE, old rows NULL, no rebuild | `docs/evidence/schema_evolution.json` |
| CDC | 25 updates + 5 inserts in one MERGE, planned counts = Delta's `operationMetrics` | `docs/evidence/cdc_merge.json` |

**Honest limitation to state in the report:** across 20.7 M real rows, only **one** trip key appeared
in two different monthly files. Late-arriving trips exist (119 flagged `LATE_ARRIVAL`) but they are
new trips, not resubmissions. The MERGE *update* path is therefore exercised by the seeded
correction feed, not by the monthly files. The update logic is real; the update traffic is not.

---

## 2. Data-quality findings

### 2.1 The 2025 rejection rate is one vendor's new stream

Rejections went from ~1.3 % (2024 Q1) to ~5.1 % (2025 Q1). 679,586 of the 680,112 hard rejects are
`INVALID_FARE`, and the growth is entirely negative fares from **VendorID 2, payment_type 0**:
2,066 rows in 2024-01 → 140,460 in 2025-03, averaging −4.27 with NULL rate codes. The older refund
pattern (payment types 2/3/4, averaging about −20) barely moved. *(`docs/evidence/findings.md` §3)*

### 2.2 VendorID 7 reports zero-duration trips exclusively

Vendor 7 first appears in 2025-01 and reaches 21,481 trips in 2025-03 — exactly its zero-duration
count. Vendors 1 and 2 report 600–900 zero-duration trips a month, unchanged since 2024. Every
vendor 7 trip has `dropoff = pickup`. *(`docs/evidence/findings.md` §2)*

### 2.3 The unstructured stream is growing fast

Rows with NULL `RatecodeID`, `passenger_count`, `congestion_surcharge` and `store_and_fwd_flag`
(all `payment_type = 0`) grew from 4.7 % of trips in 2024-01 to 22.1 % in 2025-03. This single
pattern explains the two largest soft flags: `UNKNOWN_RATECODE` (13.79 % of rows) and
`PASSENGER_COUNT_MISSING_OR_ZERO` (13.57 %). *(`docs/evidence/findings.md` §4)*

### 2.4 A rule we corrected using the evidence

The first run flagged `TOTAL_MISMATCH` on 31.0 % of rows. The diagnostic showed 90.7 % of the gaps
were exactly −2.50, −3.25, −0.75 or +2.50: the congestion surcharges, which TLC records three
different ways in the same file. Comparing `total_amount` against a range instead of a single sum
dropped the flag to **1.35 %** with no change to any hard reject. *(`docs/DQ_RULES.md`)*

This is worth presenting as method, not just result: profiling first, then a rule, then a
diagnostic when the rule's output looked implausible.

### 2.5 The trip key holds up

`keys_with_conflicting_content` looks alarming (31 k–61 k per file) but almost exactly equals the
count of reversal pairs — a trip and its negative mirror share one key. Those negatives are
rejected before deduplication, leaving **39 genuine collisions and 1 exact duplicate in 20.7 M
rows**. *(`docs/profile/bronze_profile.md` §5–6)*

---

## 3. Congestion pricing (the analytical question)

### 3.1 The zone list was verified against the data

38 zones are marked CBD. Each shows 94.8–99.7 % of its post-policy pickups paying the fee, and no
unlisted zone looks like a missed one. Five of the 38 were inferred from geography when
data.ny.gov was unreachable; all five came back at 97.2–99.6 %, so the inference was tested rather
than assumed. *(`docs/evidence/cbd_zone_validation.md`)*

### 3.2 Boundary-crossing trips grew less than the rest of the city

Trips per calendar day, 2025 vs 2024, and the same figure relative to non-CBD trips:

| Month | INTO_CBD | OUT_OF_CBD | WITHIN_CBD | NON_CBD (baseline) |
|---:|---:|---:|---:|---:|
| January | +8.4 % (−4.5 pp) | +6.8 % (−6.1 pp) | +18.4 % (+5.6 pp) | +12.9 % |
| February | +13.7 % (−9.6 pp) | +8.3 % (−15.1 pp) | +20.8 % (−2.5 pp) | +23.3 % |
| March | +6.6 % (−13.6 pp) | +4.8 % (−15.4 pp) | +10.7 % (−9.5 pp) | +20.2 % |

Trips crossing the zone boundary grew consistently less than trips entirely outside it, in all
three months and in both directions. Trips wholly inside the zone track the city trend more
closely.

**State this as a description, not a cause.** Q1 2025 differs from Q1 2024 in weather, events and
ride-hail competition as well as congestion pricing. Normalising against non-CBD trips removes
city-wide trends but not CBD-specific ones. Two further caveats: fees also apply to trips passing
through the zone, so some `NON_CBD` trips legitimately carry one; and Gold excludes rows flagged
`REVERSED`, `IMPLAUSIBLE_SPEED` or `EXTREME_DISTANCE`, which is a slightly larger share in 2025
(1.27 % overall), marginally understating 2025 growth.

---

## 4. Storage optimization (D2)

Measured on 20.07 M Silver rows, 3.1 GB, 5 interleaved rounds after a discarded warm-up.
*(`docs/benchmark/benchmark_results.md`)*

### 4.1 Data skipping works, and the I/O evidence is the strong part

| Query | Best variant | Files scanned | MB read | Time |
|---|---|---|---|---|
| Q1 `PULocationID = 132` | Z-ORDER (1 col) | 200 → **5** | 3,184 → **164** (−94.8 %) | −37.4 % |
| Q2 zone + one week | liquid clustering | 200 → **3** | 3,184 → **94** (−97.1 %) | −57.2 % |
| Q3 control (`payment_type`) | none | 200 → 98 (no skipping) | 3,184 → 3,112 (−2.3 %) | within noise |

### 4.2 Read the control correctly

Q3 shows 10–14 % "improvement", but **files scanned stay at 98–99 of 99**: nothing is skipped.
The baseline's min (163 ms) is below the compacted median (168 ms), so those percentages are
within run-to-run noise plus a small gain from reading 100 larger files instead of 200 smaller
ones. The correct conclusion is that clustering gives **no** benefit to a query that does not
filter on the clustering columns — which is exactly what a control is for.

### 4.3 Z-ORDER dimensionality is a real trade-off

On Q1, Z-ORDER on one column scans 5 files; on two columns it scans 42. Interleaving a second
column spreads each zone's rows across more files. On the two-dimensional Q2 the ordering flips:
liquid clustering (3 files) beats single-column Z-ORDER (5). Optimise for the query shape you
actually run.

### 4.4 Why time improved far less than I/O

Q1 reads 94.8 % less data but runs only 37 % faster, because absolute times are 100–300 ms, where
query planning, task scheduling and JVM overhead dominate, and because local NVMe plus the OS page
cache makes re-reads cheap. On cloud object storage, where each file costs a network round trip,
the same file reduction would produce a much larger time gain. **Files scanned and bytes read are
the primary evidence; wall-clock time is secondary.** The baseline here (200 files, 16 MB each) is
also a mild version of the small-file problem — a real streaming ingest leaves thousands of files
under 1 MB, where the gap would be wider.

---

## 5. What is left

The task plan is complete through E2. Remaining: F1 (report, slides, demo). The numbers above,
plus the files in `docs/evidence/`, `docs/profile/` and `docs/benchmark/`, are the source material.
