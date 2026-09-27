# Data-quality contract TLC-v2 (real TLC lane)

The synthetic fixture lane keeps the B1-v1.0 contract (4 rules, exact 3,060 / 2,880 / 180
reconciliation, `src/silver.py`). The real TLC files have problems that the four rules do not
cover, so the real lane uses **TLC-v2**, implemented in `src/dq_rules.py` and tested in
`tests/test_dq_rules.py`.

## Two outcomes, not one

| Outcome | Meaning | Where the row goes |
|---|---|---|
| **Hard reject** | The trip cannot be placed in time/space or priced. | `silver/yellow_trips_rejected` with one `reject_reason` (the first rule that fires) and the original Bronze values |
| **Soft flag** | A real trip with a questionable attribute. | `silver/yellow_trips`, listed in `dq_flags` |

Rejecting every odd row would over-clean: a zero-passenger trip still earned a fare, and a trip
with an unknown drop-off zone still happened. Each Gold metric decides which flags to exclude
(`GOLD_EXCLUDED_FLAGS` in `src/config.py`); the counts of everything excluded are published in
`gold/dq_monthly`.

## Hard rules (evaluated in this order)

| # | reject_reason | Condition | Why hard |
|---|---|---|---|
| 1 | `INVALID_PICKUP_DATETIME` | pickup NULL or unparseable (`try_to_timestamp`, strict `yyyy-MM-dd HH:mm:ss`) | No time → cannot be partitioned, deduplicated or analysed |
| 2 | `INVALID_DROPOFF_DATETIME` | dropoff NULL or unparseable | Duration and trip key undefined |
| 3 | `MISSING_LOCATION` | PU or DO NULL (incl. int overflow → NULL via `try_cast`) | Trip cannot be placed |
| 4 | `INVALID_FARE` | fare NULL or ≤ 0; `reject_detail` = `REVERSAL_OF_ACCEPTED_TRIP` / `NEGATIVE_FARE_UNPAIRED` / `NULL_FARE` / `ZERO_FARE` | Not a priced trip. **Change vs B1:** B1 let NULL fare pass; TLC-v2 rejects it |
| 5 | `NEGATIVE_DURATION` | dropoff < pickup | Physically impossible |
| 6 | `OUT_OF_PERIOD` | pickup earlier than `late_window_days` (31) before the file month, or later than `lead_tolerance_hours` (24) after it | Clock errors (e.g. 2002, 2009 dates seen in TLC files) |
| 7 | `DUPLICATE_TRIP` | same `trip_key` and identical content inside one batch (extra copies) | Double counting |
| 8 | `KEY_COLLISION` | same `trip_key`, different content inside one batch | Cannot tell which version is right; kept for review |

Dedup keeps the row with the smallest `row_hash`, so the result does not depend on row order.

## Soft flags

| Flag | Condition (thresholds in `DQThresholds`) |
|---|---|
| `LATE_ARRIVAL` | pickup before the file month (within the late window). Inserted into its true `pickup_month` |
| `AFTER_SOURCE_MONTH` | pickup after the file month (within tolerance) |
| `ZERO_DURATION` / `LONG_DURATION` | duration = 0 s / > 4 h |
| `ZERO_DISTANCE` / `EXTREME_DISTANCE` | distance ≤ 0 or NULL / > 100 mi |
| `IMPLAUSIBLE_SPEED` | > 80 mph with duration ≥ 60 s |
| `PASSENGER_COUNT_MISSING_OR_ZERO` | passenger_count NULL or ≤ 0 |
| `UNKNOWN_RATECODE` | RatecodeID NULL or not 1–6 (99 = unknown) |
| `UNKNOWN_ZONE` | zone 264/265, or outside 1–263 |
| `NEGATIVE_COMPONENT` | any surcharge/tip/toll/fee < 0 on an accepted trip |
| `TOTAL_MISMATCH` | \|total − sum of components\| > 0.05 |
| `REVERSED` | an accepted trip whose negative-fare mirror (same `trip_key`, same absolute fare) was rejected in the same batch |

## Keys

* `trip_key` = SHA-256 of VendorID, pickup, dropoff, PULocationID, DOLocationID. Money columns
  are excluded on purpose, so a fare/tip correction updates the same trip in MERGE.
* `row_hash` = SHA-256 of all typed business columns; MERGE updates only when it differs.
* The profile reports how often two genuinely different trips share a key
  (`keys_with_conflicting_content`). Cite that number when defending the key choice.

## Threshold review (completed after the first full run, 20,752,804 rows)

Evidence: `docs/profile/bronze_profile.md` (Bronze v6, 2024 Q1 + 2025 Q1).

| Threshold | Value | Evidence from the profile | Decision |
|---|---|---|---|
| late_window_days | 31 | `pickup_before_month` 13–31 rows/month; `pickup_before_late_window` 0–5 (dates like 2002-12-31, 2007-12-05) | Keep. The window separates plausible late arrivals from clock errors |
| lead_tolerance_hours | 24 | `pickup_after_month` 1–3 rows/month; `pickup_after_tolerance` 0 in every month | Keep. Only trips starting just after midnight on the 1st spill over |
| max_duration_hours | 4 | median 11.6–12.5 min, p99 ≈ 60 min, max 6,276–9,455 min; `duration_over_max` 1,199–2,031 (≈0.05 %) | Keep as a soft flag |
| max_distance_miles | 100 | median 1.7 mi, p99 ≈ 20 mi, max 176,836–320,136 mi; `distance_over_max` 59–214 | Keep. The maxima are impossible and stay flagged, not deleted |
| max_speed_mph | 80 | `speed_over_max` 116–390 per month (≈0.01 %) | Keep as a soft flag |
| total_mismatch_tolerance | 0.05 | First run flagged 25–32 % of rows. `docs/evidence/findings.md` showed 90.7 % of the gaps are exactly −2.50, −3.25, −0.75 or +2.50, i.e. the congestion surcharges | **Rule changed** (see below); the tolerance itself stays 0.05 |
| unknown_zone_ids | 264, 265 | `unknown_zone_264_265` 25,008–35,374 per month | Keep |
| valid_ratecodes | 1–6 | code 99 on 28,663–43,988 rows; NULL on 140,162–916,663 (the payment_type 0 block) | Keep |

### TOTAL_MISMATCH: comparing against a range, not a sum

TLC records the congestion surcharges three different ways, all present in the same file:

| Pattern | Gap (`total_amount − Σ components`) | Rows |
|---|---:|---:|
| Itemised and included in `total_amount` | 0.00 | the majority |
| Itemised but **excluded** from `total_amount` | −2.50 (2024), −3.25 (2025) | 4.0 M |
| **Charged without being itemised** (`congestion_surcharge` NULL, the payment_type 0 rows) | +2.50 | 2.2 M |

A single expected sum therefore cannot be right for every row. The rule now compares
`total_amount` against a range: `low` = the components certain to be included, `high` = `low` plus
the uncertain surcharges (their value, or the standard 2.50 / 0.75 when NULL). `TOTAL_MISMATCH`
fires only when `total_amount` falls outside `[low, high]` beyond the tolerance. That removes
about 5.8 M false positives (90.7 % of the flag) while still catching genuinely inconsistent
totals — roughly 2.9 % of rows, including gaps of −4.25, −5.00 and +5.50 that no surcharge explains.

`UNCERTAIN_COMPONENT_COLUMNS` and `STANDARD_SURCHARGE_AMOUNTS` in `src/config.py` hold this list.

Measured on the full rebuild: `TOTAL_MISMATCH` fell from **6,434,035 rows (31.0 %)** to **279,594 (1.35 %)**, with every hard-reject count unchanged (680,112).

### What the rejects actually are

Hard rejects total 680,112 of 20,752,834 rows (3.3 %), and 679,586 of them are `INVALID_FARE`:

| Month | Reject rate | Negative fares | of which paired with a positive trip | unpaired |
|---|---:|---:|---:|---:|
| 2024-01 | 1.29 % | 37,448 | 30,980 | 6,468 |
| 2024-02 | 1.38 % | 40,655 | 31,547 | 9,108 |
| 2024-03 | 1.67 % | 58,464 | 38,766 | 19,698 |
| 2025-01 | 4.19 % | 144,117 | 52,231 | 91,886 |
| 2025-02 | 5.15 % | 182,655 | 48,613 | 134,042 |
| 2025-03 | 5.08 % | 208,722 | 60,768 | 147,954 |

The 2025 jump is therefore not a change in our rules: negative-fare rows quadrupled, and the share
that pairs with a matching positive trip fell from ~80 % to ~30 %. `scripts/investigate_findings.py`
breaks these down by vendor and payment type.

### Key quality (defends `trip_key`)

`keys_with_conflicting_content` looks alarming (31,109–61,188 per file) but almost exactly equals
`with_matching_positive_trip` in the reversal section: the "conflict" is a trip and its negative
reversal sharing one key. Those negatives are rejected as `INVALID_FARE` before deduplication, so
only **39 real `KEY_COLLISION` rows across 20.7 M** survive, and exact duplicates are 1 row in total.
`trip_key` is therefore a sound business key for MERGE.

## Vendor-specific behaviour found in 2025 (report these as findings)

| Finding | Evidence (`docs/evidence/findings.md`) |
|---|---|
| **Every VendorID 7 trip has `dropoff = pickup`.** Vendor 7 first appears in 2025-01 and grows to 21,481 trips in 2025-03 — exactly the number of zero-duration trips it reports. Vendors 1 and 2 report 600–900 zero-duration trips a month, unchanged since 2024 | §2 |
| **All negative fares come from VendorID 2.** The 2025 growth is entirely `payment_type = 0` rows: 2,066 (2024-01) → 140,460 (2025-03), averaging −4.27 with NULL RatecodeID. The older refund pattern (payment types 2/3/4, averaging about −20) barely changed | §3 |
| **The payment_type 0 stream is growing fast.** Rows with NULL RatecodeID/passenger_count/congestion_surcharge/store_and_fwd_flag went from 4.7 % (2024-01) to 22.1 % (2025-03) of all trips, mostly VendorID 2 | §4 |

These rows are kept in Silver with flags, not deleted, so the report can quantify them.

## Known dataset facts that shaped the rules

* TLC records tips only for card payments → Gold reports tip % over all trips (plan definition)
  and over card trips.
* `payment_type = 0` (Flex Fare) and VendorID 6/7 appear in recent files; they are valid values,
  shown in the profile's distributions, not rejected.
* `cbd_congestion_fee` exists only from 2025; congestion pricing started 2025-01-05, so January
  2025 is split into pre/post periods in Gold.
