# Pipeline results (auto-generated)

Run started 2026-10-02T01:05:49+00:00 · status **SUCCESS** · source `tlc` · months 2024-01, 2024-02, 2024-03, 2025-01, 2025-02, 2025-03

| Step | Status | Seconds | Key result |
|---|---|---:|---|
| reference | OK | 5.7 | zones=265, cbd_zones=38 |
| bronze | OK | 20.9 | bronze_count=20752804, bronze_version=5 |
| profile | OK | 185.6 | keys_seen_in_more_than_one_file=1 |
| silver | OK | 359.9 | silver_count=20072692, rejected_count=680112, conservation_ok=True |
| cdc | OK | 51.8 | checks_ok=True |
| evolution | OK | 0.8 | status=SCHEMA_EVOLVED |
| time_travel | OK | 62.5 | silver_versions=7, merge_commits=6 |
| gold | OK | 39.2 | gold_trips=19808248, zone_hourly_rows=553881 |
| benchmark | OK | 319.9 | rows=20072697 |

## Silver reconciliation

Bronze 20,752,804 = rejected 680,112 + inserted 20,072,692 + updated 0 + no-op 0

| Batch | Bronze rows | Rejected | Inserted | Updated | No-op | Silver version |
|---|---:|---:|---:|---:|---:|---:|
| yellow_2024-01_c4d59da7 | 2,964,624 | 38,409 | 2,926,215 | 0 | 0 | 0 |
| yellow_2024-02_c76c43c1 | 3,007,526 | 41,512 | 2,966,014 | 0 | 0 | 1 |
| yellow_2024-03_2d4cdc8f | 3,582,628 | 59,880 | 3,522,748 | 0 | 0 | 2 |
| yellow_2025-01_9af277e4 | 3,475,226 | 145,644 | 3,329,582 | 0 | 0 | 3 |
| yellow_2025-02_037cba55 | 3,577,543 | 184,195 | 3,393,348 | 0 | 0 | 4 |
| yellow_2025-03_20c4b77c | 4,145,257 | 210,472 | 3,934,785 | 0 | 0 | 5 |

Reject reasons: INVALID_FARE 679,586, NEGATIVE_DURATION 476, OUT_OF_PERIOD 10, DUPLICATE_TRIP 1, KEY_COLLISION 39

## CDC MERGE (C1)

Silver v5 -> v6: planned {'inserts': 5, 'updates': 25, 'noop': 0}, Delta metrics {'inserted': 5, 'updated': 25}, rows 20,072,692 -> 20,072,697. Checks: {'matched rows were updated (updated > 0)': True, 'updated == n_updates': True, 'inserted == n_inserts': True, 'after == before + inserted': True}

## CBD flows, 2025 vs 2024 (trips per calendar day)

| Month | Flow | 2024/day | 2025/day | YoY % | vs non-CBD (pp) |
|---:|---|---:|---:|---:|---:|
| 1 | INTO_CBD | 13322.06 | 14442.06 | 8.407 | -4.465 |
| 1 | NON_CBD | 25805.52 | 29127.23 | 12.872 | 0.0 |
| 1 | OUT_OF_CBD | 15724.45 | 16793.45 | 6.798 | -6.074 |
| 1 | UNKNOWN_ZONE | 980.29 | 865.84 | -11.675 | -24.547 |
| 1 | WITHIN_CBD | 37557.61 | 44482.48 | 18.438 | 5.566 |
| 2 | INTO_CBD | 14165.03 | 16112.39 | 13.748 | -9.561 |
| 2 | NON_CBD | 26403.21 | 32557.5 | 23.309 | 0.0 |
| 2 | OUT_OF_CBD | 17162.41 | 18579.75 | 8.258 | -15.051 |
| 2 | UNKNOWN_ZONE | 959.86 | 840.71 | -12.413 | -35.722 |
| 2 | WITHIN_CBD | 42493.59 | 51352.79 | 20.848 | -2.461 |
| 3 | INTO_CBD | 16255.81 | 17325.42 | 6.58 | -13.632 |
| 3 | NON_CBD | 28435.77 | 34183.1 | 20.212 | 0.0 |
| 3 | OUT_OF_CBD | 18883.45 | 19784.23 | 4.77 | -15.442 |
| 3 | UNKNOWN_ZONE | 1094.13 | 853.13 | -22.027 | -42.239 |
| 3 | WITHIN_CBD | 47710.32 | 52809.16 | 10.687 | -9.525 |

Gold vs manual SQL on Silver: all_match = True. External reconciliation: SKIPPED.

Evidence files: docs/evidence/, docs/profile/, docs/benchmark/.
