# Pipeline results (auto-generated)

Run started 2026-09-24T15:24:33+00:00 · status **SUCCESS** · source `tlc` · months 2024-01, 2024-02, 2024-03, 2025-01, 2025-02, 2025-03

| Step | Status | Seconds | Key result |
|---|---|---:|---|
| reference | OK | 7.2 | zones=265, cbd_zones=0 |
| bronze | OK | 10.9 | bronze_count=207456, bronze_version=5 |
| profile | OK | 6.8 | keys_seen_in_more_than_one_file=0 |
| silver | OK | 29.7 | silver_count=200603, rejected_count=6853, conservation_ok=True |
| cdc | OK | 7.6 | checks_ok=True |
| evolution | OK | 0.3 | status=SCHEMA_EVOLVED |
| time_travel | OK | 13.6 | silver_versions=7, merge_commits=6 |
| gold | OK | 5.9 | gold_trips=200563, zone_hourly_rows=107562 |

## Silver reconciliation

Bronze 207,456 = rejected 6,853 + inserted 200,603 + updated 0 + no-op 0

| Batch | Bronze rows | Rejected | Inserted | Updated | No-op | Silver version |
|---|---:|---:|---:|---:|---:|---:|
| yellow_2024-01_c4d59da7 | 29,554 | 404 | 29,150 | 0 | 0 | 0 |
| yellow_2024-02_c76c43c1 | 30,052 | 420 | 29,632 | 0 | 0 | 1 |
| yellow_2024-03_2d4cdc8f | 35,611 | 607 | 35,004 | 0 | 0 | 2 |
| yellow_2025-01_9af277e4 | 34,611 | 1,489 | 33,122 | 0 | 0 | 3 |
| yellow_2025-02_037cba55 | 36,241 | 1,842 | 34,399 | 0 | 0 | 4 |
| yellow_2025-03_20c4b77c | 41,387 | 2,091 | 39,296 | 0 | 0 | 5 |

Reject reasons: INVALID_FARE 6,850, NEGATIVE_DURATION 3

## CDC MERGE (C1)

Silver v5 -> v6: planned {'inserts': 5, 'updates': 25, 'noop': 0}, Delta metrics {'inserted': 5, 'updated': 25}, rows 200,603 -> 200,608. Checks: {'matched rows were updated (updated > 0)': True, 'updated == n_updates': True, 'inserted == n_inserts': True, 'after == before + inserted': True}

## CBD flows, 2025 vs 2024 (trips per calendar day)

| Month | Flow | 2024/day | 2025/day | YoY % | vs non-CBD (pp) |
|---:|---|---:|---:|---:|---:|
| 1 | CBD_UNAVAILABLE | 940.23 | 1068.23 | 13.614 | None |
| 2 | CBD_UNAVAILABLE | 1021.69 | 1228.21 | 20.214 | None |
| 3 | CBD_UNAVAILABLE | 1128.81 | 1267.39 | 12.277 | None |

Gold vs manual SQL on Silver: all_match = True. External reconciliation: SKIPPED.

Evidence files: docs/evidence/, docs/profile/, docs/benchmark/.
