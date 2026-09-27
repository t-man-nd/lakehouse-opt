# Data sources and scope

## Why this scope

The project explores real data rather than forcing the architecture onto a toy set. Two
comparable quarters one year apart give a question worth answering and exercise every Delta
feature naturally:

| Need (rubric) | How the real data provides it |
|---|---|
| Volume for a meaningful benchmark (D2) | ~20 M trips across 6 files |
| Schema evolution (C2) | 2024 files have 19 columns; 2025 files add `cbd_congestion_fee` |
| Type drift | Source timestamps are `timestamp_ntz` and IDs are int32; Bronze widens them to `timestamp`/`bigint` (logged per batch). In the currently published files the only column-level drift is `cbd_congestion_fee`: TLC has already standardised `Airport_fee` casing and `passenger_count` types. The test data still simulates the older drift so the harmonisation code stays covered. |
| CDC / MERGE (C1) | Late arrivals and resubmitted trips across monthly files; plus a seeded, labelled correction feed |
| Analytical question (D1) | Congestion pricing started 2025-01-05: how did trips into/out of/within the CBD change vs 2024, relative to non-CBD trips? |

## Sources

| Source | URL | Used for |
|---|---|---|
| Yellow Taxi trip records 2024-01..03, 2025-01..03 | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet` | Bronze → Silver → Gold |
| Taxi zone lookup | `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` | `silver/dim_zone` (zone, borough) |
| MTA CBD taxi zones (data.ny.gov `yfdc-w5jh`) | `https://data.ny.gov/api/views/yfdc-w5jh/rows.csv?accessType=DOWNLOAD` | `is_cbd` flag → CBD flows |
| TLC pickups/drop-offs by zone and industry (NYC Open Data `c5iv-bn4s`), optional | `https://data.cityofnewyork.us/api/views/c5iv-bn4s/rows.csv?accessType=DOWNLOAD` | External reconciliation of our pickup counts |

TLC publishes monthly with roughly a two-month delay and warns that the Parquet schema may be
standardised across years. The manifest records SHA-256 per file, so a republished file is
detected (`REPUBLISHED` event) and ingested as a new Bronze batch instead of silently replacing
history.

## Columns not verified in advance

The header of the two Open Data CSVs could not be checked before writing the code:

* **CBD zones:** the official dataset's ID column is `taxi_zone`. data.ny.gov returns HTTP 403
  to some networks (including ours, from Vietnam, in the browser too). In that case the downloader
  writes a built-in list with its provenance in a `source` column:
  - 33 zones (`mta_yfdc_w5jh_partial`) read from the official export (data.ny.gov yfdc-w5jh,
    rows.xml, retrieved 2026-09-24; the export we could read was cut off after zone 232);
  - 5 zones (`inferred_geography`: 233, 234, 246, 249, 261), the remaining Manhattan zones south
    of 60th Street. These are an inference, not official data.

  Gold then tests the list against the data itself (`docs/evidence/cbd_zone_validation.md`):
  every trip that *starts* in the zone after 2025-01-05 owes the fee, so CBD zones should show a
  fee share near 100 % among their pickups. Zones that disagree are flagged
  (`LISTED_BUT_LOW_SHARE`, `NOT_LISTED_HIGH_SHARE`). Report the verdict counts, and correct the
  list in `scripts/download_sources.py` if the evidence contradicts it.
* **TLC aggregates:** columns are detected by keyword; otherwise the Gold step reports
  `SKIPPED` with the header. The pipeline still succeeds.

## Acquisition

```bash
python scripts/download_sources.py                        # 6 months + zone lookup + CBD zones
python scripts/download_sources.py --with-tlc-aggregates  # + optional external check
python scripts/download_sources.py --skip-download        # rebuild manifest from files on disk
```

Outputs: `data/raw/tlc/*.parquet`, `data/raw/ref/*.csv`, `docs/source_data_manifest.json` and
`.md`. The regenerated `.md` supersedes the earlier 2025-only manifest; it contains the same
2025 hashes plus the 2024 files, so any hash difference there is itself a finding to report.

## Synthetic data (clearly labelled, two places only)

1. **B1 fixture lane** (team's existing contract, `src/generator.py`): dirty JSON built from real
   rows, used to prove the exact 3,060 / 2,880 / 180 reconciliation.
2. **CDC correction feed** (`tlc_silver.apply_cdc`, seed 42): 25 real Silver trips with
   tip +2.00 (every second one also fare +1.50) plus 5 late trips cloned +3 minutes. It lands
   in Bronze as batch `cdc_corrections_seed42`, with `_source_file = synthetic://…`, so it can
   never be mistaken for TLC data.

Everything else in Bronze, Silver and Gold is real TLC data.

## Known limitations to state in the report

* The YoY comparison is descriptive, not causal: 2025 also differs in weather, events and
  ride-hail competition. Comparing CBD flows *relative to non-CBD trips* removes city-wide
  trends but not CBD-specific ones.
* Congestion fees also apply to trips that pass through the zone without starting or ending in
  it, so some `NON_CBD` trips legitimately carry the fee.
* Timestamps are NYC local wall-clock values without a zone; the session is pinned to UTC so
  Spark never shifts them. Daylight-saving transitions (March 10, 2024 and March 9, 2025) show
  as a missing 02:00 hour.
