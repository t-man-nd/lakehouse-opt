# Source data manifest

Generated: 2026-09-24T16:07:09+00:00  
Source: NYC TLC Trip Record Data (official CloudFront links) and NY/NYC Open Data.

## Trip files

| Month | File | Rows | Bytes | Columns | SHA-256 |
|---|---|---:|---:|---:|---|
| 2024-01 | `yellow_tripdata_2024-01.parquet` | 2,964,624 | 49,961,641 | 19 | `c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510` |
| 2024-02 | `yellow_tripdata_2024-02.parquet` | 3,007,526 | 50,349,284 | 19 | `c76c43c18c6c6664080dd920baab4928988d5786a6b65980792ca7cd796f9f20` |
| 2024-03 | `yellow_tripdata_2024-03.parquet` | 3,582,628 | 60,078,280 | 19 | `2d4cdc8fb96726cdd3803b13b02d2e61e71d45720aff0ebc693a8bdd1f249823` |
| 2025-01 | `yellow_tripdata_2025-01.parquet` | 3,475,226 | 59,158,238 | 20 | `9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a` |
| 2025-02 | `yellow_tripdata_2025-02.parquet` | 3,577,543 | 60,343,086 | 20 | `037cba555a73663f3a51a2c27816e40e3feb364769942bdf122b9da31e377bd3` |
| 2025-03 | `yellow_tripdata_2025-03.parquet` | 4,145,257 | 69,964,745 | 20 | `20c4b77cce457b7cfdae77c5069ca7dc91c167f876a41467f4502ab69487a186` |

Total trip rows: **20,752,804**

## Schema drift between files

Columns whose name or physical type differs between files. Bronze harmonises names and widens types; the drift itself is evidence for the report.

| Column | 2024-01 | 2024-02 | 2024-03 | 2025-01 | 2025-02 | 2025-03 |
|---|---|---|---|---|---|---|
| cbd_congestion_fee | MISSING | MISSING | MISSING | cbd_congestion_fee:double | cbd_congestion_fee:double | cbd_congestion_fee:double |

## Reference files

| File | Bytes | SHA-256 | Note |
|---|---:|---|---|
| `taxi_zone_lookup.csv` | 12,331 | `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed` |  |
| `cbd_zones.csv` | 1,018 | `b14cae22480f3aa0bb82abdbe07bf8639cc7926067eafd0b8be23f94d6b6a69d` | fallback list (33 official + 5 inferred) |

## Events

- CBD_FALLBACK: cbd_zones.csv (data.ny.gov unreachable; used built-in list (33 official + 5 inferred zones). See docs/evidence/cbd_zone_validation.md)
