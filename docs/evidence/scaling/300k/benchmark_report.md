# D2 — Silver storage optimization benchmark

Run UTC: 2026-09-24T12:41:23.297040+00:00. Source: `/Users/haaminh109/Desktop/Minh/Tài liệu học tập (DSEB)/Kì 7/Big Data/lakehouse-opt/data/scale_runs/300k/run_01/silver`, version 2, **288,103 rows**.

## Method

The four required conditions run in order baseline → Z-ORDER 1 column → Z-ORDER 2 columns → CLUSTER BY + OPTIMIZE FULL, repeated for each query. All start from byte-identical baseline files and preserve every pinned Silver row.

Each query/layout has one untimed warmup and 5 timed executions. Spark DataFrame/catalog cache is cleared each time. OS page cache is **not** flushed: this is a warm-cache local benchmark. Delta planning is completed before timing; collection, task execution and result transfer are timed. Preparation, optimization and metrics collection are excluded.

`files_scanned` is the executed Spark scan operator counter. `bytes_read` is the filesystem input-byte counter of successful tasks in that query’s jobs; it is not physical disk traffic and not a sum of whole Parquet file sizes. Job/stage IDs and raw samples are retained in results.json.

Runtime: Spark 4.0.1, Delta 4.0.1, local[4], UTC; AQE disabled during measurements. Baseline partitions: 256. Layout target: 1,048,576 bytes. Supplemental compaction uses the original 1 GiB target; actual file sizes are reported below.

## Layouts before/after

| Condition | Files before | Files after | Bytes before | Bytes after | Optimize seconds |
|---|---:|---:|---:|---:|---:|
| Baseline | 256 | 256 | 15250618 | 15250618 | 1e-06 |
| Z-ORDER 1 column | 256 | 14 | 15250618 | 11597004 | 1.378763 |
| Z-ORDER 2 columns | 256 | 14 | 15250618 | 11261312 | 1.510168 |
| CLUSTER BY + OPTIMIZE FULL | 256 | 14 | 15250618 | 11211013 | 1.887916 |
| OPTIMIZE compaction control | 256 | 1 | 15250618 | 10770293 | 0.971622 |

## Q1_1D_PointLookup (1D)

Predicate: `PULocationID = 161`. Matching trips: **13265**. All results equal the pinned source query.

| Condition | Median ms | Improvement % | Median files scanned | Median input bytes |
|---|---:|---:|---:|---:|
| Baseline | 103.8914 | 0.0 | 256 | 8034978 |
| Z-ORDER 1 column | 21.8831 | 78.94 | 1 | 225900 |
| Z-ORDER 2 columns | 28.1081 | 72.94 | 11 | 2668174 |
| CLUSTER BY + OPTIMIZE FULL | 27.9263 | 73.12 | 9 | 2506696 |

## Q2_2D_LocationAndTime (2D)

Predicate: `PULocationID = 161 AND tpep_pickup_datetime >= TIMESTAMP '2025-01-23 00:00:00' AND tpep_pickup_datetime < TIMESTAMP '2025-01-24 00:00:00'`. Matching trips: **243**. All results equal the pinned source query.

| Condition | Median ms | Improvement % | Median files scanned | Median input bytes |
|---|---:|---:|---:|---:|
| Baseline | 101.9892 | 0.0 | 256 | 21117276 |
| Z-ORDER 1 column | 18.3506 | 82.01 | 1 | 492964 |
| Z-ORDER 2 columns | 18.8395 | 81.53 | 3 | 1202810 |
| CLUSTER BY + OPTIMIZE FULL | 19.5232 | 80.86 | 3 | 1550604 |

## Q3_2D_RangeAggregation (2D)

Predicate: `PULocationID BETWEEN 146 AND 176 AND DOLocationID BETWEEN 222 AND 252`. Matching trips: **17462**. All results equal the pinned source query.

| Condition | Median ms | Improvement % | Median files scanned | Median input bytes |
|---|---:|---:|---:|---:|
| Baseline | 93.7413 | 0.0 | 256 | 8034978 |
| Z-ORDER 1 column | 19.4466 | 79.26 | 4 | 907456 |
| Z-ORDER 2 columns | 30.7093 | 67.24 | 14 | 3839028 |
| CLUSTER BY + OPTIMIZE FULL | 30.6996 | 67.25 | 14 | 3905548 |

## Supplemental compaction control

This separate paired phase runs baseline → compacted, repeated three or more times after its own warmups. Its medians must be compared within this phase, not substituted into the four-condition phase.

| Query | Baseline median ms | Compacted median ms | Improvement % |
|---|---:|---:|---:|
| Q1_1D_PointLookup | 88.5208 | 24.0856 | 72.79 |
| Q2_2D_LocationAndTime | 83.0703 | 29.1462 | 64.91 |
| Q3_2D_RangeAggregation | 86.6181 | 26.018 | 69.96 |

## Observations and threats to validity

Positive improvement means faster in this run; negative improvement means slower. File skipping and elapsed time are reported separately. No speedup is assumed or selected after observing timings.

- This input has 288,103 rows. The file target is an experimental setting, not a production recommendation; Parquet/footer and Spark scheduling costs can dominate at small scales.
- Query predicates are selected deterministically from available data before timing, not from observed speedups. Sampling provenance is recorded under source.sampling when supplied; absence of that metadata is not evidence of a representative sample. Controlled B1 defects and synthetic CDC/evolution rows must be distinguished from the source population.
- One warmup and interleaving reduce some startup effects but do not eliminate JIT, GC, OS cache or fixed-order bias. Median does not eliminate all outliers. Raw runs and sample dispersion remain available.
- The same machine performs reads and writes; these are local filesystem results. Task bytes include filesystem reads and can include read-ahead/footer activity, not a physical disk measurement.
- Compaction, clustering, compression and file count interact. Report preparation cost and all file counts; do not attribute every elapsed-time change solely to data skipping.
- 5 repetitions per condition report observed variability, not a claim of statistical significance. These local warm-cache results cannot establish production capacity.

## References

- [Delta liquid clustering and OPTIMIZE FULL](https://docs.delta.io/delta-clustering/)
- [Delta OPTIMIZE and Z-order](https://docs.delta.io/optimizations-oss/)
- [Spark monitoring and task input metrics](https://spark.apache.org/docs/4.0.1/monitoring.html)
