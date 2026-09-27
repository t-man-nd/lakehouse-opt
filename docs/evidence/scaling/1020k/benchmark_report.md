# D2 — Silver storage optimization benchmark

Run UTC: 2026-09-24T12:45:17.962314+00:00. Source: `/Users/haaminh109/Desktop/Minh/Tài liệu học tập (DSEB)/Kì 7/Big Data/lakehouse-opt/data/scale_runs/1020k/run_01/silver`, version 2, **979,303 rows**.

## Method

The four required conditions run in order baseline → Z-ORDER 1 column → Z-ORDER 2 columns → CLUSTER BY + OPTIMIZE FULL, repeated for each query. All start from byte-identical baseline files and preserve every pinned Silver row.

Each query/layout has one untimed warmup and 5 timed executions. Spark DataFrame/catalog cache is cleared each time. OS page cache is **not** flushed: this is a warm-cache local benchmark. Delta planning is completed before timing; collection, task execution and result transfer are timed. Preparation, optimization and metrics collection are excluded.

`files_scanned` is the executed Spark scan operator counter. `bytes_read` is the filesystem input-byte counter of successful tasks in that query’s jobs; it is not physical disk traffic and not a sum of whole Parquet file sizes. Job/stage IDs and raw samples are retained in results.json.

Runtime: Spark 4.0.1, Delta 4.0.1, local[4], UTC; AQE disabled during measurements. Baseline partitions: 256. Layout target: 1,048,576 bytes. Supplemental compaction uses the original 1 GiB target; actual file sizes are reported below.

## Layouts before/after

| Condition | Files before | Files after | Bytes before | Bytes after | Optimize seconds |
|---|---:|---:|---:|---:|---:|
| Baseline | 256 | 256 | 44318056 | 44318056 | 1e-06 |
| Z-ORDER 1 column | 256 | 42 | 44318056 | 39251658 | 2.609738 |
| Z-ORDER 2 columns | 256 | 42 | 44318056 | 37553493 | 3.50731 |
| CLUSTER BY + OPTIMIZE FULL | 256 | 42 | 44318056 | 37159559 | 3.148657 |
| OPTIMIZE compaction control | 256 | 1 | 44318056 | 36430232 | 2.635488 |

## Q1_1D_PointLookup (1D)

Predicate: `PULocationID = 161`. Matching trips: **45411**. All results equal the pinned source query.

| Condition | Median ms | Improvement % | Median files scanned | Median input bytes |
|---|---:|---:|---:|---:|
| Baseline | 93.467 | 0.0 | 256 | 16458424 |
| Z-ORDER 1 column | 22.3352 | 76.1 | 2 | 520476 |
| Z-ORDER 2 columns | 31.4722 | 66.33 | 19 | 4809202 |
| CLUSTER BY + OPTIMIZE FULL | 33.4402 | 64.22 | 19 | 5581760 |

## Q2_2D_LocationAndTime (2D)

Predicate: `PULocationID = 161 AND tpep_pickup_datetime >= TIMESTAMP '2025-01-23 00:00:00' AND tpep_pickup_datetime < TIMESTAMP '2025-01-24 00:00:00'`. Matching trips: **804**. All results equal the pinned source query.

| Condition | Median ms | Improvement % | Median files scanned | Median input bytes |
|---|---:|---:|---:|---:|
| Baseline | 114.6345 | 0.0 | 256 | 47412768 |
| Z-ORDER 1 column | 22.3385 | 80.51 | 2 | 1056912 |
| Z-ORDER 2 columns | 19.7127 | 82.8 | 3 | 1204816 |
| CLUSTER BY + OPTIMIZE FULL | 20.6281 | 82.01 | 1 | 562636 |

## Q3_2D_RangeAggregation (2D)

Predicate: `PULocationID BETWEEN 146 AND 176 AND DOLocationID BETWEEN 222 AND 252`. Matching trips: **59602**. All results equal the pinned source query.

| Condition | Median ms | Improvement % | Median files scanned | Median input bytes |
|---|---:|---:|---:|---:|
| Baseline | 99.1652 | 0.0 | 256 | 16458424 |
| Z-ORDER 1 column | 33.9481 | 65.77 | 9 | 2439044 |
| Z-ORDER 2 columns | 39.5374 | 60.13 | 31 | 8967928 |
| CLUSTER BY + OPTIMIZE FULL | 39.377 | 60.29 | 33 | 9691328 |

## Supplemental compaction control

This separate paired phase runs baseline → compacted, repeated three or more times after its own warmups. Its medians must be compared within this phase, not substituted into the four-condition phase.

| Query | Baseline median ms | Compacted median ms | Improvement % |
|---|---:|---:|---:|
| Q1_1D_PointLookup | 86.1539 | 43.3895 | 49.64 |
| Q2_2D_LocationAndTime | 96.1311 | 48.7699 | 49.27 |
| Q3_2D_RangeAggregation | 98.9151 | 58.4967 | 40.86 |

## Observations and threats to validity

Positive improvement means faster in this run; negative improvement means slower. File skipping and elapsed time are reported separately. No speedup is assumed or selected after observing timings.

- This input has 979,303 rows. The file target is an experimental setting, not a production recommendation; Parquet/footer and Spark scheduling costs can dominate at small scales.
- Query predicates are selected deterministically from available data before timing, not from observed speedups. Sampling provenance is recorded under source.sampling when supplied; absence of that metadata is not evidence of a representative sample. Controlled B1 defects and synthetic CDC/evolution rows must be distinguished from the source population.
- One warmup and interleaving reduce some startup effects but do not eliminate JIT, GC, OS cache or fixed-order bias. Median does not eliminate all outliers. Raw runs and sample dispersion remain available.
- The same machine performs reads and writes; these are local filesystem results. Task bytes include filesystem reads and can include read-ahead/footer activity, not a physical disk measurement.
- Compaction, clustering, compression and file count interact. Report preparation cost and all file counts; do not attribute every elapsed-time change solely to data skipping.
- 5 repetitions per condition report observed variability, not a claim of statistical significance. These local warm-cache results cannot establish production capacity.

## References

- [Delta liquid clustering and OPTIMIZE FULL](https://docs.delta.io/delta-clustering/)
- [Delta OPTIMIZE and Z-order](https://docs.delta.io/optimizations-oss/)
- [Spark monitoring and task input metrics](https://spark.apache.org/docs/4.0.1/monitoring.html)
