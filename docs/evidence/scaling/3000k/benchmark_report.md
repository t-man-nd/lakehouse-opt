# D2 — Silver storage optimization benchmark

Run UTC: 2026-09-24T12:54:55.546369+00:00. Source: `/Users/haaminh109/Desktop/Minh/Tài liệu học tập (DSEB)/Kì 7/Big Data/lakehouse-opt/data/scale_runs/3000k/run_01/silver`, version 2, **2,880,103 rows**.

## Method

The four required conditions run in order baseline → Z-ORDER 1 column → Z-ORDER 2 columns → CLUSTER BY + OPTIMIZE FULL, repeated for each query. All start from byte-identical baseline files and preserve every pinned Silver row.

Each query/layout has one untimed warmup and 5 timed executions. Spark DataFrame/catalog cache is cleared each time. OS page cache is **not** flushed: this is a warm-cache local benchmark. Delta planning is completed before timing; collection, task execution and result transfer are timed. Preparation, optimization and metrics collection are excluded.

`files_scanned` is the executed Spark scan operator counter. `bytes_read` is the filesystem input-byte counter of successful tasks in that query’s jobs; it is not physical disk traffic and not a sum of whole Parquet file sizes. Job/stage IDs and raw samples are retained in results.json.

Runtime: Spark 4.0.1, Delta 4.0.1, local[4], UTC; AQE disabled during measurements. Baseline partitions: 256. Layout target: 1,048,576 bytes. Supplemental compaction uses the original 1 GiB target; actual file sizes are reported below.

## Layouts before/after

| Condition | Files before | Files after | Bytes before | Bytes after | Optimize seconds |
|---|---:|---:|---:|---:|---:|
| Baseline | 256 | 256 | 123734051 | 123734051 | 2e-06 |
| Z-ORDER 1 column | 256 | 118 | 123734051 | 115275068 | 9.779371 |
| Z-ORDER 2 columns | 256 | 118 | 123734051 | 107846383 | 8.046252 |
| CLUSTER BY + OPTIMIZE FULL | 256 | 118 | 123734051 | 106439874 | 7.677131 |
| OPTIMIZE compaction control | 256 | 1 | 123734051 | 107949855 | 6.957013 |

## Q1_1D_PointLookup (1D)

Predicate: `PULocationID = 161`. Matching trips: **134289**. All results equal the pinned source query.

| Condition | Median ms | Improvement % | Median files scanned | Median input bytes |
|---|---:|---:|---:|---:|
| Baseline | 153.6873 | 0.0 | 256 | 57279248 |
| Z-ORDER 1 column | 45.269 | 70.54 | 7 | 1656752 |
| Z-ORDER 2 columns | 73.7217 | 52.03 | 34 | 9349882 |
| CLUSTER BY + OPTIMIZE FULL | 70.9371 | 53.84 | 35 | 10285892 |

## Q2_2D_LocationAndTime (2D)

Predicate: `PULocationID = 161 AND tpep_pickup_datetime >= TIMESTAMP '2025-01-23 00:00:00' AND tpep_pickup_datetime < TIMESTAMP '2025-01-24 00:00:00'`. Matching trips: **2309**. All results equal the pinned source query.

| Condition | Median ms | Improvement % | Median files scanned | Median input bytes |
|---|---:|---:|---:|---:|
| Baseline | 177.6315 | 0.0 | 256 | 90853648 |
| Z-ORDER 1 column | 38.6841 | 78.22 | 7 | 3532376 |
| Z-ORDER 2 columns | 24.3129 | 86.31 | 3 | 1205110 |
| CLUSTER BY + OPTIMIZE FULL | 25.2336 | 85.79 | 2 | 1122916 |

## Q3_2D_RangeAggregation (2D)

Predicate: `PULocationID BETWEEN 146 AND 176 AND DOLocationID BETWEEN 222 AND 252`. Matching trips: **176001**. All results equal the pinned source query.

| Condition | Median ms | Improvement % | Median files scanned | Median input bytes |
|---|---:|---:|---:|---:|
| Baseline | 125.4932 | 0.0 | 256 | 57279248 |
| Z-ORDER 1 column | 48.5662 | 61.3 | 25 | 6341360 |
| Z-ORDER 2 columns | 61.2249 | 51.21 | 66 | 19393332 |
| CLUSTER BY + OPTIMIZE FULL | 59.4216 | 52.65 | 63 | 18522376 |

## Supplemental compaction control

This separate paired phase runs baseline → compacted, repeated three or more times after its own warmups. Its medians must be compared within this phase, not substituted into the four-condition phase.

| Query | Baseline median ms | Compacted median ms | Improvement % |
|---|---:|---:|---:|
| Q1_1D_PointLookup | 107.4469 | 90.3775 | 15.89 |
| Q2_2D_LocationAndTime | 160.2377 | 142.0519 | 11.35 |
| Q3_2D_RangeAggregation | 154.2759 | 138.2191 | 10.41 |

## Observations and threats to validity

Positive improvement means faster in this run; negative improvement means slower. File skipping and elapsed time are reported separately. No speedup is assumed or selected after observing timings.

- This input has 2,880,103 rows. The file target is an experimental setting, not a production recommendation; Parquet/footer and Spark scheduling costs can dominate at small scales.
- Query predicates are selected deterministically from available data before timing, not from observed speedups. Sampling provenance is recorded under source.sampling when supplied; absence of that metadata is not evidence of a representative sample. Controlled B1 defects and synthetic CDC/evolution rows must be distinguished from the source population.
- One warmup and interleaving reduce some startup effects but do not eliminate JIT, GC, OS cache or fixed-order bias. Median does not eliminate all outliers. Raw runs and sample dispersion remain available.
- The same machine performs reads and writes; these are local filesystem results. Task bytes include filesystem reads and can include read-ahead/footer activity, not a physical disk measurement.
- Compaction, clustering, compression and file count interact. Report preparation cost and all file counts; do not attribute every elapsed-time change solely to data skipping.
- 5 repetitions per condition report observed variability, not a claim of statistical significance. These local warm-cache results cannot establish production capacity.

## References

- [Delta liquid clustering and OPTIMIZE FULL](https://docs.delta.io/delta-clustering/)
- [Delta OPTIMIZE and Z-order](https://docs.delta.io/optimizations-oss/)
- [Spark monitoring and task input metrics](https://spark.apache.org/docs/4.0.1/monitoring.html)
