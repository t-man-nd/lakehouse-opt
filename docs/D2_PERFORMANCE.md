# D2 — Silver storage optimization benchmark

Run UTC: 2026-09-24T11:09:54.789076+00:00. Source: `/Users/haaminh109/Desktop/Minh/Tài liệu học tập (DSEB)/Kì 7/Big Data/lakehouse-opt/data/e1_runs/official_20260924_verified/silver`, version 2, **2,983 rows**.

## Method

The four required conditions run in order baseline → Z-ORDER 1 column → Z-ORDER 2 columns → CLUSTER BY + OPTIMIZE FULL, repeated for each query. All start from byte-identical baseline files and preserve every pinned Silver row.

Each query/layout has one untimed warmup and 3 timed executions. Spark DataFrame/catalog cache is cleared each time. OS page cache is **not** flushed: this is a warm-cache local benchmark. Delta planning is completed before timing; collection, task execution and result transfer are timed. Preparation, optimization and metrics collection are excluded.

`files_scanned` is the executed Spark scan operator counter. `bytes_read` is the filesystem input-byte counter of successful tasks in that query’s jobs; it is not physical disk traffic and not a sum of whole Parquet file sizes. Job/stage IDs and raw samples are retained in results.json.

Runtime: Spark 4.0.1, Delta 4.0.1, local[2], UTC; AQE disabled during measurements. Layout target: 32,768 bytes for the small classroom experiment. Supplemental compaction uses the original 1 GiB target; the sample is far smaller than 1 GiB.

## Layouts before/after

| Condition | Files before | Files after | Bytes before | Bytes after | Optimize seconds |
|---|---:|---:|---:|---:|---:|
| Baseline | 32 | 32 | 419375 | 419375 | 1e-06 |
| Z-ORDER 1 column | 32 | 12 | 419375 | 225843 | 0.823667 |
| Z-ORDER 2 columns | 32 | 12 | 419375 | 220510 | 0.764497 |
| CLUSTER BY + OPTIMIZE FULL | 32 | 12 | 419375 | 220054 | 1.152119 |
| OPTIMIZE compaction control | 32 | 1 | 419375 | 108102 | 0.645442 |

## Q1_1D_PointLookup (1D)

Predicate: `PULocationID = 161`. Matching trips: **69**. All results equal the pinned source query.

| Condition | Median ms | Improvement % | Median files scanned | Median input bytes |
|---|---:|---:|---:|---:|
| Baseline | 35.0211 | 0.0 | 32 | 526985 |
| Z-ORDER 1 column | 22.321 | 36.26 | 1 | 17980 |
| Z-ORDER 2 columns | 27.3555 | 21.89 | 12 | 220366 |
| CLUSTER BY + OPTIMIZE FULL | 24.7689 | 29.27 | 12 | 220014 |

## Q2_2D_LocationAndTime (2D)

Predicate: `PULocationID = 161 AND tpep_pickup_datetime >= TIMESTAMP '2025-01-01 00:00:00' AND tpep_pickup_datetime < TIMESTAMP '2025-01-02 00:00:00'`. Matching trips: **26**. All results equal the pinned source query.

| Condition | Median ms | Improvement % | Median files scanned | Median input bytes |
|---|---:|---:|---:|---:|
| Baseline | 31.0884 | 0.0 | 32 | 879649 |
| Z-ORDER 1 column | 20.0028 | 35.66 | 1 | 35456 |
| Z-ORDER 2 columns | 20.3855 | 34.43 | 5 | 178976 |
| CLUSTER BY + OPTIMIZE FULL | 20.3493 | 34.54 | 5 | 179232 |

## Q3_2D_RangeAggregation (2D)

Predicate: `PULocationID BETWEEN 146 AND 176 AND DOLocationID BETWEEN 155 AND 185`. Matching trips: **74**. All results equal the pinned source query.

| Condition | Median ms | Improvement % | Median files scanned | Median input bytes |
|---|---:|---:|---:|---:|
| Baseline | 34.909 | 0.0 | 32 | 526985 |
| Z-ORDER 1 column | 19.2403 | 44.88 | 3 | 54644 |
| Z-ORDER 2 columns | 21.2442 | 39.14 | 12 | 220366 |
| CLUSTER BY + OPTIMIZE FULL | 23.9467 | 31.4 | 12 | 220014 |

## Supplemental compaction control

This separate paired phase runs baseline → compacted, repeated three or more times after its own warmups. Its medians must be compared within this phase, not substituted into the four-condition phase.

| Query | Baseline median ms | Compacted median ms | Improvement % |
|---|---:|---:|---:|
| Q1_1D_PointLookup | 30.4889 | 16.0768 | 47.27 |
| Q2_2D_LocationAndTime | 33.1808 | 25.0095 | 24.63 |
| Q3_2D_RangeAggregation | 48.905 | 21.3304 | 56.38 |

## Observations and threats to validity

Positive improvement means faster in this run; negative improvement means slower. File skipping and elapsed time are reported separately. No speedup is assumed or selected after observing timings.

- The roughly 3,000-row teaching sample demonstrates operators and counters, not production throughput. A deliberately small target makes multiple clustered files observable; Parquet/footer and Spark scheduling costs can dominate.
- B1 takes the first clean TLC rows, concentrated around month boundaries; predicates are selected deterministically from available data before timing, so this is not a representative NYC workload.
- One warmup and interleaving reduce some startup effects but do not eliminate JIT, GC, OS cache or fixed-order bias. Median does not eliminate all outliers. Raw runs and sample dispersion remain available.
- The same machine performs reads and writes; these are local filesystem results. Task bytes include filesystem reads and can include read-ahead/footer activity, not a physical disk measurement.
- Compaction, clustering, compression and file count interact. Report preparation cost and all file counts; do not attribute every elapsed-time change solely to data skipping.
- Three repetitions meet the rubric but do not establish statistical significance. Rerun with more iterations and a representative larger Silver snapshot for capacity decisions.

## References

- [Delta liquid clustering and OPTIMIZE FULL](https://docs.delta.io/delta-clustering/)
- [Delta OPTIMIZE and Z-order](https://docs.delta.io/optimizations-oss/)
- [Spark monitoring and task input metrics](https://spark.apache.org/docs/4.0.1/monitoring.html)
