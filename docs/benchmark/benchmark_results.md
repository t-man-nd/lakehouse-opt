# Optimization benchmark (D2)

Silver snapshot rows: 20,072,697 · rounds: 5 (+1 warm-up) · target file size: 32 MB · baseline files: 200

## Table variants (files before/after)

| Variant | Files before | Files after | Size MB | Avg file MB | How prepared |
|---|---:|---:|---:|---:|---|
| baseline | 200 | 200 | 3184.3 | 15.92 | repartition(200) of Silver (random order) |
| compacted | 200 | 100 | 3151.6 | 31.52 | OPTIMIZE |
| zorder_1col | 200 | 99 | 3135.4 | 31.67 | OPTIMIZE |
| zorder_2col | 200 | 99 | 3115.9 | 31.47 | OPTIMIZE |
| liquid_cluster | 67 | 98 | 3111.7 | 31.75 | CLUSTER BY + OPTIMIZE FULL |

## q1_zone: PULocationID = 132 (JFK)

| Variant | Median ms | Min | Max | Files scanned | MB read | Improvement vs baseline |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 172.5 | 165.31 | 195.55 | 200 | 3184.29 | 0.0% |
| compacted | 167.7 | 161.3 | 177.57 | 100 | 3151.57 | 2.8% |
| zorder_1col | 108.0 | 105.89 | 118.37 | 5 | 164.38 | 37.4% |
| zorder_2col | 128.8 | 125.05 | 133.0 | 42 | 1317.59 | 25.3% |
| liquid_cluster | 125.2 | 120.24 | 128.71 | 35 | 1116.5 | 27.4% |

## q2_zone_week: PULocationID = 161 AND pickup in 2025-02-10..16

| Variant | Median ms | Min | Max | Files scanned | MB read | Improvement vs baseline |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 275.1 | 263.71 | 285.53 | 200 | 3184.29 | 0.0% |
| compacted | 267.8 | 265.23 | 277.65 | 100 | 3151.57 | 2.7% |
| zorder_1col | 119.1 | 115.53 | 128.94 | 5 | 165.67 | 56.7% |
| zorder_2col | 127.1 | 115.93 | 131.35 | 6 | 188.72 | 53.8% |
| liquid_cluster | 117.7 | 112.86 | 124.49 | 3 | 93.56 | 57.2% |

## q3_control: payment_type = 2 (not clustered; control)

| Variant | Median ms | Min | Max | Files scanned | MB read | Improvement vs baseline |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 185.2 | 163.14 | 195.44 | 200 | 3184.29 | 0.0% |
| compacted | 167.5 | 162.54 | 186.23 | 100 | 3151.57 | 9.6% |
| zorder_1col | 166.1 | 154.61 | 178.45 | 99 | 3135.4 | 10.3% |
| zorder_2col | 159.1 | 152.69 | 174.76 | 99 | 3115.91 | 14.1% |
| liquid_cluster | 166.5 | 158.19 | 178.19 | 98 | 3111.69 | 10.1% |

Results identical across variants: `{'q1_zone': True, 'q2_zone_week': True, 'q3_control': True}`

## Threats to validity

* **OS page cache.** Spark caches are cleared before every run, but the operating
  system's file cache cannot be dropped without root. After the warm-up round every
  variant is equally "warm", so the comparison is fair but absolute times are optimistic
  compared with cold cloud object storage. Files scanned and bytes read are unaffected
  by caching and are the primary evidence.
* **Local disk, one machine.** Object stores (S3/ADLS) add per-file request latency,
  which makes the small-file penalty *larger* than measured here.
* **Target file size.** OPTIMIZE's default 1 GB target would leave ~1 file for this
  dataset and make skipping impossible, so the target was scaled to the data size
  (`--target-file-mb`). Results depend on this choice.
* **Query selection.** Q1/Q2 filter on the clustering columns by design; Q3 is the
  control. Workloads filtering on other columns will not see these gains.
* **Snapshot/metadata caching.** Delta caches the transaction-log snapshot per JVM;
  planning time after the first run is lower for all variants alike.
* **Variance.** Medians over N interleaved rounds are reported with min/max; small
  differences (a few %) are within noise.

