# Optimization benchmark (D2)

Silver snapshot rows: 20,072,697 · rounds: 5 (+1 warm-up) · target file size: 32 MB · baseline files: 200

## Table variants (files before/after)

| Variant | Files before | Files after | Size MB | Avg file MB | How prepared |
|---|---:|---:|---:|---:|---|
| baseline | 200 | 200 | 3182.2 | 15.91 | repartition(200) of Silver (random order) |
| compacted | 200 | 100 | 3149.3 | 31.49 | OPTIMIZE |
| zorder_1col | 200 | 99 | 3132.2 | 31.64 | OPTIMIZE |
| zorder_2col | 200 | 99 | 3113.7 | 31.45 | OPTIMIZE |
| liquid_cluster | 67 | 98 | 3109.1 | 31.73 | CLUSTER BY + OPTIMIZE FULL |

## q1_zone: PULocationID = 132 (JFK)

| Variant | Median ms | Min | Max | Files scanned | MB read | Improvement vs baseline |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 302.8 | 253.31 | 1703.86 | 200 | 3182.15 | 0.0% |
| compacted | 298.7 | 235.8 | 1118.31 | 100 | 3149.29 | 1.4% |
| zorder_1col | 120.8 | 116.64 | 347.0 | 5 | 162.93 | 60.1% |
| zorder_2col | 151.5 | 141.46 | 412.52 | 33 | 1039.04 | 50.0% |
| liquid_cluster | 180.8 | 141.22 | 401.28 | 29 | 908.33 | 40.3% |

## q2_zone_week: PULocationID = 161 AND pickup in 2025-02-10..16

| Variant | Median ms | Min | Max | Files scanned | MB read | Improvement vs baseline |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 624.5 | 452.87 | 1258.35 | 200 | 3182.15 | 0.0% |
| compacted | 412.1 | 344.18 | 1446.12 | 100 | 3149.29 | 34.0% |
| zorder_1col | 151.8 | 131.67 | 502.39 | 6 | 189.92 | 75.7% |
| zorder_2col | 158.1 | 120.93 | 326.55 | 5 | 152.61 | 74.7% |
| liquid_cluster | 127.3 | 117.1 | 319.34 | 2 | 57.53 | 79.6% |

## q3_control: payment_type = 2 (not clustered; control)

| Variant | Median ms | Min | Max | Files scanned | MB read | Improvement vs baseline |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 302.6 | 248.82 | 699.05 | 200 | 3182.15 | 0.0% |
| compacted | 254.7 | 236.01 | 869.81 | 100 | 3149.29 | 15.8% |
| zorder_1col | 274.8 | 246.89 | 712.08 | 99 | 3132.22 | 9.2% |
| zorder_2col | 298.7 | 254.65 | 1095.92 | 99 | 3113.75 | 1.3% |
| liquid_cluster | 260.9 | 250.96 | 1182.46 | 98 | 3109.08 | 13.8% |

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

