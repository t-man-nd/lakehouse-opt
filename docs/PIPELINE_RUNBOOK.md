# Pipeline runbook (real TLC lane)

## 1. Setup

* **Python 3.11 or 3.12.** Not 3.14: `pyarrow<22` has no wheels for it and PySpark 4.0.x does
  not support it. `uv venv --python 3.12 .venv` fetches the right Python automatically.
* **Java 17 (or 21)**, visible through `JAVA_HOME`:

  ```bash
  sudo apt install openjdk-17-jdk-headless        # Ubuntu / Debian
  sudo dnf install java-17-openjdk-headless       # Fedora
  sudo pacman -S jdk17-openjdk                    # Arch
  export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(which java)")")")"
  echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc
  java -version                                   # must print 17.x or 21.x
  ```
* Internet on the first run (Spark downloads the Delta JARs
  `io.delta:delta-spark_2.13:4.0.1` from Maven Central into `~/.ivy2`).
* Windows: set `JAVA_HOME` and `HADOOP_HOME` (winutils) as for the C2 work.
* Memory: the full 6-month run needs about 6–8 GB for the driver.

```bash
python -m venv .venv && source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
# Driver memory defaults to half your RAM (2-12 GB). Override only if needed, and note that
# an `export` is lost when you open a new terminal:
# export SPARK_DRIVER_MEMORY=8g                            # Windows: set SPARK_DRIVER_MEMORY=8g
```

## 2. Run

```bash
python scripts/download_sources.py                 # ~1 GB download, once
python lakehouse_pipeline.py --dev-sample 1        # optional: 1 % rehearsal in data/lakehouse-dev (minutes)
python lakehouse_pipeline.py                       # full run, all steps except benchmark
python lakehouse_pipeline.py --steps benchmark     # D2 lab (builds 5 table copies; slow)
python -m pytest -q                                # unit + end-to-end tests
```

Other entry points:

| Command | Purpose |
|---|---|
| `python lakehouse_pipeline.py --steps reference bronze profile` | Explore first, then tune thresholds (docs/DQ_RULES.md) |
| `python lakehouse_pipeline.py --steps silver gold --rebuild` | Rebuild Silver/Gold after changing a rule (Bronze kept) |
| `python lakehouse_pipeline.py --source fixture` | B1-v1.0 fixture lane (3,060 / 2,880 / 180) |
| `python optimization_benchmark.py --rounds 5 --target-file-mb 32 --rebuild` | Benchmark alone, custom settings |

Every step is idempotent: re-running skips Bronze batches already ingested (by `_batch_id`,
which embeds the file hash), skips Silver batches already in `ops/silver_batch_log`, does
nothing if the CDC feed was applied, and rebuilds Gold from the current Silver snapshot.

## 3. Step outputs

| Step | Delta tables written | Evidence file |
|---|---|---|
| reference | `bronze/zone_lookup`, `bronze/cbd_zones`, `silver/dim_zone` | — |
| bronze | `bronze/yellow_trips` (partitioned by `_source_month`) | `docs/evidence/bronze_history.json` |
| profile | — | `docs/profile/bronze_profile.{json,md}` |
| silver | `silver/yellow_trips` (by `pickup_month`), `silver/yellow_trips_rejected`, `ops/silver_batch_log` | reconciliation table in `docs/RESULTS.md` |
| cdc | Bronze batch `cdc_corrections_seed42`, Silver MERGE | `docs/evidence/cdc_merge.json`, `data/raw/cdc/` |
| evolution | — (verifies the change that ingest made) | `docs/evidence/schema_evolution.json` |
| time_travel | copy in `data/lakehouse/_scratch/` | `silver_history.json`, `time_travel.json`, `vacuum_demo.json`, `delta_log_annotated.md` |
| gold | `gold/zone_hourly_metrics`, `gold/cbd_flow_monthly`, `gold/cbd_flow_yoy`, `gold/dq_monthly` | `docs/evidence/gold_verification.json` |
| benchmark | `data/lakehouse/benchmark/<variant>` | `docs/benchmark/benchmark_results.{json,md}` |
| (every run) | — | `docs/pipeline_run.json`, `docs/RESULTS.md` |

## 4. Task-plan mapping (PCCV sheet)

| Task | Acceptance check | Where it is implemented | Evidence |
|---|---|---|---|
| B1 contract + generator | contract, 4 rules, manifest | team files (unchanged); TLC-v2 contract in `src/dq_rules.py` | `docs/DQ_RULES.md` |
| B2 Bronze | metadata `_ingest_ts/_source_file/_batch_id`, append, mergeSchema, no filtering | `src/tlc_bronze.py` (+ `_source_month`, `_file_sha256`, commit userMetadata) | row counts vs manifest, `bronze_history.json` |
| B3 Silver + quarantine | try_cast under ANSI, CORRECTED parser, `reject_reason`, silver + rejected = bronze | `src/dq_rules.py`, `src/tlc_silver.py` | per-batch conservation in `RESULTS.md`; `tests/test_dq_rules.py` |
| C1 CDC/MERGE | update + insert in one MERGE, fixture from real ids, fixed seed, only money columns change, `updated > 0`, `after = before + inserted` | `tlc_silver.apply_cdc` | `cdc_merge.json` (planned vs Delta metrics, before/after samples) |
| C2 Schema evolution | no rebuild, old rows NULL, DESCRIBE HISTORY on Bronze and Silver | real `cbd_congestion_fee` from 2025 files; `tlc_silver.apply_evolution` | `schema_evolution.json` (commit version, operation, before/after schema) |
| C3 Time travel / log / VACUUM | ≥ 2 old versions, VACUUM on a copy, retention-check hack labelled, `_delta_log` stats explained | `src/time_travel.py` | `time_travel.json`, `vacuum_demo.json`, `delta_log_annotated.md` |
| D1 Gold | tip % = AVG(tip/fare) with fare > 0, earnings = fare + tip + extra, UTC, one-command rebuild, matches manual query | `src/gold.py` | `gold_verification.json` (manual SQL cells + totals) |
| D2 Performance | 4+ conditions, interleaved, ≥ 3 runs, median, files + bytes scanned, file counts, anti-cache, threats | `optimization_benchmark.py` | `benchmark_results.md` |
| E1 End-to-end | one command, `pipeline_run.json` | `lakehouse_pipeline.py` | `docs/pipeline_run.json`, `docs/RESULTS.md` |
| E2 Tests / idempotency | rerun gives identical counts, "not-a-date" → `INVALID_PICKUP_DATETIME`, tip % test | `tests/` | `pytest` output |

### E1 function signatures

| Planned signature | Implemented as |
|---|---|
| `bronze.run(spark, months, overwrite)` | `src.tlc_bronze.run(spark, months, overwrite=False, paths=None, dev_sample_pct=None)` |
| `silver.build(spark)` | `src.tlc_silver.build(spark, paths=None, thresholds=DQThresholds(), rebuild=False)` |
| `silver.apply_cdc(spark)` | `src.tlc_silver.apply_cdc(spark, paths=None, seed=42, n_updates=25, n_inserts=5)` |
| `silver.apply_evolution(spark)` | `src.tlc_silver.apply_evolution(spark, paths=None)` |
| `gold.run(spark)` | `src.gold.run(spark, paths=None, months=None)` |

All return a dict of counts. The real-lane modules are prefixed `tlc_` so the team's verified
fixture modules (`src/bronze.py`, `src/silver.py`) stay unchanged. `src/schema_evolution.py`
(synthetic `surcharge_fee`) is superseded by the real evolution but kept for history.

## 5. Tests

| File | Needs Delta | Covers |
|---|---|---|
| `tests/test_dq_rules.py` | no | planted anomalies found exactly, conservation, key semantics, ANSI parsing, determinism, per-batch schema |
| `tests/test_gold_logic.py` | no | tip % formula, earnings, exclusions, CBD flow, policy/leap-year day counts, YoY |
| `tests/test_support_tools.py` | no | scan metrics from the executed plan, manifest drift, CBD zone detection, `_delta_log` parsing |
| `tests/test_offline_smoke.py` | no | profiling, CDC feed builder, DQ mart, Gold SQL self-check, results renderer |
| `tests/test_pipeline_e2e.py` | yes | full run on synthetic TLC-shaped data: B2–E2 acceptance criteria and idempotent re-runs |
| `tests/test_silver.py` | yes | team's B3 fixture tests (unchanged) |

`tests/tlc_like.py` generates the synthetic TLC-shaped files (with the real 2024/2025 schema
drift and planted anomalies). Without the Delta JARs, Delta tests skip with the reason shown;
`LAKEHOUSE_TEST_NO_DELTA=1` forces that mode.

## 6. Check on the first real run

1. **CBD zone count** in `docs/source_data_manifest.md` looks plausible (see DATA_SOURCES.md).
2. **Thresholds**: fill the table at the end of `docs/DQ_RULES.md` from the profile.
3. **Key quality**: `keys_with_conflicting_content` in the profile should be tiny; if not,
   discuss adding `trip_distance` to the key.
4. **Liquid clustering**: if `OPTIMIZE ... FULL` fails on a path-based table in your Delta
   build, run plain `OPTIMIZE` for that variant and note it under threats to validity.
5. **Benchmark file size**: with `--target-file-mb 32` the optimized tables should have tens of
   files; if they collapse to 1–3 files, lower the target so data skipping has something to skip.

## 7. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `unresolved dependency: io.delta#delta-spark_2.13` | No internet to Maven on first run; connect once, JARs are cached |
| `Bronze row count mismatch` | File changed on disk after the manifest was built; re-run `download_sources.py --skip-download` |
| `MERGE inserted X but plan expected Y` | Two source rows share a key in one batch (dedup bypassed) or Silver was edited by hand; rebuild Silver |
| `Could not detect a LocationID or zone-name column` | CBD dataset header differs; add the column name to `_ID_CANDIDATES` in `scripts/download_sources.py` |
| `JAVA_GATEWAY_EXITED` / `JAVA_HOME is not set` | Java missing or not on PATH; see section 1 |
| `HTTP Error 403` for data.ny.gov | The script tries 5 endpoints; if all fail, export the CBD dataset as CSV/GeoJSON in a browser to `data/raw/ref/cbd_zones_raw.csv` and re-run |
| `OutOfMemoryError: Java heap space` | Every run prints its `[spark] driver memory ...` line: check it. Raise `SPARK_DRIVER_MEMORY` (about half your RAM), or raise `SPARK_SHUFFLE_PARTITIONS` (e.g. 128) to make each task smaller, or rehearse with `--dev-sample 1` |
| `Disk quota exceeded` during a shuffle | Spark spills to `data/spark-tmp/`; free space there or set `LAKEHOUSE_SPARK_TMP` to a bigger disk |
