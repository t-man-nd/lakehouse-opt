# Delta Lakehouse on The New York City Taxi and Limousine Commission Data

**Medallion Architecture, Incremental CDC, Schema Evolution, Time Travel, and Storage-Layout Optimization on 20.7 Million NYC Yellow Taxi Records**

**Authors:** Ninh Duy Tuân, Đoàn Tùng Lâm, Hà Quang Minh, Phạm Thị Ngọc Ánh, Phạm Đức Anh, Nguyễn Thị Mai Anh

**Runtime:** Python 3.11/3.12, Java 17/21, PySpark 4.0.1, delta-spark 4.0.1

**Date:** 1 October 2026
---

## Abstract

We present the design, implementation, and empirical evaluation of a Delta Lake–based lakehouse that ingests six months of NYC Yellow Taxi trip records (Q1 2024 and Q1 2025; 20,752,804 trips) through a Bronze–Silver–Gold medallion pipeline, and use it to study a concrete analytical question: how did congestion pricing, introduced on 5 January 2025, change trip volumes into, out of, and within Manhattan's central business district? Beyond the analysis, the system is engineered as a production service: every stage is idempotent, every batch is reconciled row-for-row, and every claim in this report is traceable to a machine-generated evidence file. Four results stand out. First, row conservation holds at scale: 20,752,834 Bronze rows equal 680,112 quarantined rows plus 20,072,697 inserted rows plus 25 updated rows. Second, a data-quality rule that initially flagged 31.0% of rows was diagnosed from the empirical gap distribution and redesigned, reducing the flag rate to 1.35% without altering any hard rejection. Third, the transaction log makes the cost of copy-on-write directly measurable: append-only MERGE operations copied zero rows, whereas a 30-row correction MERGE rewrote 3,934,729. Fourth, layout optimization yields true data skipping: a Z-ORDERed table answers a single-zone query by scanning 5 of 200 files and 164 MB instead of 3,184 MB (−94.8%), while a control query confirms that clustering does not help unrelated predicates. We also report a negative result: at 3,060 rows, compaction collapses every layout to one file and the benchmark is uninformative, which motivated the move to real data.

**Keywords:** data lakehouse, Delta Lake, medallion architecture, change data capture, schema evolution, data skipping, Z-ORDER, liquid clustering, data quality, NYC taxi

---

## Contents

1. [Introduction](#1-introduction)
2. [Theoretical and Architectural Foundations](#2-theoretical-and-architectural-foundations)
3. [System Design and Implementation](#3-system-design-and-implementation)
4. [Transaction-Log Analysis](#4-transaction-log-analysis)
5. [Storage-Layout Evaluation](#5-storage-layout-evaluation)
6. [Challenges, Limitations, and Lessons Learned](#6-challenges-limitations-and-lessons-learned)
7. [Conclusion and Future Work](#7-conclusion-and-future-work)
8. [References](#8-references)

## 1. Introduction

### 1.1 Motivation

Data warehouses offer transactional guarantees at high cost and with closed formats; data lakes offer cheap, open storage but no transactional semantics. A lakehouse places a table-format layer, here Delta Lake, over open Parquet files to obtain ACID transactions, versioning, and schema enforcement on inexpensive storage [1, 2]. The practical question for an engineering team is not whether these properties exist in principle but whether a concrete pipeline can *demonstrate* them on realistic, dirty data, at a scale where storage-layout decisions matter, and with enough operational discipline that the system can be rerun, audited, and trusted.

### 1.2 Objectives and contributions

This work pursues three objectives: (i) to explain and *evidence* how the Delta transaction log delivers atomicity, isolation, optimistic concurrency control, and time travel; (ii) to build a Bronze–Silver–Gold pipeline in which every design decision has a stated justification and a measurable outcome; and (iii) to quantify the effect of `OPTIMIZE`, `ZORDER`, and liquid clustering using files scanned and bytes read, not wall-clock time alone. The main contributions are:

1. An **incremental, idempotent, per-batch pipeline** over 20.7 M real records, with exact row conservation per batch and an auditable batch ledger (Sections 3.2–3.4).
2. A **diagnostic-driven revision of a data-quality rule**, replacing a single expected sum with an interval to account for three inconsistent encodings of congestion surcharges (Section 3.4.4).
3. **Quantified copy-on-write and VACUUM behavior** read directly from the transaction log (Sections 3.4.6, 3.5, 4).
4. A **layout benchmark with a control query** and I/O-level metrics taken from the executed physical plan (Section 5).
5. An honest account of **limits and negative results**, including the scale at which the benchmark first became informative (Sections 1.3, 5.4, and 6).

### 1.3 Data and experimental scope

**Why real data.** The project began with a small synthetic dataset (3,060 JSON rows with deliberately injected defects, hereafter the *fixture lane*) to validate cleaning rules. The focus then moved to real data because real data supplies, naturally, what each Delta feature requires:

**Table 1.** Requirements and how real data satisfies them.

| Requirement                   | How the real data provides it                                                                                            |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| A meaningful layout benchmark | \~20 M trips; at 108 KB neither a small-file problem nor data skipping can be observed (Section 1.3, last paragraph)     |
| Schema evolution              | 2024 files have 19 columns; 2025 files add `cbd_congestion_fee`, a genuine schema change rather than a fabricated column |
| Data cleaning                 | Genuinely dirty data forces *profile first, then define rules*                                                           |
| CDC                           | Late-arriving trips (119 flagged `LATE_ARRIVAL`) plus a seeded correction feed                                           |
| An analytical question        | Congestion pricing began 2025-01-05; two quarters one year apart support a year-over-year comparison                     |

**Sources.** Six Yellow Taxi Parquet files (2024-01…03 and 2025-01…03) from the NYC Taxi & Limousine Commission (TLC) [8]; the taxi-zone lookup table; and the MTA congestion-relief-zone list [9]. A manifest records the SHA-256 of every file, so a republished file is detected and ingested as a new batch rather than silently replacing history.

**Table 2.** Source trip files.

| Month     | Rows           | Columns |
| --------- | -------------- | ------- |
| 2024-01   | 2,964,624      | 19      |
| 2024-02   | 3,007,526      | 19      |
| 2024-03   | 3,582,628      | 19      |
| 2025-01   | 3,475,226      | 20      |
| 2025-02   | 3,577,543      | 20      |
| 2025-03   | 4,145,257      | 20      |
| **Total** | **20,752,804** |         |

**Two disclosures about sources.** (a) The data.ny.gov endpoint returned HTTP 403 from the team's network. The CBD zone list therefore combines 33 official zones (the readable portion of the export) with 5 zones inferred from geography; the inference is tested against the data in Section 3.6.3. (b) An external reconciliation against TLC's published aggregates was not performed; the corresponding step reports `SKIPPED`.

**Two synthetic components, both labelled.** (1) The fixture lane (Section 3.4.5); (2) a seeded CDC correction feed (Section 3.4.6). Every other row in Bronze, Silver, and Gold is real TLC data.

**Two lanes, two roles.** The fixture lane supplies *ground truth*: because exactly 180 defects are injected, the equality `Silver 2,880 + Rejected 180 = Bronze 3,060` can be verified reason by reason, which serves as a unit test of rule semantics. The real-data lane supplies *scale and realism*: it shows that the same architecture holds when the dirt is not planned. A real-data pipeline cannot, by itself, prove that a rule catches everything it should, because the correct answer is unknown; the fixture lane cannot show behavior at scale. The two are complementary.

**A negative result that shaped the design.** At fixture scale (2,983 rows, 108 KB), `OPTIMIZE` collapses every layout to a single file, so `files_scanned` is 1 for all variants and data skipping has nothing to skip. Measured median-latency differences (tens of milliseconds) were within noise, and the sign of the effect even flipped between two runs. This *small-data paradox* is the direct reason the benchmark was rebuilt on real data (Section 5).

### 1.4 Design decisions at a glance

**Table 3.** Principal design decisions and their justification.

| Decision                                                                                | Justification                                                                                                       | Section |
| --------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- | ------- |
| Bronze neither filters nor deduplicates                                                 | Silver must be re-derivable when rules change; Bronze log statistics still record the raw defects                   | 3.3     |
| `trip_key` = SHA-256(VendorID, pickup, dropoff, PU, DO), **excluding** monetary columns | A fare/tip correction must resolve to an UPDATE, not a new trip                                                     | 3.4.2   |
| Two outcomes: *hard rejection* and *soft flag*                                          | Rejecting every odd row over-cleans (a zero-passenger trip still earned a fare); flagging keeps the data measurable | 3.4.1   |
| Silver processes one batch at a time, reading Bronze at the version that committed it   | Idempotency, per-batch conservation, and each batch retains its original schema                                     | 3.4.3   |
| `TOTAL_MISMATCH` compares against an interval                                           | The source encodes surcharges three different ways                                                                  | 3.4.4   |
| Gold tip percentage = `AVG(tip/fare)` per trip, reported for all trips and card trips   | TLC records tips only for card payments                                                                             | 3.6     |
| Benchmark target file size 32 MB, 200-file baseline                                     | The default 1 GB target leaves \~3 files and nothing to skip                                                        | 5.1     |

### 1.5 Organization

Section 2 provides the technical foundations; Section 3 describes the system and implementation; Section 4 analyzes the transaction log; Section 5 evaluates storage layouts; Section 6 documents limitations and operational lessons; Section 7 concludes and identifies next steps.

---

## 2. Theoretical and Architectural Foundations

### 2.1 Data warehouse, data lake, and data lakehouse

The three architectures differ less in *where* data is stored than in the *table semantics* layered over storage.

- **Data warehouse** (e.g., Teradata, Redshift, Snowflake). The system owns data at table level, enforces schema on write, optimizes query plans, and provides ACID transactions. Costs are high, ETL pipelines are long, machine-learning workloads often require exported copies, and storage formats are frequently vendor-specific.
- **Data lake** (HDFS, S3, ADLS, GCS). Any data type is stored in open formats at low cost and is readable by many engines. However, a directory of Parquet files is *not* a transactional table: a failed job leaves partial files visible to readers, concurrent writers are not detected, there is no version history, row-level UPDATE/DELETE requires rewriting entire partitions, and schemas drift across files.
- **Data lakehouse** (Delta Lake, Apache Iceberg, Apache Hudi). A transactional metadata layer sits between the compute engine and open files. Data remains on decoupled storage while the table format supplies snapshots, history, and read/write rules, so BI and ML workloads share a single copy.

[Figure 1. Position of the table-format layer between compute engines and open file storage.](report/figures/lakehouse_layers.png)

**Table 4.** Comparison of the three architectures.

| Criterion           | Warehouse                  | Lake (plain files)            | Lakehouse                               |
| ------------------- | -------------------------- | ----------------------------- | --------------------------------------- |
| Unit of management  | Table in a DBMS/MPP system | Files and directories         | Logical table over open files           |
| Schema handling     | Schema-on-write            | Schema-on-read                | Enforcement plus controlled evolution   |
| Storage/compute     | Often tightly coupled      | Decoupled                     | Decoupled, mediated by the table format |
| Table-level ACID    | Yes                        | No                            | Yes (via the log)                       |
| UPDATE/DELETE/MERGE | Native                     | Difficult, unsafe             | Row-level via the transactional layer   |
| Version history     | System-dependent           | None                          | Snapshots, time travel, audit           |
| Principal risk      | Cost, lock-in              | No transactions or governance | Metadata operations, tuning             |

**Reading Armbrust et al. (CIDR 2021) [1].** The paper identifies four problems with two-tier "lake plus warehouse" designs (reliability, data staleness, limited support for advanced analytics, and total cost) and proposes three ideas: a transactional metadata layer over open file formats, layout and caching optimizations to recover SQL performance, and declarative DataFrame APIs for ML. Two caveats apply: the TPC-DS results were measured by the authors' own organization on its own engine, and limitations noted in the paper (single-table transactions; commit throughput constrained when the log resides on an object store) reflect 2021.

### 2.2 The Delta transaction log (`_delta_log/`)

#### 2.2.1 Physical structure

A Delta table is a directory of **immutable** Parquet files plus a `_delta_log/` directory. Every successful transaction writes exactly **one** sequentially numbered JSON file. The log is the **single source of truth**: a Parquet file present on disk is not thereby part of the table.

**Table 5.** Actions recorded in a commit.

| Action       | Role                                                                                              |
| ------------ | ------------------------------------------------------------------------------------------------- |
| `protocol`   | Minimum reader/writer versions                                                                    |
| `metaData`   | Table id, `schemaString`, partition columns, configuration                                        |
| `add`        | Adds a file: path, size, `dataChange`, `stats` (record count, min/max, null counts)               |
| `remove`     | Tombstone: removes a file from the current snapshot (not yet deleted physically)                  |
| `commitInfo` | Operation, `isolationLevel`, `operationMetrics`, `userMetadata`; the source of `DESCRIBE HISTORY` |
| `txn`, `cdc` | Idempotent writes (streaming); Change Data Feed files                                             |

#### 2.2.2 Snapshots and multi-version concurrency

Replaying the log in order yields the set of active files: `Active(v) = Active(v−1) ∪ Added(v) \ Removed(v)`. Because files are immutable, every UPDATE is **copy-on-write**: affected files are read, corrected copies are written, and a commit records `remove` for the old files and `add` for the new ones. Several logical versions coexist without any file being overwritten. The cost of this design is measured directly in Section 3.4.6.

#### 2.2.3 ACID: mechanism and evidence in this project

**Table 6.** How the log provides each ACID property, with evidence from the pipeline.

| Property        | Delta mechanism                                                                                                                                                        | Evidence in this project                                                                                     |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| **Atomicity**   | All files of a transaction become visible only when its JSON commit exists; files from a failed job are orphans never referenced by any version (VACUUM reclaims them) | Each TLC file is exactly one Bronze commit (v0–v5), tagged with `userMetadata` (batch id, SHA-256)           |
| **Consistency** | Writes are validated against schema and protocol; business invariants are *not* known to Delta                                                                         | The `fare ≤ 0` rule belongs to Silver, not Delta; the new `cbd_congestion_fee` column requires `mergeSchema` |
| **Isolation**   | Snapshot isolation for readers; optimistic concurrency control for writers                                                                                             | Silver MERGE commits record `readVersion`; time travel reads v0–v6 concurrently without interference         |
| **Durability**  | Data and log reside on storage; snapshots are reconstructed from the log                                                                                               | Version 0 still returns 2,926,215 rows after every MERGE and after VACUUM on a copy                          |

ACID is **not** data quality: the log cannot know that a negative fare is wrong; that is Silver's responsibility. Two further limits apply. Transactions are scoped to a **single table**, so Silver and its quarantine table are separate commits; the pipeline therefore maintains an `ops/silver_batch_log` ledger to know which batches completed. And durability and atomicity depend on the storage layer (object stores need a suitable log-store implementation); this project runs on a local file system.

#### 2.2.4 Optimistic concurrency control

Delta takes **no locks**. A writer proceeds in three phases: (1) **read**, recording its `readVersion`; (2) **write** new Parquet files, which are invisible to readers because no commit yet references them; (3) **validate and commit** by attempting to create the next-numbered commit file. The storage layer must permit exactly one writer to create a given version. A losing writer reads the intervening commits and checks them for conflicts against what it read (`ConcurrentAppendException`, `ConcurrentDeleteReadException`, `ConcurrentDeleteDeleteException`, `MetadataChangedException`, among others). If no true conflict exists, it retries at the next version **without rewriting its data**.

*Application here.* Bronze commits are blind appends (`isBlindAppend: true`); a MERGE is not (`isBlindAppend: false`), because it reads the table to match keys. The pipeline **assumes a single writer** (Section 6): no two-writer conflict experiment has been conducted. Note that commits record `isolationLevel: "Serializable"`; we quote the logged value verbatim and do not infer conflict behavior from this field alone, which should be reconciled with Delta's documentation of the default level.

#### 2.2.5 Snapshot isolation and time travel

A reader pins a version when its query begins and reads only that version's files; readers therefore never block writers and vice versa. **Time travel is snapshot isolation with a user-chosen version.** The Silver layer exploits this operationally: each Bronze batch is read *at the version that committed it* (Section 3.4.3).

#### 2.2.6 Checkpoints and retention

Every ten commits (`delta.checkpointInterval`), Delta writes a Parquet checkpoint that aggregates the still-effective actions; readers load the latest checkpoint and replay only subsequent JSON files. Bronze and Silver each have seven commits (v0–v6), below the default interval, so no checkpoint is expected (to be confirmed by listing `_delta_log`). The time-travel horizon is bounded by `delta.logRetentionDuration` (default 30 days) and `delta.deletedFileRetentionDuration` (default 7 days): **time travel requires both the log entry and the data file** (Section 3.5).

### 2.3 Change data capture

CDC identifies and propagates changes (inserts, updates, deletes) rather than reloading entire datasets.

**Table 7.** CDC approaches.

| Approach                      | Mechanism                                   | Trade-offs                                                                          |
| ----------------------------- | ------------------------------------------- | ----------------------------------------------------------------------------------- |
| Log-based (Debezium, DMS) [6] | Reads the source database's transaction log | No extra source load; preserves commit order; captures deletes; requires log access |
| Timestamp/query-based         | Queries `updated_at` above a watermark      | Simple; misses hard deletes                                                         |
| Trigger-based                 | Triggers write to a shadow table            | Accurate; slows source transactions                                                 |
| Snapshot differencing         | Compares two full extracts                  | Simple; computationally expensive                                                   |

Three commonly conflated terms deserve distinction: *CDC* captures changes at a source; *MERGE INTO* applies changes to a target; *Change Data Feed* lets a Delta table itself emit row-level changes downstream. This project uses MERGE and does **not** enable CDF.

**Four conditions for correct CDC:** (1) a stable key; (2) event ordering; (3) deduplication before MERGE (Delta rejects a MERGE in which several source rows match one target row); (4) idempotence under replay. Section 3.4.6 reports the extent to which each is satisfied.

### 2.4 Medallion architecture

**Table 8.** Layer contracts.

|           | Bronze                             | Silver                                                          | Gold                                |
| --------- | ---------------------------------- | --------------------------------------------------------------- | ----------------------------------- |
| Contract  | Every row as received, append-only | One row per trip, typed, deduplicated, flagged                  | Tables shaped by business questions |
| Bad data  | **Preserved**                      | Hard rejections quarantined; minor issues flagged in `dq_flags` | Excluded by flag                    |
| Key       | Batch (`_batch_id`)                | `trip_key`                                                      | zone × date × hour                  |
| Consumers | Engineers, auditors                | Engineers, analysts                                             | BI, ML                              |

*Rationale.* (a) No loss of source data: Bronze permits Silver to be rebuilt when rules change, as the team did when revising `TOTAL_MISMATCH` (`--steps silver gold --rebuild`, Bronze untouched). (b) Debuggability through checkable invariants. (c) Access control by layer. *Costs.* Storage multiplies with the number of layers, latency increases, a Silver defect propagates to Gold, and the three tiers are an organizational convention rather than three storage formats.

### 2.5 Performance engineering

**The small-file problem.** Distributed engines pay a fixed cost *per file* (listing, opening, reading the Parquet footer, task scheduling) regardless of its size. Ten GB split into 2,000 files of 5 MB costs far more than the same data in 20 files of 512 MB. Small files arise from micro-batch and streaming ingestion, from each MERGE writing replacement files, from partitioning on high-cardinality columns, and from over-large `repartition(n)`. In Delta every file is also an `add` action in the log; on object stores every list or read is a billed API call.

**Compaction (`OPTIMIZE`).** Bin-packing groups small files into bins of roughly `spark.databricks.delta.optimize.maxFileSize` (default 1 GB), writes each bin as a new Parquet file, and commits `add` plus `remove` actions with `dataChange=false`. Query results are unchanged; old files remain for time travel until VACUUM. Compaction corrects file *count and size*; it does **not** co-locate related rows.

**Data skipping.** Each commit stores record counts and min/max/null-count statistics for the first **32 columns** (`delta.dataSkippingNumIndexedCols`). Because statistics live in the log, files are eliminated at planning time, **before any Parquet file is opened**. Skipping is conservative: a file is dropped only when its statistics *prove* it cannot match. Statistics are file-level bounds, not a B-tree; string values are truncated (the project's real log shows `_source_file` cut to 32 characters).

**Z-ORDER and liquid clustering.** Because data arrives in time order, each file spans almost *every* pickup zone, so min/max ranges are wide and no file can be skipped. Partitioning by zone would create \~265 small directories, recreating the small-file problem. `OPTIMIZE … ZORDER BY (cols)` orders rows along a **Z-order (Morton) space-filling curve**, interleaving the bits of the chosen columns into one key so that rows close in several columns receive close keys. Sorting by that key and cutting into files *narrows each file's [min, max] range* on all Z-ordered columns, and the statistics are rewritten. (The **Hilbert** curve belongs to *liquid clustering*, not to `ZORDER` in open-source Delta.)

Three properties govern use, and all three are verified numerically in Section 5: adding columns **dilutes locality** per column (Q1 scans 5 files with one column but 42 with two); gains accrue only to predicates **correlated with the layout** (a control query on `payment_type` gains nothing); and Z-ORDER is **not incremental**, whereas liquid clustering (`CLUSTER BY`) is incremental and cannot be combined with partitioning.

[Figure 2. Per-file min/max ranges before and after Z-ORDER.](report/figures/file_minmax_grid.png)

---

## 3. System Design and Implementation

### 3.1 Runtime environment

The system runs on Python 3.11/3.12, Java 17/21, PySpark 4.0.1, and delta-spark 4.0.1 (versions pinned in `requirements.txt`). The Spark session (`src/spark_session.py`) fixes the settings below, each with a stated reason.

**Table 9.** Pinned Spark configuration.

| Setting                                          | Rationale                                                                                                                                                                                                                                                     |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `spark.sql.ansi.enabled=true`                    | Invalid casts **raise errors** instead of silently yielding NULL; dirty data becomes NULL only through explicit `try_cast`                                                                                                                                    |
| `spark.sql.session.timeZone=UTC`                 | TLC timestamps are NYC wall-clock values stored **without a zone**; pinning UTC prevents Spark from shifting them. Consequently `pickup_hour` is NYC local time, and daylight-saving transitions (10 March 2024, 9 March 2025) appear as a missing 02:00 hour |
| `spark.sql.legacy.timeParserPolicy=CORRECTED`    | Strict datetime parsing; no lenient guessing                                                                                                                                                                                                                  |
| `shuffle.partitions=64`, `maxPartitionBytes=64m` | 20 M rows on one machine: smaller input splits keep per-task heap small and avoid out-of-memory failures                                                                                                                                                      |
| `spark.local.dir` on the data disk               | Spill and shuffle files must not land on a small or quota-limited `/tmp`                                                                                                                                                                                      |

### 3.2 Pipeline architecture and operational properties

[Figure 3. The pipeline's ten steps with their principal counts.](report/figures/pipeline_dataflow.png)

```
lakehouse_pipeline.py      runner: reference → bronze → profile → silver → cdc → evolution → time_travel → gold [→ benchmark]
scripts/download_sources.py  downloads the six TLC files and reference data; writes a SHA-256 manifest
src/tlc_bronze.py          Bronze: one commit per file, userMetadata, mergeSchema
src/tlc_profile.py         Bronze profiling used to calibrate rule thresholds
src/dq_rules.py            TLC-v2 contract (8 hard rules, 13 soft flags)
src/tlc_silver.py          Silver: per-batch MERGE, CDC, schema evolution
src/time_travel.py         history, versionAsOf, VACUUM on a copy
src/gold.py                four marts and verification against independent SQL
optimization_benchmark.py  five layouts × three queries
src/bronze.py, silver.py, generator.py   synthetic fixture lane (B1-v1.0 contract)
tests/                     57 tests

```

**Operational properties.** The pipeline is built to be rerun safely:

- **Idempotent by construction.** A rerun skips Bronze batches already ingested (by `_batch_id`, which embeds the file hash), skips Silver batches present in `ops/silver_batch_log`, does not reapply an applied CDC feed, and rebuilds Gold from the current Silver snapshot.
- **Self-checking.** After every MERGE, Delta's `operationMetrics` are compared with a plan computed beforehand; any discrepancy aborts the run.
- **Auditable.** Each run writes `docs/pipeline_run.json` (step results, timings, counts) and `docs/RESULTS.md`; each Bronze and Silver commit carries `userMetadata` linking it to a batch and file hash.
- **Resource-aware.** Driver memory defaults to half of system RAM; a `--dev-sample 1` mode runs a deterministic 1% rehearsal; a runbook documents out-of-memory and disk-spill remedies (Appendix A).

### 3.3 Bronze layer

**Principles** (`src/tlc_bronze.py`): *source-faithful* (no row is filtered); **one Delta commit per source file**, tagged with `userMetadata` (batch id, source path, SHA-256) so later steps can find which Bronze version introduced which batch; *idempotent* by `_batch_id`; `mergeSchema=true`; partitioned by `_source_month`. Bronze only **harmonizes column names and widens types** (e.g., `int → bigint`, `timestamp_ntz → timestamp`), logging each change per batch.

**Table 10.** Bronze commit history.

| Version | Batch                                                               | Rows written   |
| ------- | ------------------------------------------------------------------- | -------------- |
| v0      | yellow_2024-01                                                      | 2,964,624      |
| v1      | yellow_2024-02                                                      | 3,007,526      |
| v2      | yellow_2024-03                                                      | 3,582,628      |
| v3      | yellow_2025-01 (**adds `cbd_congestion_fee`**)                      | 3,475,226      |
| v4      | yellow_2025-02                                                      | 3,577,543      |
| v5      | yellow_2025-03                                                      | 4,145,257      |
| v6      | `cbd_corrections_seed42` → `cdc_corrections_seed42` (**synthetic**) | 30             |
|         | **Total**                                                           | **20,752,834** |

Rows v0–v5 match the manifest (20,752,804); v6 is the 30-row correction feed, whose `_source_file` begins `synthetic://` so it can never be mistaken for TLC data.

*Why ingest 2024 before 2025?* So that `cbd_congestion_fee` appears as a genuine schema-change event at commit v3 (Section 3.4.7). *Why partition by `_source_month`?* Low cardinality (six values), each batch writes to exactly one partition, and queries by source month are pruned; partitioning by zone would create hundreds of small directories (Section 2.5). *Profile before rules:* the team profiled Bronze and only then tuned thresholds (Section 3.4.4), so that the data, not intuition, decides.

### 3.4 Silver layer

#### 3.4.1 The TLC-v2 cleaning contract

Real TLC files exhibit problems that the four rules of the fixture lane do not cover, so the real-data lane applies the **TLC-v2** contract (`src/dq_rules.py`, `docs/DQ_RULES.md`).

**Table 11.** The two outcomes of classification.

| Outcome            | Meaning                                                         | Destination                                                                                                         |
| ------------------ | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| **Hard rejection** | The trip cannot be placed in time or space, or cannot be priced | `silver/yellow_trips_rejected`, exactly one `reject_reason` (first rule that fires) plus the original Bronze values |
| **Soft flag**      | A genuine trip with a questionable attribute                    | `silver/yellow_trips`, listed in `dq_flags`                                                                         |

*Why two outcomes?* Rejecting every unusual row over-cleans: a zero-passenger trip still produced revenue, and a trip with an unknown drop-off zone still occurred. Each Gold metric chooses which flags to exclude (`GOLD_EXCLUDED_FLAGS`), and the counts of everything excluded are published in `gold/dq_monthly`.

**Eight hard rules, in priority order:** (1) pickup timestamp NULL or unparseable; (2) drop-off timestamp NULL or unparseable; (3) missing pickup/drop-off location (including integer overflow, which becomes NULL via `try_cast`); (4) `fare_amount` NULL or ≤ 0 (`INVALID_FARE`, with a `reject_detail` of reversal-of-accepted-trip, unpaired negative, NULL, or zero); (5) drop-off earlier than pickup; (6) `OUT_OF_PERIOD` (pickup more than 31 days before, or 24 hours after, the file's month; years such as 2002 and 2009 occur in the data); (7) `DUPLICATE_TRIP` (same key, identical content within a batch); (8) `KEY_COLLISION` (same key, different content within a batch). Deduplication retains the row with the smallest `row_hash`, so results do not depend on row order.

**Thirteen soft flags:** `LATE_ARRIVAL`, `AFTER_SOURCE_MONTH`, `ZERO_DURATION`, `LONG_DURATION` (>4 h), `ZERO_DISTANCE`, `EXTREME_DISTANCE` (>100 mi), `IMPLAUSIBLE_SPEED` (>80 mph), `PASSENGER_COUNT_MISSING_OR_ZERO`, `UNKNOWN_RATECODE`, `UNKNOWN_ZONE` (264/265), `NEGATIVE_COMPONENT`, `TOTAL_MISMATCH`, `REVERSED`.

*Why `try_cast` rather than `cast`?* Under ANSI mode a failed `cast` of an invalid string **terminates the whole job**; `try_cast` yields NULL so a rule can catch the row and route it to quarantine. Dates are parsed with `try_to_timestamp` and the strict format `yyyy-MM-dd HH:mm:ss`. *Change relative to the fixture contract:* B1 let NULL fares pass; TLC-v2 rejects `NULL_FARE`, since such a trip cannot be priced.

#### 3.4.2 Keys: `trip_key` and `row_hash`

- `trip_key` is the SHA-256 of (VendorID, pickup, drop-off, PULocationID, DOLocationID). **Monetary columns are excluded deliberately**: were fare or tip part of the key, a correction would be classed as a *new* trip (INSERT) rather than an UPDATE.
- `row_hash` is the SHA-256 of all typed business columns. MERGE updates only when `row_hash` differs, so resubmitting identical content is a no-op rather than a file rewrite.

*Does the key hold up?* The profile reports `keys_with_conflicting_content` at an alarming 31,109–61,188 per file, but this figure almost exactly equals the count of *reversal pairs*: a trip and its negative mirror share one key. Negative rows are rejected as `INVALID_FARE` **before** deduplication, leaving only **39 genuine `KEY_COLLISION` rows and one exact duplicate across 20.7 M rows** (`docs/profile/bronze_profile.md`, §5–6). `trip_key` is therefore a sound business key for MERGE.

#### 3.4.3 Incremental processing model

For each Bronze batch not yet in `ops/silver_batch_log`, in commit order:

1. Read the batch **at the Bronze version that committed it** (time travel). A 2024 batch therefore keeps its 19-column schema even though Bronze now has 24.
2. Classify with TLC-v2: hard failures to quarantine, soft issues to `dq_flags`, in-batch duplicates to quarantine.
3. **Plan the MERGE** by joining batch keys to Silver, counting in advance the expected inserts, true updates (content changed), and no-ops (identical resubmission).
4. `MERGE INTO Silver ON (pickup_month, trip_key)`: `WHEN MATCHED AND row_hash differs → UPDATE *`; `WHEN NOT MATCHED → INSERT *`, with schema evolution enabled; then compare Delta's `operationMetrics` with the plan and abort on mismatch.
5. Record counts in the batch log. Per-batch conservation holds: `bronze_rows = rejected + inserted + updated + no-op`.

*Why join on `(pickup_month, trip_key)`?* Including the partition column enables partition pruning during MERGE. Late-arriving trips (a February file containing January pickups) are inserted into their true `pickup_month` partition.

[Figure 4. How a single batch (2025-03) splits into accepted and rejected rows.](report/figures/silver_funnel_2025_03.png)

**Table 12.** Per-batch row conservation. "Rejected" is computed as Bronze minus inserted, from `bronze_history.json` and `silver_history.json`.

| Batch              | Bronze         | Inserted into Silver    | Rejected    |
| ------------------ | -------------- | ----------------------- | ----------- |
| 2024-01            | 2,964,624      | 2,926,215               | 38,409      |
| 2024-02            | 3,007,526      | 2,966,014               | 41,512      |
| 2024-03            | 3,582,628      | 3,522,748               | 59,880      |
| 2025-01            | 3,475,226      | 3,329,582               | 145,644     |
| 2025-02            | 3,577,543      | 3,393,348               | 184,195     |
| 2025-03            | 4,145,257      | 3,934,785               | 210,472     |
| **Six TLC files**  | **20,752,804** | **20,072,692**          | **680,112** |
| CDC feed (30 rows) | 30             | 5 inserted + 25 updated | 0           |
| **Overall**        | **20,752,834** | **20,072,697**          | **680,112** |

Overall: `20,752,834 = 680,112 (rejected) + 20,072,697 (inserted) + 25 (updated) + 0 (no-op)` (`docs/ANALYSIS.md`, §1).

#### 3.4.4 Data-quality findings and evidence-driven rule revision

**The rejection rate more than tripled in 2025**, from about 1.3% to 5.1%. Of 680,112 hard rejections, 679,586 are `INVALID_FARE`, and the growth comes almost entirely from **VendorID 2 with `payment_type = 0`**: 2,066 rows in January 2024 to 140,460 in March 2025, averaging −$4.27 with NULL rate codes. The older refund pattern (payment types 2/3/4, averaging about −$20) barely moved. The share of negative rows that pair with a matching positive trip fell from roughly 80% to roughly 30%, so the increase is **not an artifact of our rules**.

**Table 13.** Rejection rates and negative fares by month.

| Month   | Reject rate | Negative-fare rows | With positive pair | Unpaired |
| ------- | ----------- | ------------------ | ------------------ | -------- |
| 2024-01 | 1.29%       | 37,448             | 30,980             | 6,468    |
| 2024-02 | 1.38%       | 40,655             | 31,547             | 9,108    |
| 2024-03 | 1.67%       | 58,464             | 38,766             | 19,698   |
| 2025-01 | 4.19%       | 144,117            | 52,231             | 91,886   |
| 2025-02 | 5.15%       | 182,655            | 48,613             | 134,042  |
| 2025-03 | 5.08%       | 208,722            | 60,768             | 147,954  |

**Two further findings** (`docs/evidence/findings.md`). (i) **VendorID 7** first appears in January 2025 and reports only trips with `dropoff = pickup`: 21,481 trips in March 2025, exactly its zero-duration count (vendors 1 and 2 report 600–900 per month, unchanged since 2024). (ii) The **"NULL block"** (rate code, passenger count, congestion surcharge, and store-and-forward flag all NULL, always `payment_type = 0`) grew from 4.7% of trips (January 2024) to 22.1% (March 2025), and alone explains the two largest soft flags, `UNKNOWN_RATECODE` (13.79%) and `PASSENGER_COUNT_MISSING_OR_ZERO` (13.57%). These rows are **retained in Silver with flags**, not deleted, so they can be quantified.

**A rule corrected by evidence.** The first full run flagged `TOTAL_MISMATCH` on **6,434,035 rows (31.0%)**, an implausible figure. Rather than loosen the threshold until the number looked acceptable, the team examined the distribution of `gap = total_amount − Σ components` (`scripts/investigate_findings.py`).

[Figure 5. Distribution of TOTAL_MISMATCH gaps: 90.7% fall on a handful of exact surcharge values.](report/figures/total_mismatch_gaps.png)

Of the gaps, 90.7% fell exactly on −2.50, −3.25, −0.75, or +2.50, the congestion surcharges, which TLC records **three different ways within the same file**:

**Table 14.** Three encodings of congestion surcharges.

| Pattern                                                                                | Gap                        | Rows         |
| -------------------------------------------------------------------------------------- | -------------------------- | ------------ |
| Itemized and **included** in `total_amount`                                            | 0.00                       | the majority |
| Itemized but **excluded** from `total_amount`                                          | −2.50 (2024), −3.25 (2025) | 4.0 M        |
| **Charged but not itemized** (`congestion_surcharge` NULL, the `payment_type 0` group) | +2.50                      | 2.2 M        |

No single expected sum can be correct for every row. The revised rule compares `total_amount` with an **interval** `[low, high]`, where `low` is the sum of components certain to be included and `high` adds the uncertain surcharges. Measured on a full rebuild, `TOTAL_MISMATCH` fell from 6,434,035 rows (31.0%) to **279,594 (1.35%)** while **every hard-rejection count remained unchanged (680,112)**. The residual flags still catch genuinely inconsistent totals (e.g., gaps of −4.25, −5.00, and +5.50 that no surcharge explains). The episode illustrates a method: profile, define a rule, and run a diagnostic when the rule's output looks implausible.

**Threshold review** (`docs/DQ_RULES.md`): `late_window_days = 31` (only 0–5 rows per month fall outside, all with absurd dates such as 2002-12-31); `lead_tolerance_hours = 24`; `max_duration_hours = 4` (median 11.6–12.5 min, p99 ≈ 60 min); `max_distance_miles = 100` (median 1.7 mi); `max_speed_mph = 80`. Physically impossible extremes are **flagged, not deleted**.

#### 3.4.5 The synthetic fixture lane

`python lakehouse_pipeline.py --source fixture` runs the Bronze and Silver steps on 3,060 JSON rows (`src/generator.py`, seed 42). Each batch contains 1,020 raw rows: 1,000 unique rows, of which 40 carry disjoint injected defects, plus 20 duplicates.

**Table 15.** Fixture-lane rejections.

| Reject reason             | Per batch | Total   |
| ------------------------- | --------- | ------- |
| `INVALID_PICKUP_DATETIME` | 10        | 30      |
| `MISSING_LOCATION`        | 20        | 60      |
| `INVALID_FARE`            | 10        | 30      |
| `DUPLICATE_TRIP`          | 20        | 60      |
| **Total rejected**        | 60        | **180** |

`Silver 2,880 + Rejected 180 = Bronze 3,060` (`docs/b3_run.json`). Defects are **injected deliberately** so that the correct answer is known; the generator also removes naturally dirty rows from the base sample so that they cannot contaminate the verified counts. *Trade-off acknowledged:* this lane validates the *rules*, not the *real quality* of TLC data, which is the real-data lane's role. *Limitation:* the runner currently executes this lane only through Silver; CDC, time travel, and Gold run on the real-data lane.

#### 3.4.6 CDC via `MERGE INTO`

**Where does CDC traffic come from?** Analysis shows that among 20.7 M real rows, **only one trip key appears in two different monthly files**: late-arriving trips exist (119 flagged `LATE_ARRIVAL`) but they are new trips, not resubmissions. The UPDATE path is therefore not exercised by the real files. The team added a **seeded synthetic correction feed** (`apply_cdc`, seed 42): 25 real Silver trips receive a tip increase of $2.00 (every second one also a fare increase of $1.50), plus 5 late trips cloned three minutes later. The feed lands in Bronze as a genuine batch (`cdc_corrections_seed42`, Bronze v6) and flows through the same Silver path. *The update logic is real; the update traffic is not.*

**Table 16.** CDC plan versus Delta's reported metrics.

| Quantity          | Planned        | Reported by Delta (`operationMetrics`) |
| ----------------- | -------------- | -------------------------------------- |
| Updates           | 25             | `numTargetRowsUpdated` = 25            |
| Inserts           | 5              | `numTargetRowsInserted` = 5            |
| Silver rows after | 20,072,692 + 5 | 20,072,697                             |

All four checks pass: `updated > 0`, `updated == n_updates`, `inserted == n_inserts`, `after == before + inserted` (`docs/evidence/cdc_merge.json`).

**Table 17.** Sample before/after values.

| `trip_key`  | fare            | tip             | total             |
| ----------- | --------------- | --------------- | ----------------- |
| `2a44ae62…` | 12.80 → 12.80   | 3.71 → **5.71** | 22.26 → **24.26** |
| `7808a2ed…` | 7.20 → **8.70** | 2.89 → **4.89** | 17.34 → **20.84** |

[Figure 6. The correction MERGE, row by row.](report/figures/cdc_before_after.png)

**Copy-on-write cost, measured.** Silver's history exposes the cost of an UPDATE-bearing MERGE against append-only MERGEs (`docs/evidence/silver_history.json`):

**Table 18.** MERGE cost comparison.

| Version | Type                                | Files added | Files removed | Rows copied   |
| ------- | ----------------------------------- | ----------- | ------------- | ------------- |
| v1–v5   | Append-only MERGE (monthly batches) | 3 each      | **0**         | **0**         |
| v6      | MERGE with UPDATE (CDC)             | 1           | 1             | **3,934,729** |

To change 25 rows and add 5, Delta rewrote the **entire file** containing them, copying about 131,000 unchanged rows per modified row (computed: 3,934,729 / 30). The `pickup_month` predicate confines the rewrite to one partition, but *within* a partition the unit of rewrite is the whole file. This is the practical argument for **deletion vectors** at scale (Section 7). Append-only MERGEs cost nothing in this respect because they touch only new partitions.

*Extent to which the four CDC conditions are met.* Stable key: yes (`trip_key`). Deduplication: yes (within batch, before MERGE). Idempotence: yes (a reapplied feed is skipped; identical content is a no-op via `row_hash`). **Event ordering: only partially** (batches are applied in order, with no sequence number). **DELETEs are not handled**, CDF is not enabled, and a **single writer** is assumed. Silver is a **Type 1 slowly changing** table (current state); history lives in Bronze and in time travel.

#### 3.4.7 Schema evolution

The two file groups differ in schema naturally: 2024 files have 19 columns, 2025 files 20 (adding `cbd_congestion_fee`, double). Because Bronze loads 2024 first, the new column appears as a genuine schema-change event.

[Figure 7. The schema change as recorded in the Delta log.](report/figures/schema_evolution_commits.png)

**Table 19.** Schema-change commits.

| Table  | Commit | Operation      | Change                                 |
| ------ | ------ | -------------- | -------------------------------------- |
| Bronze | v3     | WRITE (Append) | `+cbd_congestion_fee`                  |
| Silver | v3     | MERGE          | `+cbd_congestion_fee`, 33 → 34 columns |

**Evidence that no rebuild occurred** (`docs/evidence/schema_evolution.json`): `not_a_rebuild: true`; the log contains one `CREATE` event at v0 and one `SCHEMA_CHANGE` at v3, with no accompanying `remove`. Existing rows are not rewritten, so the new column is NULL for all 2024 rows and populated for all 2025 rows:

**Table 20.** Null check on `cbd_congestion_fee` in Silver.

| Month   | Silver rows | NULL values |
| ------- | ----------- | ----------- |
| 2024-01 | 2,926,215   | 2,926,215   |
| 2024-02 | 2,966,014   | 2,966,014   |
| 2024-03 | 3,522,748   | 3,522,748   |
| 2025-01 | 3,329,582   | 0           |
| 2025-02 | 3,393,348   | 0           |
| 2025-03 | 3,934,790   | 0           |

(2025-03 shows 3,934,790 = 3,934,785 + 5 inserts from the CDC feed.) *Why `DESCRIBE HISTORY` alone is insufficient evidence:* a schema-adding commit appears only as `WRITE` or `MERGE`; there is no operation named "schema evolution". The evidence lies in the `metaData.schemaString` action inside that commit's JSON file.

### 3.5 Time travel, audit, and VACUUM

[Figure 8. Silver versions and the outcome of VACUUM on a copy.](report/figures/version_timeline.png)

Silver has seven versions. Row counts per version were obtained by actually reading each one with `versionAsOf` (`docs/evidence/time_travel.json`).

**Table 21.** Silver versions.

| Version | Operation             | Rows (cumulative) | Columns |
| ------- | --------------------- | ----------------- | ------- |
| v0      | WRITE (2024-01 batch) | 2,926,215         | 33      |
| v1      | MERGE (2024-02)       | 5,892,229         | 33      |
| v2      | MERGE (2024-03)       | 9,414,977         | 33      |
| v3      | MERGE (2025-01)       | 12,744,559        | **34**  |
| v4      | MERGE (2025-02)       | 16,137,907        | 34      |
| v5      | MERGE (2025-03)       | 20,072,692        | 34      |
| v6      | MERGE (CDC)           | 20,072,697        | 34      |

Comparing v5 with v6 yields `rows_changed_or_added = 30` (25 corrected, 5 added) and `rows_added = 5`; the **prior** values of every corrected trip can be read with `versionAsOf 5`.

**VACUUM: where time travel ends.** The production table is **never** vacuumed. The runner copies the table to a scratch directory and operates *only on the copy*: it temporarily disables `retentionDurationCheck` (a **demonstration-only measure**, labelled as such and restored immediately) and runs `VACUUM … RETAIN 0 HOURS`.

**Table 22.** VACUUM on a copy (`vacuum_demo.json`).

| Observation                          | Result                                                            |
| ------------------------------------ | ----------------------------------------------------------------- |
| Files listed for deletion by DRY RUN | **1**                                                             |
| Copy, current snapshot               | 20,072,697 rows, readable                                         |
| Copy, versions 0–4 and 6             | **Readable**                                                      |
| Copy, version **5**                  | **Read fails** (`Py4JJavaError`; the data file of v5 was deleted) |
| Original table, version 0            | 2,926,215 rows, intact                                            |

*Why does only v5 fail?* VACUUM deletes files that the latest version no longer references and that were `remove`d beyond the retention period. The append-only MERGEs (v1–v5) removed no files; only MERGE v6 removed one file, belonging to partition 2025-03, which v5 still referenced. **VACUUM does not break "all older versions"; it breaks exactly those versions that point to files a later commit has `remove`d.** After VACUUM, `DESCRIBE HISTORY` still lists v5, but reading it fails: *time travel needs both the log and the data*. This is also why the retention check defaults to seven days.

**Not demonstrated:** `RESTORE TABLE … TO VERSION AS OF` (which would create a *new* version rather than erase history, and would fail on the vacuumed copy for the same reason) and `timestampAsOf`.

### 3.6 Gold layer

`src/gold.py` builds four marts from Silver with one command.

**Table 23.** Gold marts.

| Table                 | Content                                                                                     |
| --------------------- | ------------------------------------------------------------------------------------------- |
| `zone_hourly_metrics` | Pickup zone × date × hour: trips, fares, tip percentage, driver earnings (**553,881 rows**) |
| `cbd_flow_monthly`    | Trips by CBD flow (into / out of / within / non-CBD), split at the policy start date        |
| `cbd_flow_yoy`        | 2025 versus 2024 per calendar day, per flow, and relative to non-CBD trips                  |
| `dq_monthly`          | Rejection and flag rates per source month (data quality as a product)                       |

#### 3.6.1 Metric definitions and justification

1. **`avg_tip_pct = AVG(tip_amount / fare_amount)` per trip, only where `fare_amount > 0`**, rather than `SUM(tip)/SUM(fare)`. The two answer different questions ("what does a typical passenger tip" versus "what share of total fares is tipped"). The denominator guard prevents infinities. The metric is **reported twice: over all trips and over card-paid trips**, because TLC records tips only for card payments; cash tips are absent from the data, so the all-trip average is biased downward by cash trips with zero recorded tip.
2. **`driver_earnings = fare + tip + extra`** (`extra` NULL → 0). Tolls, MTA tax, improvement surcharge, congestion surcharge, airport fee, and CBD fee are pass-through charges. The data has no driver identifier, so this is gross earnings per zone-hour.
3. **Exclusions.** Rows flagged `REVERSED`, `IMPLAUSIBLE_SPEED`, or `EXTREME_DISTANCE` remain in Silver but are excluded from Gold. Gold holds **19,808,248** trips out of 20,072,697 in Silver (about 1.3% excluded, computed).
4. **Time.** `pickup_hour` is NYC local time. The grain includes the date; with 20 M trips the sample is dense enough that an hour-of-day profile can also be derived through trip-weighted aggregation.

#### 3.6.2 Verification

Gold is reconciled against an **independent SQL query on Silver** (`docs/evidence/gold_verification.json`): `all_match: true`. Global totals agree: `gold_trips = manual_trips = 19,808,248` and `gold_earnings = manual_earnings = 461,016,275.03`. Three sampled cells match on every metric (e.g., zone 79, 2025-02-02, hour 1: 991 trips, mean fare 15.1256, tip 15.9631%, earnings 18,048.24 on both sides).

#### 3.6.3 The congestion-pricing question and validation of the CBD list

The CBD list contains 33 official zones and 5 inferred zones (233, 234, 246, 249, 261), since data.ny.gov returned 403. Rather than trust it, Gold **tests the list against the data**: every trip *starting* in a CBD zone after 5 January 2025 owes the fee, so CBD zones should show a fee share near 100% (`docs/evidence/cbd_zone_validation.md`).

**Table 24.** CBD list validation.

| Verdict                                                          | Zones |
| ---------------------------------------------------------------- | ----- |
| `CONFIRMED_BY_FEES` (listed; 94.8–99.7% of pickups paid the fee) | 38    |
| `CONSISTENT_NON_CBD` (unlisted; no fee)                          | 187   |
| `TOO_FEW_TRIPS`                                                  | 36    |

No zone was marked `LISTED_BUT_LOW_SHARE` or `NOT_LISTED_HIGH_SHARE`. All **five inferred zones** returned 97.2–99.6%, so the inference was tested rather than assumed.

**Results.** Trips per calendar day, 2025 versus 2024, and relative to non-CBD trips (`gold/cbd_flow_yoy`):

**Table 25.** Year-over-year change in trips per day (percentage-point difference relative to non-CBD in parentheses).

| Month    | INTO_CBD         | OUT_OF_CBD       | WITHIN_CBD       | NON_CBD (baseline) |
| -------- | ---------------- | ---------------- | ---------------- | ------------------ |
| January  | +8.4% (−4.5 pp)  | +6.8% (−6.1 pp)  | +18.4% (+5.6 pp) | +12.9%             |
| February | +13.7% (−9.6 pp) | +8.3% (−15.1 pp) | +20.8% (−2.5 pp) | +23.3%             |
| March    | +6.6% (−13.6 pp) | +4.8% (−15.4 pp) | +10.7% (−9.5 pp) | +20.2%             |

Trips **crossing the zone boundary** grew less than trips entirely outside it in all three months and in both directions; trips wholly within the zone track the citywide trend more closely. **These results are descriptive, not causal.** Q1 2025 differs from Q1 2024 in weather, events, and ride-hail competition; normalizing by non-CBD trips removes citywide trends but not CBD-specific ones; fees also apply to trips passing *through* the zone, so some `NON_CBD` trips legitimately carry a fee; and Gold excludes flagged rows, a slightly larger share in 2025, which marginally *understates* 2025 growth.

---

## 4. Transaction-Log Analysis

### 4.1 Commit inventory

**Table 26.** Observable signatures in the log.

| Table  | Version | Operation           | Signature in the log                                       |
| ------ | ------- | ------------------- | ---------------------------------------------------------- |
| Bronze | v0–v5   | WRITE (Append)      | One commit per TLC file; `add` only; `isBlindAppend: true` |
| Bronze | v3      | WRITE (Append)      | Additional `metaData`: schema changed                      |
| Bronze | v6      | WRITE (Append)      | Correction feed, 30 rows, one `add`                        |
| Silver | v0      | WRITE               | 28 files, 2,926,215 rows                                   |
| Silver | v1–v5   | MERGE (append-only) | 3 files `add`ed, **0 `remove`**, 0 rows copied             |
| Silver | v3      | MERGE               | Additional `metaData`: `+cbd_congestion_fee`               |
| Silver | v6      | MERGE (with UPDATE) | 1 `add`, **1 `remove`**, 3,934,729 rows copied             |

The table functions as a map: **`remove` implies copy-on-write**; **`metaData` in a non-initial commit implies a schema change**; **`add` only implies a pure append**.

### 4.2 Annotated excerpt: a real Bronze commit

Commit `…0006.json` of Bronze (`docs/evidence/delta_log_annotated.md`):

```json
{"commitInfo":{"operation":"WRITE","operationParameters":{"mode":"Append","partitionBy":"[\"_source_month\"]"},
  "readVersion":5,"isolationLevel":"Serializable","isBlindAppend":true,
  "operationMetrics":{"numFiles":"1","numOutputRows":"30","numOutputBytes":"10211"},
  "userMetadata":"{\"batch_id\":\"cdc_corrections_seed42\",\"layer\":\"bronze\",\"sha256\":\"4025a0aa…\",
                   \"source_file\":\"synthetic://cdc_corrections_seed42.parquet\"}", …}}
{"add":{"path":"_source_month=2025-03/part-00000-….snappy.parquet","partitionValues":{"_source_month":"2025-03"},
  "size":10211,"dataChange":true,"stats":"{\"numRecords\":30,\"minValues\":{…\"PULocationID\":24…},
                                           \"maxValues\":{…\"PULocationID\":263…},\"nullCount\":{…}}"}}

```

*Reading the excerpt.* `readVersion: 5` with `isBlindAppend: true` denotes an append that did not read the table, and therefore cannot conflict with a MERGE under OCC. `userMetadata` is the bridge by which Silver discovers which Bronze version committed which batch. `partitionValues` places the file in partition `2025-03`. The `stats` show `PULocationID` ∈ [24, 263]: only 30 rows, yet nearly the entire zone range, so *such a file is never skipped when filtering by zone*. This is direct evidence of the problem Z-ORDER addresses (Section 5). Two minor details: string statistics are truncated at 32 characters (the `_source_file` bound ends with `\u007f` to remain a valid upper bound), and `isolationLevel` is logged as `Serializable` (see the note in Section 2.2.4).

### 4.3 Checkpoints

Each table has seven commits, below the default checkpoint interval of ten, so none is expected (to be confirmed by listing `_delta_log`); readers replay all JSON files from v0. A checkpoint demonstration can be produced cheaply with a scratch table at `delta.checkpointInterval=3` and eight commits.

---

## 5. Storage-Layout Evaluation

### 5.1 Experimental setup

**Subject.** A snapshot of Silver (**20,072,697 rows, 3.1 GB**), unpartitioned so every layout holds identical rows.

**Layouts.** Five layouts are built from the same Silver snapshot. Query results were verified **identical** across all layouts (`results_identical_across_variants = true` for all three queries).

**Table 27.** Layouts under test.

| Layout         | Construction                                                | Files before → after | Size (MB) | MB/file |
| -------------- | ----------------------------------------------------------- | -------------------- | --------- | ------- |
| baseline       | `repartition(200)` of Silver (random order)                 | 200 → 200            | 3,184.3   | 15.92   |
| compacted      | baseline + `OPTIMIZE`                                       | 200 → 100            | 3,151.6   | 31.52   |
| zorder_1col    | baseline + `ZORDER BY (PULocationID)`                       | 200 → 99             | 3,135.4   | 31.67   |
| zorder_2col    | baseline + `ZORDER BY (PULocationID, tpep_pickup_datetime)` | 200 → 99             | 3,115.9   | 31.47   |
| liquid_cluster | `CLUSTER BY (PULocationID, pickup)` + `OPTIMIZE FULL`       | 67 → 98              | 3,111.7   | 31.75   |

*Why a 200-file random baseline?* It reproduces the small-file state that streaming ingestion or repeated MERGEs leave behind, and ensures every file's min/max range is wide (no locality). *Why a 32 MB target?* OPTIMIZE's default 1 GB target would leave a 3.1 GB table with about three files, nothing to skip, which is precisely what happened at fixture scale. The target was therefore scaled to the data (`--target-file-mb`), and **results depend on this choice**.

[Figure 9. Files scanned per layout.](report/figures/benchmark_files_scanned.png)

**Queries.**

- **Q1 (one dimension):** `PULocationID = 132` (JFK Airport).
- **Q2 (two dimensions):** `PULocationID = 161` (Midtown Center) within the week 10–16 February 2025.
- **Q3 (control):** `payment_type = 2`, a column that is **not** clustered. If clustering appears to help here, caching should be suspected.

**Protocol.** One discarded warm-up round plus five measured rounds. Within each round the layouts run **interleaved** in a fixed order rather than all runs of one layout consecutively. Each run calls `spark.catalog.clearCache()`, builds a fresh DataFrame (fresh snapshot resolution), and `collect()`s a tiny aggregate so result transfer is negligible. **Per-run metrics** are wall time, files scanned, and bytes read, **taken from the scan node's SQL metrics in the executed physical plan** (that is, *after* data skipping), not estimated from the log. Medians are reported; improvement is `(baseline − variant) / baseline`.

### 5.2 Results

Source: `docs/benchmark/benchmark_results.md`, five measured rounds.

**Table 28.** Q1: `PULocationID = 132`.

| Layout          | Median ms (min–max)     | Files scanned | MB read   | Time reduction |
| --------------- | ----------------------- | ------------- | --------- | -------------- |
| baseline        | 172.5 (165.3–195.6)     | 200           | 3,184.3   | –              |
| compacted       | 167.7 (161.3–177.6)     | 100           | 3,151.6   | 2.8%           |
| **zorder_1col** | **108.0** (105.9–118.4) | **5**         | **164.4** | **37.4%**      |
| zorder_2col     | 128.8 (125.1–133.0)     | 42            | 1,317.6   | 25.3%          |
| liquid_cluster  | 125.2 (120.2–128.7)     | 35            | 1,116.5   | 27.4%          |

**Table 29.** Q2: zone 161, one week.

| Layout             | Median ms (min–max)     | Files scanned | MB read  | Time reduction |
| ------------------ | ----------------------- | ------------- | -------- | -------------- |
| baseline           | 275.1 (263.7–285.5)     | 200           | 3,184.3  | –              |
| compacted          | 267.8 (265.2–277.7)     | 100           | 3,151.6  | 2.7%           |
| zorder_1col        | 119.1 (115.5–128.9)     | 5             | 165.7    | 56.7%          |
| zorder_2col        | 127.1 (115.9–131.4)     | 6             | 188.7    | 53.8%          |
| **liquid_cluster** | **117.7** (112.9–124.5) | **3**         | **93.6** | **57.2%**      |

**Table 30.** Q3 (control): `payment_type = 2`.

| Layout         | Median ms (min–max) | Files scanned | MB read | Time reduction |
| -------------- | ------------------- | ------------- | ------- | -------------- |
| baseline       | 185.2 (163.1–195.4) | 200           | 3,184.3 | –              |
| compacted      | 167.5 (162.5–186.2) | 100           | 3,151.6 | 9.6%           |
| zorder_1col    | 166.1 (154.6–178.5) | 99            | 3,135.4 | 10.3%          |
| zorder_2col    | 159.1 (152.7–174.8) | 99            | 3,115.9 | 14.1%          |
| liquid_cluster | 166.5 (158.2–178.2) | 98            | 3,111.7 | 10.1%          |

### 5.3 Analysis

[Figure 10. The scan node before and after Z-ORDER for Q1.](report/figures/spark_ui_scan_q1.png)

1. **The strongest evidence is I/O.** For Q1, single-column Z-ORDER scans **5 of 200 files (−97.5%)** and reads **164 MB instead of 3,184 MB (−94.8%)**; for Q2, liquid clustering scans **3 files and 94 MB (−97.1%)**. This is data skipping in operation: the log's min/max statistics narrow once data is clustered (Figure 2).
2. **Compaction alone does not create skipping.** `compacted` halves the file count (200 → 100) yet still scans all 100 files for Q1 and Q2; its 2.7–2.8% time gain is within noise. Compaction corrects file *count*, not row *placement*; placement is the job of Z-ORDER and clustering.
3. **Reading the control correctly.** Q3 shows "improvements" of 10–14%, but **files scanned remain 98–100, effectively the whole table: nothing is skipped**, and the baseline's minimum (163 ms) is below the compacted median (168 ms). The difference is run-to-run noise plus a small benefit from reading 100 larger files rather than 200 smaller ones. The correct conclusion is that clustering **does not** help a query that does not filter on the clustering columns, which is exactly what a control exists to establish.
4. **Dimensionality is a genuine trade-off.** On Q1, single-column Z-ORDER scans 5 files, whereas **two columns scan 42**, because interleaving a second column spreads each zone's rows across more files. On the two-dimensional Q2 the order reverses: liquid clustering (3 files) beats single-column Z-ORDER (5). One must optimize for the query shapes actually run, not "more columns is better".
5. **Why time improves less than I/O.** Q1 reads 94.8% less data but runs only 37% faster: at 100–300 ms, planning, task scheduling, and JVM overhead dominate, and local NVMe plus the OS page cache make re-reads cheap. On object storage, where each file costs a network round trip, the same file reduction would yield a much larger time gain. **Files scanned and bytes read are therefore the primary evidence; wall-clock time is secondary.**
6. **Storage size.** Clustered layouts are 1.5–2.3% smaller than the baseline (3,111–3,135 MB versus 3,184 MB). Sorted data tends to compress better because similar values sit together; this is a plausible inference that was not tested separately.

### 5.4 Threats to validity

- **OS page cache.** `clearCache()` clears Spark's caches, not the operating system's. After warm-up all layouts are equally warm, so the comparison is fair, but absolute times are optimistic relative to cold object storage.
- **Single machine, local disk.** Object stores add per-file latency, which would make the small-file penalty *larger* than measured.
- **Target file size** determines the outcome (Section 5.1).
- **Query selection.** Q1 and Q2 filter on the clustering columns by design; Q3 is the control. Workloads filtering on other columns will not see these gains.
- **Snapshot caching.** Delta caches the log snapshot per JVM, so planning time after the first run is lower for all layouts alike.
- **Variance.** Medians over five interleaved rounds are reported with min/max; differences of a few percent are within noise.
- **Not yet done.** A **plain-Parquet** variant (no log) to separate the effect of the Delta format from that of layout; the baseline runs first in every round (a small cold-start bias); and the 200-file random baseline is a *mild* form of the small-file problem (real streaming ingestion leaves thousands of files under 1 MB, where the gap would be wider).

---

## 6. Challenges, Limitations, and Lessons Learned

**Table 31.** Problems encountered and how they were resolved.

| Problem                                     | Cause                                                                                                                   | Resolution                                                                                                  |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `TOTAL_MISMATCH` flagged 31% of rows        | TLC records congestion surcharges three ways                                                                            | Analyzed the gap distribution; compared against an interval; 1.35% remain                                   |
| Apparent tens of thousands of key conflicts | A trip and its negative mirror share a key                                                                              | Reject negative mirrors before deduplication; 39 true collisions remain                                     |
| Rejection rate rose from 1.3% to 5.1%       | VendorID 2 `payment_type 0` (negative fares); VendorID 7 (zero duration)                                                | Kept and reported as findings; rules were not bent to make the number look better                           |
| data.ny.gov returned 403                    | Network blocking                                                                                                        | 33 official + 5 inferred zones, validated against fees actually charged                                     |
| Out-of-memory and disk-spill failures       | 20 M rows on one machine                                                                                                | Driver memory scaled to RAM, `maxPartitionBytes=64m`, dedicated spill directory, `--dev-sample 1` rehearsal |
| Delta JARs required on first run            | Maven Central download                                                                                                  | Cached under `~/.ivy2` after the first run                                                                  |
| Full-run results overwritten                | `docs/RESULTS.md` and `pipeline_run.json` are rewritten on each run; a benchmark-only run left them describing one step | Use a single `--with-benchmark` run; consider per-run output files (Section 7)                              |

**Remaining limitations (stated plainly).**

1. **The UPDATE path of CDC is not exercised by real data**: only one key appears in two monthly files. Updates come from the seeded synthetic feed (25 + 5 rows); the logic is real, the traffic is not.
2. **CDC assumes a single writer, handles no DELETEs, and has no event ordering** beyond batch order. No two-writer conflict experiment demonstrates OCC.
3. **ACID is per table.** Silver and quarantine are separate commits; `ops/silver_batch_log` is a recovery mechanism, not a multi-table transaction.
4. **The congestion-pricing analysis is descriptive, not causal** (Section 3.6.3); reconciliation with TLC's published aggregates is `SKIPPED`.
5. **Benchmark scope.** No plain-Parquet variant; no object-storage measurement; results depend on target file size; single machine.
6. **The fixture lane runs only through Silver** in the current runner.
7. **Not demonstrated:** `RESTORE`, `timestampAsOf`, checkpoints, deletion vectors.
8. **Two synthetic components** (fixture, correction feed) are clearly labelled; all other data is real TLC data.

---

## 7. Conclusion and Future Work

The three architectures differ in *table semantics* rather than storage location: a lakehouse places a transactional layer over open files, and in Delta that layer is the `_delta_log/`: atomic commits that add and remove immutable Parquet files, snapshots for readers, optimistic concurrency for writers. Applied to real taxi data, the architecture does more than "run the commands":

- **Data quality becomes measurable.** The equality `20,752,834 = 680,112 + 20,072,697 + 25` is verified per batch. A faulty rule (31% flagged) was found through diagnosis and corrected with evidence (1.35%) without altering any hard rejection.
- **The log explains what happened.** Schema changed without a rebuild (`CREATE` at v0, one `SCHEMA_CHANGE` at v3, no `remove`); append-only MERGEs copied zero rows whereas a MERGE with UPDATE copied 3.9 M; VACUUM broke exactly the version that pointed to a removed file.
- **Layout optimization is evidenced at the I/O level.** From 200 files to 5 and from 3,184 MB to 164 MB for a zone query; the control query gained nothing, as theory predicts; adding a second Z-ORDER column diluted locality (5 → 42 files).
- **A negative result was kept and used.** At 100 KB the benchmark proves nothing (every layout collapses to one file), so the work moved to real data rather than presenting latency noise as an achievement.

**Future work.** (1) Add a plain-Parquet baseline and repeat the benchmark on object storage. (2) Enable **deletion vectors** to reduce the copy-on-write cost of MERGE (3,934,729 rows copied for 30 changed rows). (3) Add sequence-aware CDC with DELETE handling. (4) Demonstrate OCC with two writers, checkpoints, and `RESTORE`. (5) Enable Change Data Feed so Gold can recompute only what changed. (6) Extend the fixture lane through CDC and Gold so rule validation spans the full pipeline. (7) Write run artifacts to per-run directories to eliminate the overwrite hazard noted in Table 31.

---

## 8. References

*(All citations must be verified before submission.)*

[1] M. Armbrust, A. Ghodsi, R. Xin, and M. Zaharia, "Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics," in *Proc. CIDR*, 2021.

[2] M. Armbrust *et al.*, "Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores," *Proc. VLDB Endow.*, vol. 13, no. 12, 2020.

[3] Delta Lake, "Delta Transaction Log Protocol." https\://github.com/delta-io/delta/blob/master/PROTOCOL.md

[4] Delta Lake documentation: Concurrency Control; Optimizations (Compaction, Data Skipping, Z-Ordering); Table Utility Commands (VACUUM, DESCRIBE HISTORY, RESTORE); Change Data Feed; Liquid Clustering. https\://docs.delta.io

[5] Databricks, "What is the medallion lakehouse architecture?"

[6] Debezium documentation. https\://debezium.io/documentation/reference/stable/

[7] Apache Spark, "ANSI Compliance." https\://spark.apache.org/docs/4.0.0/sql-ref-ansi-compliance.html

[8] NYC Taxi & Limousine Commission, "TLC Trip Record Data." https\://www\.nyc.gov/site/tlc/about/tlc-trip-record-data.page

[9] Metropolitan Transportation Authority / data.ny.gov, congestion relief zone taxi-zone dataset (`yfdc-w5jh`).

---
