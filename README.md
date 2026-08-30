# SUBJECT: DELTA LAKEHOUSE ARCHITECTURE &amp; STORAGE OPTIMIZATION

A Delta Lakehouse pipeline built with Apache Spark and Delta Lake using the NYC Yellow Taxi Trip Records dataset.

The project demonstrates the **Medallion Architecture (Bronze → Silver → Gold)** together with Delta Lake capabilities such as **ACID transactions, CDC/MERGE, Schema Evolution, Time Travel**, and performance optimization using **OPTIMIZE and Z-ORDER**.

---

## 1. Project Overview

This project implements a small-scale data lakehouse for NYC Yellow Taxi trip data.

The pipeline transforms raw monthly Parquet files into progressively refined Delta tables:

```text
NYC Yellow Taxi Dataset
          │
          ▼
        RAW
          │
          ▼
       BRONZE
   Raw Delta Table
          │
          ▼
       SILVER
 Cleaning + Validation
 Deduplication + CDC
 Schema Evolution
          │
          ▼
        GOLD
 Business Aggregations
          │
          ▼
 OPTIMIZE + Z-ORDER
          │
          ▼
      Benchmark
```

The main objectives are:

* Build a Bronze/Silver/Gold lakehouse pipeline.
* Demonstrate data cleaning and validation.
* Demonstrate CDC using Delta `MERGE`.
* Demonstrate schema evolution.
* Demonstrate Delta Lake Time Travel and transaction history.
* Build an analytical Gold layer.
* Optimize query performance using `OPTIMIZE` and `Z-ORDER`.
* Compare query performance before and after optimization.

---

## 2. Dataset

This project uses the **NYC Yellow Taxi Trip Records** dataset published by the NYC Taxi & Limousine Commission (TLC).

The original dataset is provided as monthly Parquet files. We do **not** use the entire historical dataset from 2009–2026 because that is unnecessary for demonstrating the required lakehouse concepts.

Instead, the project uses a limited number of monthly files to provide enough data for:

* Spark processing
* Delta Lake operations
* aggregation
* small-file optimization
* query benchmarking

### Selected Data

Example:

```text
2024-10
2024-11
2024-12
2025-01
2025-02
2025-03
```

The raw dataset is kept unchanged in the `data/raw/` directory.

---

## 3. Project Structure

```text
project/
│
├── data/
│   │
│   ├── raw/
│   │   └── yellow_taxi/
│   │
│   ├── cdc/
│   │   ├── late_updates.parquet
│   │   └── schema_evolution.parquet
│   │
│   ├── bronze/
│   │   └── taxi_trips/
│   │
│   ├── silver/
│   │   └── taxi_trips/
│   │
│   └── gold/
│       └── taxi_hourly_metrics/
│
├── src/
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   └── pipeline.py
│
├── optimization_benchmark.py
├── REPORT.md
├── presentation.pptx
└── README.md
```

---

## 4. Architecture

### Bronze Layer

Bronze stores the raw data ingested from the source dataset.

```text
Raw Parquet
     │
     ▼
Bronze Delta Table
```

Characteristics:

* Raw data is preserved.
* Data is stored in Delta format.
* The Bronze layer is append-oriented.
* Minimal transformation is performed.

The purpose of Bronze is to maintain a reliable historical landing layer before applying business transformations.

---

### Silver Layer

Silver contains cleaned and validated trip records.

```text
Bronze
   │
   ├── Validation
   ├── Data Cleaning
   ├── Deduplication
   └── CDC / MERGE
          │
          ▼
       Silver
```

The Silver layer handles:

* invalid fares
* missing locations
* malformed timestamps
* duplicate records
* late updates
* schema changes

Example validation rules:

```text
fare_amount > 0
PULocationID IS NOT NULL
DOLocationID IS NOT NULL
pickup timestamp IS NOT NULL
```

---

### Gold Layer

Gold contains business-oriented aggregated data.

The main aggregation is performed by:

```text
PULocationID
+
Pickup Hour
```

Example metrics:

* Average trip fare
* Average tip percentage
* Driver earnings

Conceptually:

```text
Silver
   │
   ▼
Group by PULocationID + Hour
   │
   ├── AVG(fare_amount)
   ├── AVG(tip_percentage)
   └── SUM(driver_earnings)
   │
   ▼
Gold Delta Table
```

The Gold layer is intended for analytical queries and downstream reporting.

---

## 5. CDC and Delta MERGE

The NYC Taxi dataset is historical data rather than a native CDC stream.

Therefore, a small separate CDC dataset is created to simulate incoming updates.

```text
data/cdc/
├── late_updates.parquet
└── schema_evolution.parquet
```

### Example

Original Silver record:

```text
trip_id = 1001
fare_amount = 15
tip_amount = 2
```

Incoming update:

```text
trip_id = 1001
fare_amount = 17
tip_amount = 4
```

The Delta `MERGE` operation performs:

```text
IF trip_id exists
        → UPDATE

IF trip_id does not exist
        → INSERT
```

This demonstrates an incremental data ingestion workflow.

---

## 6. Schema Evolution

The project also demonstrates schema evolution by introducing a new field:

```text
surcharge_fee
```

Initial schema:

```text
trip_id
fare_amount
tip_amount
```

New schema:

```text
trip_id
fare_amount
tip_amount
surcharge_fee
```

Delta Lake schema evolution allows the Silver table to accommodate the new column without rebuilding the entire table.

---

## 7. Delta Lake Time Travel

Delta Lake maintains table history through its transaction log.

The project demonstrates Time Travel using:

```python
.option("versionAsOf", version)
```

For example:

```text
Version 0
    │
    ├── Initial Silver data
    │
Version 1
    │
    ├── CDC / MERGE
    │
Version 2
    │
    └── Schema Evolution
```

A historical version can be queried to compare the previous state of the table with the current state.

The project also examines:

```text
_delta_log/
```

and:

```sql
DESCRIBE HISTORY delta.`<silver_path>`;
```

This demonstrates Delta Lake transaction history and auditability.

---

## 8. Performance Optimization

The Gold table is optimized for queries filtering by:

```text
PULocationID
```

### OPTIMIZE

`OPTIMIZE` is used to compact small files into fewer larger files.

Conceptually:

```text
Many Small Files
       │
       ▼
   OPTIMIZE
       │
       ▼
Fewer Larger Files
```

This helps reduce file-management overhead during queries.

### Z-ORDER

The project then applies:

```sql
OPTIMIZE delta.`<gold_path>`
ZORDER BY (PULocationID);
```

Z-ORDER organizes related data to improve data skipping for queries that filter on `PULocationID`.

---

## 9. Benchmark

Query performance is measured before and after optimization.

Example query:

```text
Filter Gold data by PULocationID
```

The same query is executed multiple times in both conditions.

```text
BEFORE OPTIMIZATION
        │
        ▼
   Run query N times
        │
        ▼
 Average execution time
        │
        ▼
 OPTIMIZE + Z-ORDER
        │
        ▼
AFTER OPTIMIZATION
        │
        ▼
   Run same query N times
        │
        ▼
 Average execution time
```

Performance improvement is calculated as:

```text
Improvement (%)
=
(Before - After)
/
Before
× 100
```

The benchmark records:

* query
* number of runs
* execution time for each run
* average execution time
* improvement percentage

---

## 10. Technologies

* **Python**
* **Apache Spark / PySpark**
* **Delta Lake**
* **Parquet**
* **SQL**
* **Git**

---

## 11. Running the Project

### Step 1 — Install dependencies

```bash
pip install pyspark delta-spark
```

### Step 2 — Place the raw dataset

Place the downloaded NYC Yellow Taxi Parquet files in:

```text
data/raw/yellow_taxi/
```

### Step 3 — Run the pipeline

```bash
python src/pipeline.py
```

The pipeline creates:

```text
data/bronze/
data/silver/
data/gold/
```

### Step 4 — Run the benchmark

```bash
python optimization_benchmark.py
```

---

## 12. Expected Pipeline Output

After successful execution:

```text
RAW
 │
 └── Monthly NYC Taxi Parquet files
          │
          ▼
BRONZE
 │
 └── Raw Delta table
          │
          ▼
SILVER
 │
 ├── Cleaned records
 ├── Deduplicated records
 ├── CDC updates
 └── Evolved schema
          │
          ▼
GOLD
 │
 └── Hourly location-level metrics
          │
          ▼
OPTIMIZATION
 │
 ├── OPTIMIZE
 └── Z-ORDER(PULocationID)
          │
          ▼
BENCHMARK
 │
 └── Before vs After performance
```

---

## 13. Demonstration Checklist

The final demonstration should cover:

* [ ] Raw NYC Taxi data
* [ ] Bronze Delta table
* [ ] Silver cleaning and validation
* [ ] Duplicate handling
* [ ] CDC using `MERGE`
* [ ] Matched record → UPDATE
* [ ] Unmatched record → INSERT
* [ ] Schema evolution with `surcharge_fee`
* [ ] Delta `_delta_log`
* [ ] `DESCRIBE HISTORY`
* [ ] Time Travel using `versionAsOf`
* [ ] Gold aggregation
* [ ] `OPTIMIZE`
* [ ] `ZORDER BY (PULocationID)`
* [ ] Benchmark before optimization
* [ ] Benchmark after optimization

---

## 14. Learning Objectives

By completing this project, we demonstrate an understanding of:

1. Data Lakehouse architecture
2. Medallion Architecture
3. Delta Lake transaction logs
4. ACID transactions
5. CDC and `MERGE`
6. Schema Evolution
7. Time Travel
8. Small File Problem
9. `OPTIMIZE`
10. Z-ORDER and data skipping
11. Spark-based data processing
12. Lakehouse performance benchmarking

---

## 15. References

* NYC Taxi & Limousine Commission — Trip Record Data
* Apache Spark Documentation
* Delta Lake Documentation
