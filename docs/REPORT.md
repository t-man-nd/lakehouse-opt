# Delta Lakehouse Architecture & Storage Optimization

## 1. Introduction
### 1.1. Context & Motivation
### 1.2. Objectives
### 1.3. Dataset
### 1.4. Overall Architecture

## 2. Part A: Theoretical & Architectural Foundations
### 2.1. Data Warehouse vs. Data Lake vs. Data Lakehouse
#### 2.1.1. Comparison
#### 2.1.2. Key Features of Delta Lake
### 2.2. Delta Transaction Log (`_delta_log/`)
#### 2.2.1. Structure
#### 2.2.2. Action Types
#### 2.2.3. How the Log Guarantees ACID
#### 2.2.4. Optimistic Concurrency Control
#### 2.2.5. Snapshot Isolation
### 2.3. Change Data Capture (CDC)
#### 2.3.1. Concept
#### 2.3.2. CDC Approaches
#### 2.3.3. CDC in the Delta Lakehouse
### 2.4. Medallion Architecture
#### 2.4.1. Bronze (Raw Zone)
#### 2.4.2. Silver (Cleansed/Enriched Zone)
#### 2.4.3. Gold (Curated Business Zone)
### 2.5. Delta Lake Performance Engineering
#### 2.5.1. Small File Problem
#### 2.5.2. Compaction (OPTIMIZE)
#### 2.5.3. Z-Ordering & Data Skipping

## 3. Part B: Design & Implementation
### 3.1. Environment & Dataset Preparation
### 3.2. Pipeline Design & Repository Structure
### 3.3. Task 1: Bronze Layer Ingestion
### 3.4. Task 2: Silver Layer (Cleansing, MERGE INTO, Schema Evolution)
#### 3.4.1. Data Cleansing Rules
#### 3.4.2. Generating trip_id
#### 3.4.3. MERGE INTO (CDC Upsert)
#### 3.4.4. Schema Evolution (mergeSchema)
### 3.5. Task 3: Time Travel & Audit
#### 3.5.1. Table History
#### 3.5.2. Querying Past Versions
#### 3.5.3. Restore
### 3.6. Task 4: Gold Layer Aggregations

## 4. Transaction Log Analysis
### 4.1. Log Inventory per Operation
### 4.2. Annotated Log Excerpts
### 4.3. Checkpoint Observation

## 5. Optimization Benchmark
### 5.1. Experimental Setup
### 5.2. Test Queries
### 5.3. Table Variants
### 5.4. Results
### 5.5. Analysis
### 5.6. Threats to Validity

## 6. Challenges, Limitations & Lessons Learned

## 7. Conclusion
### 7.1. Summary of Achievements
### 7.2. Future Work

## 8. Appendix
### 8.1. How to Reproduce
### 8.2. Additional Screenshots / Outputs
### 8.3. References
