# Annotated `_delta_log` commit: latest Bronze append

File: `yellow_trips/_delta_log/00000000000000000006.json`  
Actions: {'add': 1, 'remove': 0, 'metaData': 0, 'protocol': 0}

Each line of a commit file is one action. Readers replay these actions (from the latest
checkpoint forward) to know exactly which Parquet files form a version; that replay is
what gives atomicity (a commit file either exists or not) and snapshot isolation
(a reader pins one version number).

## commitInfo

Audit record behind `DESCRIBE HISTORY`: operation, parameters, metrics, and our
`userMetadata` (batch id and source file hash).

```json
{
  "timestamp": 1790267522220,
  "operation": "WRITE",
  "operationParameters": {
    "mode": "Append",
    "partitionBy": "[\"_source_month\"]"
  },
  "readVersion": 5,
  "isolationLevel": "Serializable",
  "isBlindAppend": true,
  "operationMetrics": {
    "numFiles": "1",
    "numOutputRows": "30",
    "numOutputBytes": "10211"
  },
  "userMetadata": "{\"batch_id\": \"cdc_corrections_seed42\", \"harmonized\": {\"casted\": [], \"renamed\": []}, \"layer\": \"bronze\", \"sha256\": \"4025a0aaa61643e2dc5c248dda14a1d9884be4d91824987fbeb8d8cb4ffcea4c\", \"source_file\": \"synthetic://cdc_corrections_seed42.parquet\"}",
  "engineInfo": "Apache-Spark/4.0.1 Delta-Lake/4.0.1",
  "txnId": "148a73b3-4fe2-4a37-8711-c8c124f37737"
}
```

## One `add` action

`path` is a Parquet file now part of the table. `stats` holds `numRecords`, and per-column
`minValues`, `maxValues`, `nullCount`. A query such as `WHERE PULocationID = 132` compares
132 with each file's min/max and skips files that cannot contain it: this is data
skipping. Z-ORDER and liquid clustering work by making these min/max ranges narrow.

```json
{
  "add": {
    "path": "_source_month=2025-03/part-00000-179cf82c-3e71-4030-8e2d-94aba6371223.c000.snappy.parquet",
    "partitionValues": {
      "_source_month": "2025-03"
    },
    "size": 10211,
    "modificationTime": 1790267522214,
    "dataChange": true,
    "stats": "{\"numRecords\":30,\"minValues\":{\"VendorID\":2,\"tpep_pickup_datetime\":\"2025-03-03T17:35:33.000Z\",\"tpep_dropoff_datetime\":\"2025-03-03T17:40:52.000Z\",\"passenger_count\":1,\"trip_distance\":0.47,\"RatecodeID\":1,\"store_and_fwd_flag\":\"N\",\"PULocationID\":24,\"DOLocationID\":68,\"payment_type\":1,\"fare_amount\":5.1,\"extra\":0.0,\"mta_tax\":0.5,\"tip_amount\":1.0,\"tolls_amount\":0.0,\"improvement_surcharge\":1.0,\"total_amount\":12.92,\"congestion_surcharge\":0.0,\"Airport_fee\":0.0,\"_ingest_ts\":\"2026-09-24T16:32:02.094Z\",\"_source_file\":\"synthetic://cdc_corrections_seed\",\"_batch_id\":\"cdc_corrections_seed42\",\"_file_sha256\":\"4025a0aaa61643e2dc5c248dda14a1d9\",\"cbd_congestion_fee\":0.0},\"maxValues\":{\"VendorID\":2,\"tpep_pickup_datetime\":\"2025-03-27T21:05:13.000Z\",\"tpep_dropoff_datetime\":\"2025-03-27T21:14:38.000Z\",\"passenger_count\":3,\"trip_distance\":18.34,\"RatecodeID\":2,\"store_and_fwd_flag\":\"N\",\"PULocationID\":263,\"DOLocationID\":263,\"payment_type\":1,\"fare_amount\":70.0,\"extra\":2.5,\"mta_tax\":0.5,\"tip_amount\":8.25,\"tolls_amount\":6.94,\"improvement_surcharge\":1.0,\"total_amount\":88.44,\"congestion_surcharge\":2.5,\"Airport_fee\":1.75,\"_ingest_ts\":\"2026-09-24T16:32:02.094Z\",\"_source_file\":\"synthetic://cdc_corrections_seed\u007f\",\"_batch_id\":\"cdc_corrections_seed42\",\"_file_sha256\":\"4025a0aaa61643e2dc5c248dda14a1d9\u007f\",\"cbd_congestion_fee\":0.75},\"nullCount\":{\"VendorID\":0,\"tpep_pickup_datetime\":0,\"tpep_dropoff_datetime\":0,\"passenger_count\":0,\"trip_distance\":0,\"RatecodeID\":0,\"store_and_fwd_flag\":0,\"PULocationID\":0,\"DOLocationID\":0,\"payment_type\":0,\"fare_amount\":0,\"extra\":0,\"mta_tax\":0,\"tip_amount\":0,\"tolls_amount\":0,\"improvement_surcharge\":0,\"total_amount\":0,\"congestion_surcharge\":0,\"Airport_fee\":0,\"_ingest_ts\":0,\"_source_file\":0,\"_batch_id\":0,\"_file_sha256\":0,\"cbd_congestion_fee\":0}}"
  }
}
```
