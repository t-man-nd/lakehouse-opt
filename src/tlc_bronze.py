"""Bronze layer for the real TLC lane.

Principles
----------
* Source-faithful: every row of every file is appended; nothing is filtered.
* One Delta commit per source file, tagged with userMetadata (batch_id,
  source file, SHA-256) so any later step can find *which Bronze version*
  introduced a batch and time-travel to it.
* Idempotent: a batch whose `_batch_id` is already in Bronze is skipped.
  `_batch_id` embeds the file hash, so a republished file (new hash) is
  appended as a new batch instead of being ignored.
* Schema evolution: `mergeSchema=true`. Loading the 2024 files first and the
  2025 files second makes `cbd_congestion_fee` appear as a real schema change.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import DEFAULT_MONTHS, LakehousePaths, get_paths, trip_file_name
from src.delta_utils import commit_metadata, history_records, latest_version, table_exists
from src.dq_rules import harmonize_bronze_columns


def load_manifest(paths: LakehousePaths) -> Dict[str, dict]:
    if not paths.source_manifest.exists():
        raise FileNotFoundError(
            f"{paths.source_manifest} not found. Run `python scripts/download_sources.py` "
            "and make sure it finishes (it prints 'Manifest written')."
        )
    return json.loads(paths.source_manifest.read_text(encoding="utf-8"))["trip_files"]


def make_batch_id(month: str, sha256: str) -> str:
    return f"yellow_{month}_{sha256[:8]}"


def list_bronze_batches(spark: SparkSession, bronze_path: str) -> List[str]:
    if not table_exists(spark, bronze_path):
        return []
    rows = spark.read.format("delta").load(bronze_path).select("_batch_id").distinct().collect()
    return sorted(r["_batch_id"] for r in rows)


def find_batch_versions(spark: SparkSession, bronze_path: str) -> Dict[str, int]:
    """Map batch_id -> Bronze version that committed it (from commit userMetadata)."""
    mapping: Dict[str, int] = {}
    for record in history_records(spark, bronze_path):
        meta = record.get("userMetadata")
        if not meta:
            continue
        try:
            parsed = json.loads(meta)
        except json.JSONDecodeError:
            continue
        batch_id = parsed.get("batch_id")
        metrics = record.get("operationMetrics") or {}
        if str(metrics.get("numOutputRows", "")) == "0" or str(metrics.get("numFiles", "")) == "0":
            continue  # an empty append (e.g. a failed feed) must not count as the batch's version
        if batch_id and batch_id not in mapping:
            mapping[batch_id] = int(record["version"])
    return mapping


def read_source_file(spark: SparkSession, path: Path, dev_sample_pct: Optional[float] = None) -> DataFrame:
    df = spark.read.parquet(str(path))
    if dev_sample_pct:
        # Deterministic sample for laptops: same rows every run.
        bucket = F.abs(F.xxhash64(*[F.col(f"`{c}`") for c in df.columns])) % 10000
        df = df.filter(bucket < int(dev_sample_pct * 100))
    return df


def append_batch(
    spark: SparkSession,
    df: DataFrame,
    bronze_path: str,
    batch_id: str,
    month: str,
    source_file: str,
    sha256: str,
    expected_rows: Optional[int],
) -> Dict[str, object]:
    """Append one source file to Bronze as a single, tagged Delta commit."""
    harmonized, changes = harmonize_bronze_columns(df)
    landed = (
        harmonized.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_month", F.lit(month))
        .withColumn("_file_sha256", F.lit(sha256))
    )
    meta = {"layer": "bronze", "batch_id": batch_id, "source_file": source_file,
            "sha256": sha256, "harmonized": changes}
    with commit_metadata(spark, meta):
        (landed.write.format("delta").mode("append")
         .option("mergeSchema", "true")
         .partitionBy("_source_month")
         .save(bronze_path))
    version = latest_version(spark, bronze_path)
    written = (spark.read.format("delta").option("versionAsOf", version).load(bronze_path)
               .filter(F.col("_batch_id") == batch_id).count())
    if expected_rows is not None and written != expected_rows:
        raise RuntimeError(
            f"Bronze row count mismatch for {source_file}: wrote {written}, manifest says {expected_rows}"
        )
    return {"batch_id": batch_id, "source_file": source_file, "rows": written,
            "bronze_version": version, "harmonized": changes}


def run(
    spark: SparkSession,
    months: Optional[Iterable[str]] = None,
    overwrite: bool = False,
    paths: Optional[LakehousePaths] = None,
    dev_sample_pct: Optional[float] = None,
) -> Dict[str, object]:
    """Ingest the given months into Bronze. Returns counts for the pipeline log.

    overwrite=True deletes the Bronze table first. It exists for local resets
    only; it breaks the append-only guarantee and is never used by default.
    """
    paths = paths or get_paths()
    months = sorted(months or DEFAULT_MONTHS)
    manifest = load_manifest(paths)
    bronze_path = paths.bronze_trips

    if overwrite and Path(bronze_path).exists():
        shutil.rmtree(bronze_path)

    existing = set(list_bronze_batches(spark, bronze_path))
    ingested, skipped = [], []
    for month in months:
        info = manifest.get(month)
        if info is None:
            raise KeyError(f"Month {month} is not in the source manifest; download it first")
        batch_id = make_batch_id(month, info["sha256"])
        if batch_id in existing:
            skipped.append(batch_id)
            print(f"[bronze] skip {batch_id} (already ingested)")
            continue
        source = paths.raw_tlc_dir / trip_file_name(month)
        df = read_source_file(spark, source, dev_sample_pct)
        expected = None if dev_sample_pct else int(info["rows"])
        result = append_batch(spark, df, bronze_path, batch_id, month, source.name, info["sha256"], expected)
        print(f"[bronze] + {batch_id}: {result['rows']:,} rows -> v{result['bronze_version']} "
              f"{result['harmonized'] if any(result['harmonized'].values()) else ''}")
        ingested.append(result)

    bronze = spark.read.format("delta").load(bronze_path)
    counts = {r["_batch_id"]: r["count"] for r in bronze.groupBy("_batch_id").count().collect()}
    null_metadata = bronze.filter(
        F.col("_ingest_ts").isNull() | F.col("_source_file").isNull() | F.col("_batch_id").isNull()
        | F.col("_source_month").isNull() | F.col("_file_sha256").isNull()
    ).count()
    expected_total = None if dev_sample_pct else sum(
        int(manifest[m]["rows"]) for m in months if make_batch_id(m, manifest[m]["sha256"]) in counts)
    total = sum(counts.values())
    if expected_total is not None and total < expected_total:
        raise RuntimeError(f"Bronze has {total} rows but manifest expects at least {expected_total}")
    if null_metadata:
        raise RuntimeError(f"{null_metadata} Bronze rows have NULL lineage metadata")
    return {
        "bronze_count": total,
        "bronze_version": latest_version(spark, bronze_path),
        "rows_per_batch": dict(sorted(counts.items())),
        "ingested": ingested,
        "skipped": skipped,
        "null_metadata_rows": null_metadata,
        "dev_sample_pct": dev_sample_pct,
    }


def main() -> None:
    from src.spark_session import create_spark

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--months", nargs="+", default=DEFAULT_MONTHS)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    spark = create_spark("tlc-bronze")
    try:
        print(json.dumps(run(spark, args.months, args.overwrite), indent=2, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
