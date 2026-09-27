#!/usr/bin/env python3
"""Independent, bounded Arrow/Decimal check of this lab's three benchmark SQLs.

This is deliberately NOT a general Delta reader. It accepts local unpartitioned
tables with complete JSON history, no checkpoints, mapping or deletion vectors.
Run only after all timed experiments finish: hashing/scanning warms OS caches.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, localcontext, ROUND_HALF_UP
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import unquote

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

COLUMNS = ["PULocationID", "DOLocationID", "tpep_pickup_datetime", "fare_amount", "tip_amount"]
ORDER = ("baseline", "zorder_1col", "zorder_2col", "liquid_full")
QUERY_IDS = ("Q1_1D_PointLookup", "Q2_2D_LocationAndTime", "Q3_2D_RangeAggregation")
PROJECTION = ("SELECT COUNT(*) AS trip_count, AVG(CAST(fare_amount AS DECIMAL(18,4))) AS avg_fare, "
              "SUM(CAST(tip_amount AS DECIMAL(18,4))) AS total_tip FROM {table} WHERE ")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest_json(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def active_files(source):
    root = Path(source["path"]).resolve()
    log = root / "_delta_log"
    require(not list(log.glob("*.checkpoint.*")) and not (log / "_last_checkpoint").exists(),
            "Checkpoints are outside this independent reader's supported scope")
    active, history = {}, []
    for version in range(source["version"] + 1):
        path = log / f"{version:020d}.json"
        require(path.is_file(), f"Missing contiguous JSON history at version {version}")
        history.append({"version": version, "sha256": file_hash(path)})
        for line in path.read_text().splitlines():
            action = json.loads(line)
            if "protocol" in action:
                protocol = action["protocol"]
                require(protocol.get("minReaderVersion", 1) == 1 and not protocol.get("readerFeatures"),
                        f"Unsupported reader protocol: {protocol}")
            if "metaData" in action:
                meta = action["metaData"]
                require(not meta.get("partitionColumns"), "Partitioned tables are unsupported")
                config = meta.get("configuration", {})
                require(config.get("delta.columnMapping.mode", "none") == "none", "Column mapping unsupported")
                require(config.get("delta.enableDeletionVectors", "false").lower() != "true", "Deletion vectors unsupported")
            for kind in ("remove", "add"):
                if kind not in action:
                    continue
                entry = action[kind]
                require(entry.get("deletionVector") is None, "Deletion vector action unsupported")
                require(not entry.get("partitionValues"), "Partition values unsupported")
                if kind == "remove":
                    active.pop(entry["path"], None)
                else:
                    active[entry["path"]] = entry
    require(bool(active), "Empty active file set")
    files = []
    for relative, add in sorted(active.items()):
        path = (root / unquote(relative)).resolve()
        require("://" not in relative and not Path(unquote(relative)).is_absolute() and root in path.parents,
                "Nonlocal/escaping Delta path")
        require(path.is_file() and path.stat().st_size == add["size"], f"Missing/changed active file: {path}")
        metadata_rows = pq.ParquetFile(path).metadata.num_rows
        stats = json.loads(add.get("stats", "{}"))
        require(stats.get("numRecords") == metadata_rows, f"Delta/Parquet row-count mismatch: {relative}")
        files.append({"path": relative, "size_bytes": add["size"], "rows": metadata_rows, "sha256": file_hash(path)})
    require(sum(item["rows"] for item in files) == source["rows"], "Source count differs from Parquet metadata")
    return root, files, history


def parse_queries(queries):
    require(tuple(query["id"] for query in queries) == QUERY_IDS, "Expected exact three recorded lab queries")
    parsed = []
    patterns = (r"PULocationID = (-?\d+)",
                r"PULocationID = (-?\d+) AND tpep_pickup_datetime >= TIMESTAMP '([^']+)' AND tpep_pickup_datetime < TIMESTAMP '([^']+)'",
                r"PULocationID BETWEEN (-?\d+) AND (-?\d+) AND DOLocationID BETWEEN (-?\d+) AND (-?\d+)")
    for index, query in enumerate(queries):
        require(query["sql"] == PROJECTION + query["predicate"], "Unsupported aggregate SQL/projection")
        match = re.fullmatch(patterns[index], query["predicate"])
        require(match is not None, f"Unsupported predicate: {query['predicate']}")
        values = match.groups()
        parsed.append((int(values[0]), datetime.fromisoformat(values[1]), datetime.fromisoformat(values[2]))
                      if index == 1 else tuple(map(int, values)))
    return parsed


def decimal4(value):
    if value is None:
        return None
    value = Decimal(str(value))
    require(value.is_finite(), "Nonfinite decimal input unsupported")
    value = value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    require(abs(value) < Decimal("1e14"), "DECIMAL(18,4) overflow")
    return value


def canonical_decimal(value):
    if value is None:
        return None
    text = format(value, "f")
    return "0" if value == 0 else text.rstrip("0").rstrip(".") if "." in text else text


def oracle(root, files, predicates):
    totals = [[0, 0, Decimal(0), 0, Decimal(0)] for _ in predicates]
    scanned_rows = 0
    with localcontext() as context:
        context.prec = 60
        for info in files:
            parquet = pq.ParquetFile(root / unquote(info["path"]))
            for batch in parquet.iter_batches(columns=COLUMNS, batch_size=32768):
                scanned_rows += batch.num_rows
                pu, do, pickup = (batch.column(column) for column in COLUMNS[:3])
                for index, values in enumerate(predicates):
                    if index == 0:
                        mask = pc.equal(pu, values[0])
                    elif index == 1:
                        start, end = values[1:]
                        if pickup.type.tz:
                            start, end = start.replace(tzinfo=timezone.utc), end.replace(tzinfo=timezone.utc)
                        mask = pc.and_(pc.equal(pu, values[0]), pc.and_(pc.greater_equal(pickup, pa.scalar(start, type=pickup.type)),
                                                                 pc.less(pickup, pa.scalar(end, type=pickup.type))))
                    else:
                        mask = pc.and_(pc.and_(pc.greater_equal(pu, values[0]), pc.less_equal(pu, values[1])),
                                       pc.and_(pc.greater_equal(do, values[2]), pc.less_equal(do, values[3])))
                    selected = batch.filter(pc.fill_null(mask, False))
                    totals[index][0] += selected.num_rows
                    for column, count_slot, sum_slot in (("fare_amount", 1, 2), ("tip_amount", 3, 4)):
                        for value in selected.column(column).to_pylist():
                            number = decimal4(value)
                            if number is not None:
                                totals[index][count_slot] += 1
                                totals[index][sum_slot] += number
        expected = []
        for count, fare_count, fare_sum, tip_count, tip_sum in totals:
            mean = (fare_sum / fare_count).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP) if fare_count else None
            expected.append([{"trip_count": count, "avg_fare": canonical_decimal(mean),
                              "total_tip": canonical_decimal(tip_sum if tip_count else None)}])
    return expected, scanned_rows


def verify_phase(phase, order, expected, iterations):
    identities = Counter((m["query_id"], m["condition_id"], m["iteration"]) for m in phase["measurements"])
    required = Counter((qid, cid, iteration) for qid in QUERY_IDS for cid in order for iteration in range(1, iterations + 1))
    require(identities == required, "Missing/duplicate/unexpected timed measurement")
    warmups = Counter((m["query_id"], m["condition_id"]) for m in phase["warmups"])
    require(warmups == Counter((qid, cid) for qid in QUERY_IDS for cid in order), "Missing/duplicate warmup")
    for record in phase["measurements"] + phase["warmups"]:
        rows = expected[record["query_id"]]
        require(record["result_sha256"] == digest_json(rows), f"Result digest differs from Arrow oracle: {record}")
        if "iteration" in record:
            require(record["result_rows"] == rows and record["matching_trip_count"] == rows[0]["trip_count"],
                    "Measured result/count differs from Arrow oracle")
    return {"timed_results_verified": len(phase["measurements"]), "warmup_digests_verified": len(phase["warmups"])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("benchmark", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    require(not args.output.exists(), "Choose a fresh output evidence path")
    benchmark = json.loads(args.benchmark.read_text())
    require(benchmark["status"] == "passed", "Benchmark did not pass")
    require(tuple(benchmark["benchmark_metadata"]["condition_order"]) == ORDER, "Unexpected benchmark order")
    iterations = benchmark["benchmark_metadata"]["iterations"]
    require(isinstance(iterations, int) and iterations >= 3, "At least three iterations required")
    predicates = parse_queries(benchmark["queries"])
    root, files, history = active_files(benchmark["source"])
    rows, count = oracle(root, files, predicates)
    require(count == benchmark["source"]["rows"], "Scanned row count differs from recorded source")
    expected = dict(zip(QUERY_IDS, rows))
    for query in benchmark["queries"]:
        require(expected[query["id"]][0]["trip_count"] == query["matching_trip_count"] > 0, "Query count mismatch/empty predicate")
    phases = {"required": verify_phase(benchmark, ORDER, expected, iterations),
              "compaction": verify_phase(benchmark["compaction_control"], ("baseline", "compacted"), expected, iterations)}
    evidence = {"status": "passed", "method": "Independent PyArrow and Python Decimal; no Spark or production imports",
                "timestamp": datetime.now(timezone.utc).isoformat(), "validator_sha256": file_hash(Path(__file__)),
                "benchmark_path": str(args.benchmark.resolve()), "benchmark_sha256": file_hash(args.benchmark),
                "source": {"path": str(root), "version": benchmark["source"]["version"], "rows": count},
                "json_history": history, "active_files": files, "active_files_sha256": digest_json(files),
                "decimal_semantics": "String-form finite double to DECIMAL(18,4), HALF_UP; exact sum and average rounded HALF_UP to scale8 with precision60",
                "expected_query_results": expected, "phases": phases, "pyarrow_version": pa.__version__}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as handle:
        json.dump(evidence, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    print(json.dumps({"status": "passed", "source_rows": count, "phases": phases, "output": str(args.output)}))


if __name__ == "__main__":
    main()
