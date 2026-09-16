#!/usr/bin/env python3
"""
Unit tests for Task D2: Performance Lab & Storage Optimization Benchmark.
Implemented using standard unittest for 100% portability without external dependencies.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from optimization_benchmark import (
    ConditionMetadata,
    QueryDefinition,
    RunMeasurement,
    aggregate_results,
    compute_data_skipping_stats,
    define_queries,
)


class TestOptimizationBenchmark(unittest.TestCase):
    def test_define_queries(self):
        """Verify that benchmark query suite defines both 1D and 2D query dimensions."""
        queries = define_queries()
        self.assertGreaterEqual(len(queries), 3)
        dimensions = {q.dimension for q in queries}
        self.assertIn("1D", dimensions)
        self.assertIn("2D", dimensions)

        # Verify query 1 checks PULocationID
        q1 = next(q for q in queries if q.id == "Q1_1D_PointLookup")
        self.assertIn("PULocationID = 161", q1.sql_template)
        self.assertEqual(q1.params.get("pu_filter"), 161)

        # Verify query 2 checks both spatial and temporal filters
        q2 = next(q for q in queries if q.id == "Q2_2D_LocationAndTime")
        self.assertIn("PULocationID = 161", q2.sql_template)
        self.assertIn("tpep_pickup_datetime", q2.sql_template)
        self.assertIn("ts_range", q2.params)

    def test_aggregate_results_statistical_calculation(self):
        """Verify median, min, max, stdev and % improvement calculations."""
        conditions = {
            "cond1_baseline": ConditionMetadata(
                id="cond1_baseline",
                name="1. Baseline",
                description="Baseline",
                table_path="path/1",
                num_files=16,
                total_size_bytes=160000,
                avg_file_size_bytes=10000.0,
            ),
            "cond2_compacted": ConditionMetadata(
                id="cond2_compacted",
                name="2. OPTIMIZE",
                description="Compacted",
                table_path="path/2",
                num_files=2,
                total_size_bytes=120000,
                avg_file_size_bytes=60000.0,
            ),
            "cond3_zorder_1col": ConditionMetadata(
                id="cond3_zorder_1col",
                name="3. Z-ORDER 1 Col",
                description="Z-Order 1",
                table_path="path/3",
                num_files=4,
                total_size_bytes=130000,
                avg_file_size_bytes=32500.0,
            ),
            "cond4_zorder_2col": ConditionMetadata(
                id="cond4_zorder_2col",
                name="4. Z-ORDER 2 Col",
                description="Z-Order 2",
                table_path="path/4",
                num_files=4,
                total_size_bytes=135000,
                avg_file_size_bytes=33750.0,
            ),
        }

        queries = [
            QueryDefinition(
                id="Q1",
                name="Query 1",
                dimension="1D",
                sql_template="SELECT 1",
                description="Desc",
                params={},
            )
        ]

        measurements = [
            # Baseline: 100ms, 120ms, 110ms -> Median = 110ms
            RunMeasurement(1, "cond1_baseline", "Q1", 100.0, 10, 16, 0, 160000, 0),
            RunMeasurement(2, "cond1_baseline", "Q1", 120.0, 10, 16, 0, 160000, 0),
            RunMeasurement(3, "cond1_baseline", "Q1", 110.0, 10, 16, 0, 160000, 0),
            # Compacted: 60ms, 50ms, 55ms -> Median = 55ms -> Improvement = (110 - 55)/110 = 50.0%
            RunMeasurement(1, "cond2_compacted", "Q1", 60.0, 10, 2, 0, 120000, 0),
            RunMeasurement(2, "cond2_compacted", "Q1", 50.0, 10, 2, 0, 120000, 0),
            RunMeasurement(3, "cond2_compacted", "Q1", 55.0, 10, 2, 0, 120000, 0),
            # Z-Order 1: 30ms, 35ms, 40ms -> Median = 35ms -> Improvement = (110 - 35)/110 = 68.18%
            RunMeasurement(1, "cond3_zorder_1col", "Q1", 30.0, 10, 1, 3, 32500, 97500),
            RunMeasurement(2, "cond3_zorder_1col", "Q1", 35.0, 10, 1, 3, 32500, 97500),
            RunMeasurement(3, "cond3_zorder_1col", "Q1", 40.0, 10, 1, 3, 32500, 97500),
            # Z-Order 2: 25ms, 20ms, 30ms -> Median = 25ms -> Improvement = (110 - 25)/110 = 77.27%
            RunMeasurement(1, "cond4_zorder_2col", "Q1", 25.0, 10, 1, 3, 33750, 101250),
            RunMeasurement(2, "cond4_zorder_2col", "Q1", 20.0, 10, 1, 3, 33750, 101250),
            RunMeasurement(3, "cond4_zorder_2col", "Q1", 30.0, 10, 1, 3, 33750, 101250),
        ]

        summary = aggregate_results(conditions, queries, measurements)
        q1_stats = summary["results"]["Q1"]["conditions"]

        self.assertEqual(q1_stats["cond1_baseline"]["median_ms"], 110.0)
        self.assertEqual(q1_stats["cond1_baseline"]["pct_improvement"], 0.0)

        self.assertEqual(q1_stats["cond2_compacted"]["median_ms"], 55.0)
        self.assertEqual(q1_stats["cond2_compacted"]["pct_improvement"], 50.0)

        self.assertEqual(q1_stats["cond3_zorder_1col"]["median_ms"], 35.0)
        self.assertEqual(q1_stats["cond3_zorder_1col"]["pct_improvement"], 68.18)
        self.assertEqual(q1_stats["cond3_zorder_1col"]["files_scanned"], 1)
        self.assertEqual(q1_stats["cond3_zorder_1col"]["files_skipped"], 3)

        self.assertEqual(q1_stats["cond4_zorder_2col"]["median_ms"], 25.0)
        self.assertEqual(q1_stats["cond4_zorder_2col"]["pct_improvement"], 77.27)

    def test_compute_data_skipping_stats(self):
        """Verify Delta Log stats parsing and pruning calculation."""
        temp_dir = tempfile.mkdtemp()
        try:
            table_dir = Path(temp_dir)
            log_dir = table_dir / "_delta_log"
            log_dir.mkdir(parents=True)

            commit_file = log_dir / "00000000000000000000.json"

            # File 1 contains PULocationID in range [1, 50]
            entry1 = {
                "add": {
                    "path": "part-01.parquet",
                    "size": 1000,
                    "stats": json.dumps({
                        "numRecords": 100,
                        "minValues": {"PULocationID": 1},
                        "maxValues": {"PULocationID": 50},
                    }),
                }
            }
            # File 2 contains PULocationID in range [150, 200]
            entry2 = {
                "add": {
                    "path": "part-02.parquet",
                    "size": 1000,
                    "stats": json.dumps({
                        "numRecords": 100,
                        "minValues": {"PULocationID": 150},
                        "maxValues": {"PULocationID": 200},
                    }),
                }
            }

            with open(commit_file, "w", encoding="utf-8") as f:
                f.write(json.dumps(entry1) + "\n")
                f.write(json.dumps(entry2) + "\n")

            # Filter for PULocationID = 161: File 1 should be skipped, File 2 scanned!
            scanned, skipped, bytes_r, bytes_s = compute_data_skipping_stats(
                str(table_dir), pu_filter=161
            )
            self.assertEqual(scanned, 1)
            self.assertEqual(skipped, 1)
            self.assertEqual(bytes_r, 1000)
            self.assertEqual(bytes_s, 1000)

            # Filter for PULocationID = 300: Both files should be skipped!
            scanned, skipped, bytes_r, bytes_s = compute_data_skipping_stats(
                str(table_dir), pu_filter=300
            )
            self.assertEqual(scanned, 0)
            self.assertEqual(skipped, 2)
            self.assertEqual(bytes_r, 0)
            self.assertEqual(bytes_s, 2000)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
