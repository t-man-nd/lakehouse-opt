"""Arrow-only checks of the larger B1 population and reproducible sampling."""

from collections import Counter
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.generator import SOURCE_COLUMNS, generate_one_batch, main, normalize_source_record
from src.sampling import file_sha256, proportional_quotas, stratified_parquet_sample


def source_row(index, pickup):
    return {"VendorID": index, "tpep_pickup_datetime": pickup,
            "tpep_dropoff_datetime": pickup + timedelta(minutes=20),
            "PULocationID": 161, "DOLocationID": 230, "fare_amount": 20.0}


@pytest.fixture
def population(tmp_path):
    rows = []
    for count, date in ((70, datetime(2025, 1, 1, 7)), (20, datetime(2025, 1, 15, 18)), (10, datetime(2025, 1, 30, 23))):
        start = len(rows)
        rows.extend(source_row(start + index, date) for index in range(count))
    # Interleave days/hours to prove selection does not rely on file sort order.
    rows.sort(key=lambda row: row["VendorID"] % 7)
    for column, value in (("tpep_pickup_datetime", None), ("tpep_dropoff_datetime", None),
                          ("PULocationID", None), ("DOLocationID", None),
                          ("fare_amount", 0.0), ("fare_amount", -1.0), ("fare_amount", float("nan"))):
        row = source_row(100 + len(rows), datetime(2025, 1, 2))
        row[column] = value
        rows.append(row)
    rows.append(source_row(999, datetime(2024, 12, 31, 23)))
    path = tmp_path / "yellow_tripdata_2025-01.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path, row_group_size=13)
    return path, rows


def sample(path, count=50, seed=42, batch_size=7):
    return stratified_parquet_sample(path, count, seed, SOURCE_COLUMNS,
                                     normalize_source_record, batch_size=batch_size)


def test_proportional_quotas_preserve_demand_and_exact_sample_size():
    assert proportional_quotas({0: 700, 1: 200, 2: 100}, 101) == {0: 71, 1: 20, 2: 10}
    assert proportional_quotas({2: 1, 0: 1, 1: 1}, 2) == {2: 0, 0: 1, 1: 1}
    assert proportional_quotas({0: 9, 1: 1}, 1) == {0: 1, 1: 0}
    with pytest.raises(ValueError, match="eligible rows"):
        proportional_quotas({0: 3}, 4)


def test_sample_exact_proportions_and_source_position_provenance(population):
    path, source = population
    rows, metadata = sample(path)
    assert len(rows) == len({row["VendorID"] for row in rows}) == 50
    assert Counter(row["tpep_pickup_datetime"][:13] for row in rows) == {
        "2025-01-01 07": 35, "2025-01-15 18": 10, "2025-01-30 23": 5,
    }
    assert metadata["source_rows"] == 108
    assert metadata["eligible_rows"] == 100
    assert metadata["excluded_rows"] == 8
    assert metadata["selected_rows"] == 50
    assert metadata["source_sha256"] == file_sha256(path)
    assert metadata["pickup_month"] == "2025-01"
    selected_ids = {row["VendorID"] for row in rows}
    positions = [index for index, row in enumerate(source) if row["VendorID"] in selected_ids]
    assert [source[index]["VendorID"] for index in positions] == [row["VendorID"] for row in rows]
    assert metadata["selected_source_positions_sha256"] == hashlib.sha256(
        b"".join(struct.pack(">Q", index) for index in positions)).hexdigest()


def test_selection_deterministic_independent_of_batch_boundaries(population):
    path, _ = population
    first = sample(path, batch_size=7)
    assert first == sample(path, batch_size=31)
    changed = sample(path, seed=43)
    assert first[0] != changed[0]
    assert first[1]["strata"] == changed[1]["strata"]
    assert first[1]["selected_source_positions_sha256"] != changed[1]["selected_source_positions_sha256"]


def test_full_population_and_insufficient_eligible_rows(population):
    path, _ = population
    rows, metadata = sample(path, count=100)
    assert len(rows) == 100
    assert [stratum["source_rows"] for stratum in metadata["strata"]] == [70, 20, 10]
    assert [stratum["selected_rows"] for stratum in metadata["strata"]] == [70, 20, 10]
    with pytest.raises(ValueError, match="eligible rows"):
        sample(path, count=101)


def test_custom_source_filename_records_no_inferred_month(population, tmp_path):
    path, _ = population
    custom = tmp_path / "custom.parquet"
    custom.write_bytes(path.read_bytes())
    _, metadata = sample(custom)
    assert metadata["pickup_month"] is None
    assert metadata["eligible_rows"] == 101


def test_required_columns_checked_before_sampling(tmp_path):
    path = tmp_path / "missing.parquet"
    pq.write_table(pa.table({"fare_amount": [20.0]}), path)
    with pytest.raises(ValueError, match="missing required columns"):
        sample(path)


def test_stratified_batch_records_metadata_defects_and_protects_existing(population, tmp_path):
    path, _ = population
    output = tmp_path / "output"
    output.mkdir()
    kwargs = dict(batch_no=1, output_dir=output, source_file=path, base_rows=50, seed=42,
                  malformed_dates=1, missing_pu=1, missing_do=1, invalid_fares=1, duplicates=2,
                  sampling="stratified")
    manifest = generate_one_batch(**kwargs)
    assert manifest["raw_rows"] == 52
    assert manifest["expected_silver_rows"] == 46
    assert manifest["expected_rejected_rows"] == 6
    assert manifest["sampling"]["selected_rows"] == 50
    assert manifest["sampling"]["seed"] == 42
    raw = (output / "batch_01.json").read_bytes()
    assert len(raw.splitlines()) == 52
    with pytest.raises(FileExistsError, match="preserving"):
        generate_one_batch(**kwargs)
    assert (output / "batch_01.json").read_bytes() == raw


def test_default_first_mode_preserves_legacy_bytes(tmp_path):
    path = tmp_path / "yellow_tripdata_2025-01.parquet"
    pq.write_table(pa.Table.from_pylist([source_row(i, datetime(2025, 1, 1) + timedelta(minutes=i))
                                      for i in range(20)]), path)
    manifest = generate_one_batch(1, tmp_path, path, 14, 42, 1, 1, 1, 1, 2)
    # Frozen from the pre-sampling generator, including record key/file order.
    assert file_sha256(tmp_path / "batch_01.json") == "681db27bec1b1f5b7368be7b60ea9fdbc89e4f076a2c4e74ed81faed08486a7c"
    assert "sampling" not in manifest
    assert manifest["raw_rows"] == 16


def test_cli_emits_checksums_and_keeps_sampling_seed_separate(population, tmp_path, monkeypatch):
    path, _ = population
    output = tmp_path / "new_sample"
    monkeypatch.setattr("sys.argv", ["generator", "--sampling", "stratified", "--sampling-seed", "29",
                                    "--seed", "17", "--source-files", str(path), str(path), str(path),
                                    "--output-dir", str(output), "--base-rows", "50", "--duplicates", "2",
                                    "--malformed-dates", "1", "--missing-pu", "1", "--missing-do", "1",
                                    "--invalid-fares", "1"])
    main()
    manifest = json.loads((output / "error_manifest.json").read_text())
    assert manifest["seed"] == 17
    assert all(batch["sampling"]["seed"] == 29 for batch in manifest["batches"])
    checksums = {name: digest for digest, name in (line.split() for line in (output / "checksums.txt").read_text().splitlines())}
    assert set(checksums) == {"batch_01.json", "batch_02.json", "batch_03.json", "error_manifest.json"}
    assert all(file_sha256(output / name) == digest for name, digest in checksums.items())
    before = {file.name: file.read_bytes() for file in output.iterdir()}
    with pytest.raises(FileExistsError, match="existing files are preserved"):
        main()
    assert {file.name: file.read_bytes() for file in output.iterdir()} == before
