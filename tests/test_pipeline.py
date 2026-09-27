"""E1 boundary checks; the official-data CLI run supplies full-run evidence."""

import hashlib
import json
from pathlib import Path

import pytest

from lakehouse_pipeline import _sampling_provenance, run, verify_inputs
from src.bronze import _batch_files, run as run_bronze
from src.gold import run as run_gold
from src.time_travel import file_hashes, stream_file_signature


def test_b1_checksums_fail_before_creating_outputs(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    lines = []
    for index in range(1, 4):
        path = raw / f"batch_{index:02d}.json"
        path.write_text(json.dumps({"trip_id": str(index)}) + "\n")
        lines.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}")
    checksums = tmp_path / "checksums.txt"
    checksums.write_text("\n".join(lines) + "\n")
    verified = verify_inputs(str(raw), str(checksums))
    assert sum(batch["rows"] for batch in verified.values()) == 3
    (raw / "batch_02.json").write_text('{"trip_id":"changed"}\n')
    run_dir = tmp_path / "new_run"
    with pytest.raises(ValueError, match="checksum differs"):
        run(None, {"raw_dir": str(raw), "checksums": str(checksums)}, str(run_dir))
    assert not run_dir.exists()


@pytest.mark.parametrize("text", ["", "one", "one\n", "one\r\ntwo\rthree\n\n", "một\u2028hai\x85ba",
                                  "a" * (1024 * 1024 - 1) + "\r\ntwo\n",
                                  "a" * (1024 * 1024 - 1) + "ộ\nend"])
def test_streamed_file_signature_matches_sha_and_splitlines(tmp_path, text):
    path = tmp_path / "raw.json"
    content = text.encode("utf-8")
    path.write_bytes(content)
    expected = hashlib.sha256(content).hexdigest()
    assert stream_file_signature(path, count_lines=True) == (expected, len(text.splitlines()))
    assert stream_file_signature(path) == (expected, None)


def test_input_verification_and_vacuum_hashing_do_not_read_whole_files(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    expected = {}
    for index in range(1, 4):
        content = (f'{{"trip_id":"{index}"}}\r\n' * 200).encode()
        path = raw / f"batch_{index:02d}.json"
        path.write_bytes(content)
        expected[path.name] = hashlib.sha256(content).hexdigest()
    checksums = tmp_path / "checksums.txt"
    checksums.write_text("".join(f"{digest}  {name}\n" for name, digest in expected.items()))
    read_text = Path.read_text

    def forbid_read_bytes(*args, **kwargs):
        raise AssertionError("Large file hashes must stream")

    def metadata_only(path, *args, **kwargs):
        if path.parent == raw:
            raise AssertionError("Raw JSON line counts must stream")
        return read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", forbid_read_bytes)
    monkeypatch.setattr(Path, "read_text", metadata_only)
    assert file_hashes(raw) == expected
    result = verify_inputs(str(raw), str(checksums))
    assert {name: value["sha256"] for name, value in result.items()} == expected
    assert all(value["rows"] == 200 for value in result.values())


def test_sampling_provenance_is_verified_and_compact(tmp_path):
    manifest = tmp_path / "error_manifest.json"
    manifest.write_text(json.dumps({"batches": [{"batch": "batch_01", "source": "source.parquet", "sampling": {
        "method": "proportional_pickup_day_hour_without_replacement_v1", "seed": 42,
        "selected_rows": 3, "source_sha256": "source-digest", "strata": [
            {"pickup_hour": "2025-01-01 00:00:00", "source_rows": 100, "selected_rows": 3},
            {"pickup_hour": "2025-01-01 01:00:00", "source_rows": 1, "selected_rows": 0},
        ]}}]}))
    checksum = hashlib.sha256(manifest.read_bytes()).hexdigest()
    checksums = tmp_path / "checksums.txt"
    checksums.write_text(f"{checksum}  {manifest.name}\n")
    config = {"sampling_manifest": str(manifest), "checksums": str(checksums)}
    result = _sampling_provenance(config)
    assert result["sha256"] == checksum
    assert result["batches"][0]["strata_summary"] == {"eligible_strata": 2, "sampled_strata": 1, "selected_rows": 3}
    assert result["batches"][0]["sampling"]["source_sha256"] == "source-digest"
    assert "strata" not in result["batches"][0]["sampling"]
    assert _sampling_provenance({}) is None
    manifest.write_text('{"batches":[]}')
    with pytest.raises(ValueError, match="Sampling manifest checksum"):
        _sampling_provenance(config)


def test_existing_run_is_preserved_before_any_spark_action(tmp_path):
    original = tmp_path / "keep.txt"
    original.write_text("earlier evidence")
    with pytest.raises(FileExistsError, match="existing runs are preserved"):
        run(None, {}, str(tmp_path))
    assert original.read_text() == "earlier evidence"


@pytest.mark.parametrize("iterations", [0, 1, 2, 3.5, True])
def test_d2_minimum_repetitions_rejected_before_writes(tmp_path, iterations):
    destination = tmp_path / "new_run"
    with pytest.raises(ValueError, match="benchmark_iterations >= 3"):
        run(None, {"benchmark_iterations": iterations}, str(destination))
    assert not destination.exists()


def test_bronze_month_mapping_and_append_only_contract():
    assert _batch_files(None) == ["batch_01.json", "batch_02.json", "batch_03.json"]
    assert _batch_files(["2025-03", "2025-01"]) == ["batch_03.json", "batch_01.json"]
    with pytest.raises(ValueError, match="distinct"):
        _batch_files(["2025-01", "2025-01"])
    with pytest.raises(ValueError, match="YYYY-MM"):
        _batch_files(["2025-04"])
    with pytest.raises(ValueError, match="append-only"):
        run_bronze(None, overwrite=True)


def test_gold_public_entrypoint_requires_explicit_overwrite(tmp_path):
    marker = tmp_path / "existing-evidence.json"
    marker.write_text("{}")
    with pytest.raises(FileExistsError, match="overwrite=True"):
        run_gold(None, gold_dir=str(tmp_path))
    assert marker.read_text() == "{}"


def test_default_config_runs_all_required_stages():
    config = json.loads((Path(__file__).parents[1] / "config/pipeline.json").read_text())
    assert config["benchmark_enabled"] is True
    assert config["benchmark_iterations"] >= 3
    assert config["manifest_output"] == "docs/pipeline_run.json"


@pytest.fixture
def publication_config(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "batch_01.json").write_text('{"trip_id":"original"}\n')
    checksums = tmp_path / "checksums.txt"
    checksums.write_text("original checksum evidence")
    manifest = tmp_path / "error_manifest.json"
    manifest.write_text('{"contract_version":"B1-v1.0"}')
    config_file = tmp_path / "pipeline_config.json"
    config_file.write_text('{"original":"configuration"}')
    published = tmp_path / "published"
    return {
        "raw_dir": str(raw), "checksums": str(checksums), "manifest": str(manifest),
        "manifest_output": str(published / "run.json"),
        "gold_summary_output": str(published / "gold.json"),
        "benchmark_summary_output": str(published / "benchmark.json"),
        "benchmark_report_output": str(published / "report.md"),
    }, config_file


@pytest.mark.parametrize("protected", ["raw_json", "raw_child", "raw_symlink", "checksums", "manifest", "config", "silver_table", "evidence"])
def test_published_manifest_cannot_replace_sources_or_run_outputs(tmp_path, publication_config, protected):
    config, config_file = publication_config
    run_dir = tmp_path / "fresh_run"
    targets = {
        "raw_json": Path(config["raw_dir"]) / "batch_01.json",
        "raw_child": Path(config["raw_dir"]) / "summaries" / "run.json",
        "checksums": Path(config["checksums"]), "manifest": Path(config["manifest"]),
        "config": config_file, "silver_table": run_dir / "silver" / "_delta_log" / "run.json",
        "evidence": run_dir / "evidence" / "pipeline_run.json",
    }
    alias = tmp_path / "raw_alias.json"
    try:
        alias.symlink_to(targets["raw_json"])
    except OSError:
        if sys.platform == "win32":
            import subprocess
            junc_dir = tmp_path / "_junc_raw"
            subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(junc_dir), str(Path(config["raw_dir"]).resolve())],
                check=True, capture_output=True, text=True,
            )
            alias = junc_dir / "batch_01.json"
        else:
            raise
    targets["raw_symlink"] = alias
    originals = {path: path.read_bytes() for path in (targets["raw_json"], targets["checksums"], targets["manifest"], config_file)}
    with pytest.raises(ValueError, match="must not overlap"):
        run(None, config, str(run_dir), output_path=str(targets[protected]), config_path=str(config_file))
    assert not run_dir.exists()
    assert not (tmp_path / "published").exists()
    for path, content in originals.items():
        assert path.read_bytes() == content


@pytest.mark.parametrize("key", ["gold_summary_output", "benchmark_summary_output", "benchmark_report_output"])
def test_all_publication_settings_are_checked_before_writes(tmp_path, publication_config, key):
    config, config_file = publication_config
    config[key] = str(Path(config["raw_dir"]) / "batch_01.json")
    destination = tmp_path / "fresh_run"
    with pytest.raises(ValueError, match="must not overlap"):
        run(None, config, str(destination), config_path=str(config_file))
    assert not destination.exists()
    assert json.loads((Path(config["raw_dir"]) / "batch_01.json").read_text())["trip_id"] == "original"


@pytest.mark.parametrize("alias_kind", ["same", "symlink", "nested"])
def test_publication_artifacts_cannot_alias_each_other(tmp_path, publication_config, alias_kind):
    config, config_file = publication_config
    shared = tmp_path / "shared_output.json"
    if alias_kind == "same":
        second = shared
    elif alias_kind == "nested":
        second = shared / "nested.json"
    else:
        second = tmp_path / "shared_alias.json"
        try:
            second.symlink_to(shared)
        except OSError:
            if sys.platform == "win32":
                import subprocess
                junc_dir = tmp_path / "_junc_shared"
                subprocess.run(
                    ["cmd", "/c", "mklink", "/J", str(junc_dir), str(shared.parent.resolve())],
                    check=True, capture_output=True, text=True,
                )
                second = junc_dir / shared.name
            else:
                raise
    config["manifest_output"] = str(shared)
    config["benchmark_summary_output"] = str(second)
    with pytest.raises(ValueError, match="distinct and non-overlapping"):
        run(None, config, str(tmp_path / "fresh_run"), config_path=str(config_file))
    assert not (tmp_path / "fresh_run").exists()
    assert not shared.exists()
