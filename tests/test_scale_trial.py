"""Resource accounting and bounded foreground trial behavior (no Spark jobs)."""

import json
from pathlib import Path
import sys

import pytest

from scripts.run_scale_trial import process_tree_rss, run_trial


def test_process_tree_rss_includes_nested_jvm_and_excludes_other_tasks():
    listing = """100 1 100
101 100 200
102 101 300
200 1 50000
201 200 40000
"""
    assert process_tree_rss(100, listing) == (600 * 1024, 3)
    assert process_tree_rss(999, listing) == (0, 0)


def test_successful_child_tracks_descendants_and_pipeline_stages(tmp_path):
    manifest = tmp_path / "pipeline.json"
    program = """
import json, subprocess, sys
from pathlib import Path
child = subprocess.Popen([sys.executable, '-c', 'import time; memory = bytearray(8000000); time.sleep(0.4)'])
child.wait()
Path(sys.argv[1]).write_text(json.dumps({'status': 'passed', 'counts': {'final_silver': 299983}, 'stages': {'b3': {'status': 'passed', 'elapsed_seconds': 1.5}}}))
print('finished')
"""
    result = run_trial([sys.executable, "-c", program, str(manifest)],
                       output=tmp_path / "metrics.json", log=tmp_path / "child.log", cwd=tmp_path,
                       pipeline_manifest=manifest, interval=0.025)
    assert result["status"] == "passed"
    assert result["returncode"] == 0
    assert result["peak_process_count"] >= 2
    assert result["peak_process_tree_rss_bytes"] > 8_000_000
    assert result["sample_count"] >= 2
    assert result["pipeline"]["counts"]["final_silver"] == 299983
    assert result["pipeline"]["stages"]["b3"]["elapsed_seconds"] == 1.5
    assert "finished" in (tmp_path / "child.log").read_text()
    assert json.loads((tmp_path / "metrics.json").read_text()) == result


def test_child_failure_keeps_exit_status_and_log(tmp_path):
    result = run_trial([sys.executable, "-c", "import sys; print('failure evidence'); sys.exit(7)"],
                       output=tmp_path / "metrics.json", log=tmp_path / "child.log", cwd=tmp_path,
                       interval=0.025)
    assert result["status"] == "failed"
    assert result["returncode"] == 7
    assert "failure evidence" in (tmp_path / "child.log").read_text()


def test_timeout_stops_only_own_process_group(tmp_path):
    result = run_trial([sys.executable, "-c", "import time; time.sleep(20)"],
                       output=tmp_path / "metrics.json", log=tmp_path / "child.log", cwd=tmp_path,
                       interval=0.025, timeout=0.08)
    assert result["status"] == "stopped_by_guard"
    assert result["stop_reason"] == "timeout"
    assert result["returncode"] != 0
    assert result["wall_seconds"] < 5


def test_existing_artifacts_and_overlaps_are_refused(tmp_path):
    output = tmp_path / "metrics.json"
    output.write_text("original")
    with pytest.raises(FileExistsError):
        run_trial([sys.executable, "-c", "raise AssertionError('must not launch')"],
                  output=output, log=tmp_path / "child.log", cwd=tmp_path)
    assert output.read_text() == "original"
    with pytest.raises(ValueError, match="distinct"):
        run_trial([sys.executable, "-c", "pass"], output=tmp_path / "same",
                  log=tmp_path / "same", cwd=tmp_path)


def test_low_disk_guard_prevents_child_launch(tmp_path):
    marker = tmp_path / "launched"
    with pytest.raises(RuntimeError, match="child was not started"):
        run_trial([sys.executable, "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()"],
                  output=tmp_path / "metrics.json", log=tmp_path / "child.log", cwd=tmp_path,
                  min_free_gib=10 ** 9)
    assert not marker.exists()


def test_rejects_stale_manifest_instead_of_reporting_old_success(tmp_path):
    manifest = tmp_path / "pipeline.json"
    manifest.write_text('{"status": "passed"}')
    with pytest.raises(FileExistsError, match="fresh pipeline manifest"):
        run_trial([sys.executable, "-c", "pass"], output=tmp_path / "metrics.json",
                  log=tmp_path / "child.log", cwd=tmp_path, pipeline_manifest=manifest)
