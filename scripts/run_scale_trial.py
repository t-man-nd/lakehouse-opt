"""Run one foreground scale trial and record sampled process-tree resources.

This wrapper does not alter Spark settings. Its RSS measurement sums resident
memory of the launched process and its live descendants, including the JVM.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time


import psutil


def process_tree_rss(root_pid: int, process_listing: str | None = None) -> tuple[int, int]:
    """Return (sum RSS bytes, process count) from psutil or simulated ps snapshot."""
    if process_listing is not None:
        rows = {}
        for line in process_listing.splitlines():
            fields = line.split()
            if len(fields) != 3:
                continue
            try:
                pid, parent, rss_kib = map(int, fields)
            except ValueError:
                continue
            rows[pid] = (parent, rss_kib * 1024)
        selected = {root_pid}
        while True:
            children = {pid for pid, (parent, _) in rows.items() if parent in selected}
            updated = selected | children
            if updated == selected:
                break
            selected = updated
        present = selected & rows.keys()
        return sum(rows[pid][1] for pid in present), len(present)

    try:
        root = psutil.Process(root_pid)
        procs = [root] + root.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.ZombieProcess):
        return 0, 0

    total_rss = 0
    count = 0
    for p in procs:
        try:
            total_rss += p.memory_info().rss
            count += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return total_rss, count


def _stop_process_group(process: subprocess.Popen) -> None:
    """Stop the process and all descendants across Windows and Linux."""
    try:
        root = psutil.Process(process.pid)
        procs = root.children(recursive=True) + [root]
    except (psutil.NoSuchProcess, psutil.ZombieProcess):
        return

    for p in procs:
        try:
            p.terminate()
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            pass

    gone, alive = psutil.wait_procs(procs, timeout=5.0)
    for p in alive:
        try:
            p.kill()
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            pass
    try:
        process.wait(timeout=5.0)
    except (subprocess.TimeoutExpired, Exception):
        pass


def _write_json(path: Path, value: dict) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(path)


def run_trial(command: list[str], *, output: Path, log: Path, cwd: Path,
              samples: Path | None = None, pipeline_manifest: Path | None = None,
              interval: float = 1.0, timeout: float | None = None,
              max_rss_gib: float | None = None, min_free_gib: float | None = None,
              disk_path: Path | None = None) -> dict:
    """Run a single child command without a shell; refuse existing artifacts."""
    if not command:
        raise ValueError("A child command is required after --")
    if interval <= 0 or any(value is not None and value <= 0 for value in (timeout, max_rss_gib, min_free_gib)):
        raise ValueError("Sampling interval and supplied limits must be positive")
    cwd = cwd.resolve(strict=True)
    if not cwd.is_dir():
        raise ValueError("cwd must be a directory")
    disk_path = (disk_path or cwd).resolve(strict=True)
    output, log = output.resolve(), log.resolve()
    samples = (samples or output.with_suffix(".samples.csv")).resolve()
    artifacts = [output, log, samples]
    if any(left == right or left in right.parents or right in left.parents
           for index, left in enumerate(artifacts) for right in artifacts[index + 1:]):
        raise ValueError("Output, log, and samples paths must be distinct and non-overlapping")
    if pipeline_manifest is not None:
        pipeline_manifest = pipeline_manifest.resolve()
        if any(pipeline_manifest == path or pipeline_manifest in path.parents or path in pipeline_manifest.parents
               for path in artifacts):
            raise ValueError("Pipeline manifest must not overlap wrapper artifacts")
        if pipeline_manifest.exists():
            raise FileExistsError(f"Use a fresh pipeline manifest: {pipeline_manifest}")
    for path in artifacts:
        if path.exists():
            raise FileExistsError(f"Trial artifacts are immutable; use a new path: {path}")
    initial_free = shutil.disk_usage(disk_path).free
    if min_free_gib is not None and initial_free < min_free_gib * 1024 ** 3:
        raise RuntimeError("Free disk is below --min-free-gib; child was not started")
    for path in artifacts:
        path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    result = {
        "status": "starting", "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "command": command, "cwd": str(cwd), "log": str(log), "samples": str(samples),
        "measurement": {
            "memory": "Sampled sum of RSS for child PID and live descendant PIDs from ps; includes JVM",
            "memory_unit": "bytes", "sampling_interval_seconds": interval,
            "limitations": "Sampled peak can miss brief spikes; shared pages may be counted more than once; RSS is not heap usage or unique physical memory. Limits are sampled guards, not OS reservations.",
            "disk_path": str(disk_path),
        },
        "limits": {"timeout_seconds": timeout, "max_rss_gib": max_rss_gib, "min_free_gib": min_free_gib},
        "disk_free_before_bytes": initial_free, "disk_free_min_bytes": initial_free,
        "peak_process_tree_rss_bytes": 0, "peak_process_count": 0, "sample_count": 0,
        "sampling_error_count": 0,
    }
    # Exclusive creation is also checked at open time to avoid replacing another run.
    with output.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2)
    process = None
    try:
        with log.open("x", encoding="utf-8") as log_handle, samples.open("x", newline="", encoding="utf-8") as sample_handle:
            writer = csv.writer(sample_handle)
            writer.writerow(["elapsed_seconds", "process_tree_rss_bytes", "process_count", "disk_free_bytes"])
            process = subprocess.Popen(command, cwd=cwd, stdout=log_handle, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            result.update({"status": "running", "pid": process.pid})
            _write_json(output, result)
            while True:
                elapsed = time.monotonic() - started
                free = shutil.disk_usage(disk_path).free
                result["disk_free_min_bytes"] = min(result["disk_free_min_bytes"], free)
                try:
                    rss, count = process_tree_rss(process.pid)
                    result["sample_count"] += 1
                    result["peak_process_tree_rss_bytes"] = max(result["peak_process_tree_rss_bytes"], rss)
                    result["peak_process_count"] = max(result["peak_process_count"], count)
                    writer.writerow([round(elapsed, 3), rss, count, free])
                    sample_handle.flush()
                except (subprocess.SubprocessError, OSError) as exc:
                    rss = 0
                    result["sampling_error_count"] += 1
                    result["last_sampling_error"] = str(exc)
                returncode = process.poll()
                if returncode is not None:
                    result.update({"returncode": returncode, "status": "passed" if returncode == 0 else "failed"})
                    break
                reason = None
                if timeout is not None and elapsed >= timeout:
                    reason = "timeout"
                elif max_rss_gib is not None and rss > max_rss_gib * 1024 ** 3:
                    reason = "process_tree_rss_limit"
                elif min_free_gib is not None and free < min_free_gib * 1024 ** 3:
                    reason = "free_disk_limit"
                if reason:
                    _stop_process_group(process)
                    result.update({"status": "stopped_by_guard", "stop_reason": reason, "returncode": process.returncode})
                    break
                time.sleep(interval)
    except BaseException as exc:
        if process is not None:
            _stop_process_group(process)
        result.update({"status": "interrupted" if isinstance(exc, KeyboardInterrupt) else "wrapper_failed",
                       "error": {"type": type(exc).__name__, "message": str(exc)},
                       "returncode": process.returncode if process is not None else None})
        raise
    finally:
        result.update({"completed_at_utc": datetime.now(timezone.utc).isoformat(),
                       "wall_seconds": round(time.monotonic() - started, 3),
                       "disk_free_after_bytes": shutil.disk_usage(disk_path).free})
        result["disk_used_delta_bytes"] = initial_free - result["disk_free_after_bytes"]
        if pipeline_manifest is not None:
            result["pipeline_manifest"] = str(pipeline_manifest)
            if pipeline_manifest.is_file():
                try:
                    manifest = json.loads(pipeline_manifest.read_text(encoding="utf-8"))
                    result["pipeline"] = {key: manifest[key] for key in
                                          ("status", "scope", "stages", "counts", "elapsed_seconds", "failed_stage", "error")
                                          if key in manifest}
                except (ValueError, OSError) as exc:
                    result["pipeline_manifest_error"] = str(exc)
            else:
                result["pipeline_manifest_error"] = "Child did not create the expected manifest"
        _write_json(output, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Fresh metrics JSON path")
    parser.add_argument("--log", type=Path, required=True, help="Fresh child stdout/stderr log path")
    parser.add_argument("--samples", type=Path, help="Fresh RSS samples CSV (defaults beside metrics JSON)")
    parser.add_argument("--cwd", type=Path, default=Path.cwd())
    parser.add_argument("--pipeline-manifest", type=Path, help="Fresh E1 manifest created by the child; include stage results")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, help="Wall-clock guard in seconds")
    parser.add_argument("--max-rss-gib", type=float, help="Sampled aggregate child/JVM RSS guard")
    parser.add_argument("--min-free-gib", type=float, help="Stop if free disk falls below this value")
    parser.add_argument("--disk-path", type=Path, help="Filesystem to monitor (defaults to cwd)")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("Supply a child command after --")
    result = run_trial(**vars(args))
    print(json.dumps({key: result[key] for key in
                      ("status", "returncode", "wall_seconds", "peak_process_tree_rss_bytes", "disk_used_delta_bytes")}, indent=2))
    if result["status"] == "stopped_by_guard":
        return 124 if result["stop_reason"] == "timeout" else 125
    returncode = result["returncode"]
    return returncode if returncode >= 0 else 128 - returncode


if __name__ == "__main__":
    raise SystemExit(main())
