"""Shared pytest fixtures.

One Spark session per test run. Delta is attempted first; when its JARs cannot
be resolved (offline machine, blocked Maven), the session falls back to plain
Spark and every test marked `delta` is skipped with a clear reason. Pure
DataFrame tests (DQ rules, Gold logic, scan metrics) always run.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

# Spark's session time zone is UTC; make Python agree so collected timestamps are not shifted
# to the machine's local zone (e.g. UTC+7).
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_STATE = {"delta": False, "reason": ""}


def _make_session(with_delta: bool):
    from src.spark_session import create_spark

    return create_spark("pytest", master="local[2]", driver_memory="1g", shuffle_partitions=2, with_delta=with_delta)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    os.environ.setdefault("SPARK_LOG_LEVEL", "ERROR")
    session = None
    if os.environ.get("LAKEHOUSE_TEST_NO_DELTA") != "1":
        try:
            session = _make_session(with_delta=True)
            probe = str(tmp_path_factory.mktemp("delta_probe") / "t")
            session.range(1).write.format("delta").save(probe)
            _STATE["delta"] = True
        except Exception as exc:  # noqa: BLE001 - any failure means Delta is unavailable
            _STATE["reason"] = f"Delta Lake unavailable: {type(exc).__name__}: {str(exc).splitlines()[0][:160]}"
            if session is not None:
                session.stop()
            session = None
    if session is None:
        session = _make_session(with_delta=False)
        _STATE["reason"] = _STATE["reason"] or "Delta disabled by LAKEHOUSE_TEST_NO_DELTA=1"
    session.conf.set("spark.sql.shuffle.partitions", "2")
    yield session
    session.stop()


@pytest.fixture()
def require_delta(spark):
    if not _STATE["delta"]:
        pytest.skip(_STATE["reason"])
    return spark


def pytest_configure(config):
    config.addinivalue_line("markers", "delta: needs Delta Lake JARs (skipped when unavailable)")


def pytest_collection_modifyitems(config, items):
    """Run the team's tests/test_silver.py last: its own module-scoped fixture calls
    getOrCreate() (receiving the shared session) and stops it on teardown, which would break
    any test module that runs after it."""
    items.sort(key=lambda item: item.fspath.basename == "test_silver.py")
