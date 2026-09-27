"""Reproducible proportional day/hour sampling of local TLC Parquet files.

Both passes read bounded Arrow batches. The first counts eligible rows by pickup
hour; the second converts only the selected rows into Python records. Within
each stratum every eligible source-row position has the same inclusion chance.
"""

from __future__ import annotations

from array import array
from collections import Counter
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import random
import re
import sys
from typing import Callable


REQUIRED_COLUMNS = (
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID",
    "DOLocationID", "fare_amount",
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def proportional_quotas(counts: dict[int, int], needed: int) -> dict[int, int]:
    """Hamilton allocation: exact total, each quota within one of its ideal."""
    total = sum(counts.values())
    if needed <= 0 or needed > total:
        raise ValueError(f"Need a positive sample <= eligible rows ({total}); got {needed}")
    if any(count <= 0 for count in counts.values()):
        raise ValueError("Stratum populations must be positive")
    quotas = {key: needed * count // total for key, count in counts.items()}
    remainder_order = sorted(counts, key=lambda key: (-(needed * counts[key] % total), key))
    for key in remainder_order[:needed - sum(quotas.values())]:
        quotas[key] += 1
    return quotas


def _source_month(path: Path) -> tuple[datetime, datetime] | None:
    match = re.search(r"yellow_tripdata_(\d{4})-(\d{2})(?:\D|$)", path.name)
    if match is None:
        return None
    year, month = map(int, match.groups())
    start = datetime(year, month, 1)
    end = datetime(year + (month == 12), month % 12 + 1, 1)
    return start, end


def _eligible_hours(batch, month):
    import pyarrow as pa
    import pyarrow.compute as pc

    mask = pc.is_valid(batch.column(REQUIRED_COLUMNS[0]))
    for column in REQUIRED_COLUMNS[1:]:
        mask = pc.and_(mask, pc.is_valid(batch.column(column)))
    fare = batch.column("fare_amount")
    mask = pc.and_(mask, pc.and_(pc.is_finite(fare), pc.greater(fare, 0)))
    # NaN/inf locations cannot be converted to a valid integer by B1.
    for column in ("PULocationID", "DOLocationID"):
        mask = pc.and_(mask, pc.is_finite(batch.column(column)))
    pickup = batch.column("tpep_pickup_datetime")
    if month is not None:
        start, end = (pa.scalar(value, type=pickup.type) for value in month)
        mask = pc.and_(mask, pc.and_(pc.greater_equal(pickup, start), pc.less(pickup, end)))
    mask = pc.fill_null(mask, False)
    hours = pc.cast(pc.cast(pc.floor_temporal(pc.filter(pickup, mask), unit="hour"), pa.timestamp("s")), pa.int64())
    return mask, hours


def stratified_parquet_sample(
    path: Path, needed: int, seed: int, columns: list[str],
    normalize: Callable[[dict], dict | None], *, batch_size: int = 65_536,
) -> tuple[list[dict], dict]:
    """Return an exact sample in source order plus auditable population metadata.

    Official monthly filenames restrict pickup timestamps to that month. Custom
    filenames retain all valid pickup dates; the manifest states this explicitly.
    Keys and row-position digests use source positions, never synthetic trip IDs.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    path = Path(path)
    if needed <= 0 or batch_size <= 0:
        raise ValueError("needed and batch_size must be positive")
    initial_stat = path.stat()
    source_hash = file_sha256(path)
    parquet = pq.ParquetFile(path)
    available = set(parquet.schema_arrow.names)
    missing = set(REQUIRED_COLUMNS) - available
    if missing:
        raise ValueError(f"{path} is missing required columns: {sorted(missing)}")
    for column in REQUIRED_COLUMNS[:2]:
        if not pa.types.is_timestamp(parquet.schema_arrow.field(column).type):
            raise ValueError(f"Stratified sampling requires a Parquet timestamp column: {column}")
    for column in REQUIRED_COLUMNS[2:]:
        dtype = parquet.schema_arrow.field(column).type
        if not (pa.types.is_integer(dtype) or pa.types.is_floating(dtype)):
            raise ValueError(f"Stratified sampling requires a numeric column: {column}")
    month = _source_month(path)
    populations: Counter[int] = Counter()
    for batch in parquet.iter_batches(batch_size=batch_size, columns=list(REQUIRED_COLUMNS)):
        _, hours = _eligible_hours(batch, month)
        for item in pc.value_counts(hours).to_pylist():
            populations[item["values"]] += item["counts"]
    quotas = proportional_quotas(dict(populations), needed)
    # An independent RNG per hour also makes selection invariant to Arrow's
    # batch/row-group boundaries and dictionary iteration order.
    selected_ranks = {
        key: sorted(random.Random(f"tlc-stratified-v1:{seed}:{key}").sample(range(populations[key]), quotas[key]))
        for key in sorted(populations)
    }
    seen = Counter()
    cursors = Counter()
    rows: list[dict] = []
    selected_positions_hash = hashlib.sha256()
    source_offset = 0
    read_columns = [column for column in columns if column in available]
    for batch in parquet.iter_batches(batch_size=batch_size, columns=read_columns):
        mask, hours = _eligible_hours(batch, month)
        positions = pc.indices_nonzero(mask).to_pylist()
        selected = []
        for position, key in zip(positions, hours.to_pylist()):
            rank = seen[key]
            seen[key] += 1
            cursor = cursors[key]
            choices = selected_ranks[key]
            if cursor < len(choices) and rank == choices[cursor]:
                selected.append(position)
                cursors[key] += 1
        if selected:
            absolute_positions = array("Q", (source_offset + pos for pos in selected))
            if sys.byteorder == "little":
                absolute_positions.byteswap()
            selected_positions_hash.update(absolute_positions.tobytes())
            for row in batch.take(pa.array(selected, type=pa.int64())).to_pylist():
                normalized = normalize(row)
                if normalized is None:
                    raise ValueError("Arrow eligibility disagrees with B1 normalization")
                rows.append(normalized)
        source_offset += batch.num_rows
    if len(rows) != needed or dict(seen) != dict(populations):
        raise ValueError("Source population changed between sampling passes")
    final_stat = path.stat()
    if (initial_stat.st_size, initial_stat.st_mtime_ns) != (final_stat.st_size, final_stat.st_mtime_ns):
        raise ValueError("Source file changed during sampling")
    strata = [
        {"pickup_hour": datetime.fromtimestamp(key, tz=timezone.utc).strftime("%Y-%m-%d %H:00:00"),
         "source_rows": populations[key], "selected_rows": quotas[key]}
        for key in sorted(populations)
    ]
    return rows, {
        "method": "proportional_pickup_day_hour_without_replacement_v1",
        "seed": seed,
        "python_version": sys.version.split()[0],
        "pyarrow_version": pa.__version__,
        "allocation": "Hamilton largest remainder; ties by ascending pickup hour",
        "source_sha256": source_hash,
        "source_rows": parquet.metadata.num_rows,
        "eligible_rows": sum(populations.values()),
        "excluded_rows": parquet.metadata.num_rows - sum(populations.values()),
        "selected_rows": len(rows),
        "selected_source_positions_sha256": selected_positions_hash.hexdigest(),
        "position_digest_encoding": "source-order zero-based row positions as unsigned 64-bit big-endian integers",
        "eligibility": "non-null pickup/dropoff, finite non-null locations, finite fare > 0; pickup inside source month when inferred",
        "pickup_month": month[0].strftime("%Y-%m") if month else None,
        "strata": strata,
    }
