#!/usr/bin/env python3
"""Download and fingerprint every real data source used by the TLC lane.

What it does
------------
1. Downloads the six Yellow Taxi monthly Parquet files (2024 Q1 + 2025 Q1).
2. Downloads the taxi zone lookup and the MTA list of CBD taxi zones.
3. Optionally downloads TLC's monthly pickups-by-zone aggregate for reconciliation.
4. Writes docs/source_data_manifest.json + .md with rows, bytes, SHA-256 and
   the *physical* Parquet schema of every file, and a schema-drift table that
   shows which columns/types differ between files.

Re-running is safe: existing files are kept unless --force is given. If a
re-downloaded file has a different SHA-256 than the manifest, the manifest
records a REPUBLISHED event; Bronze will ingest it as a new batch and the
Silver MERGE treats it as a change feed.

Usage
-----
    python scripts/download_sources.py                     # all 6 months + reference files
    python scripts/download_sources.py --months 2025-01    # subset
    python scripts/download_sources.py --with-tlc-aggregates
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import (  # noqa: E402
    CBD_ZONES_URL, DEFAULT_MONTHS, TLC_ZONE_MONTHLY_URL, TRIP_URL_TEMPLATE,
    ZONE_LOOKUP_URL, get_paths, trip_file_name,
)

# Some open-data portals return 403 to non-browser user agents.
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/csv,application/json,application/geo+json,*/*;q=0.8",
}

# MTA CBD taxi zones (data.ny.gov yfdc-w5jh). Tried in order; the first that returns rows wins.
CBD_ZONES_CANDIDATE_URLS = [
    "https://data.ny.gov/resource/yfdc-w5jh.csv?$limit=5000",
    "https://data.ny.gov/resource/yfdc-w5jh.json?$limit=5000",
    CBD_ZONES_URL,
    "https://data.ny.gov/api/views/yfdc-w5jh/rows.json?accessType=DOWNLOAD",
    "https://data.ny.gov/api/geospatial/yfdc-w5jh?method=export&format=GeoJSON",
]
CBD_DATASET_PAGE = "https://data.ny.gov/Transportation/MTA-Central-Business-District-Taxi-Zones/yfdc-w5jh"
GEOMETRY_KEYS = {"the_geom", "geometry", "multipolygon", "geom", "shape"}


def download_file(url: str, dest: Path, force: bool = False, retries: int = 3) -> bool:
    """Stream a URL to disk atomically. Returns True when a download happened."""
    if dest.exists() and not force:
        print(f"  = keep    {dest.name} (exists; use --force to re-download)")
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(url, headers=REQUEST_HEADERS)
            with urllib.request.urlopen(request, timeout=120) as response, open(tmp, "wb") as out:
                shutil.copyfileobj(response, out, length=1 << 20)
            tmp.replace(dest)
            print(f"  + fetched {dest.name} ({dest.stat().st_size:,} bytes)")
            return True
        except Exception as exc:  # noqa: BLE001 - report and retry any network error
            print(f"  ! attempt {attempt}/{retries} failed for {url}: {exc}")
            time.sleep(2 * attempt)
    raise RuntimeError(f"Could not download {url}")


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def describe_parquet(path: Path) -> Dict[str, object]:
    """Read only the Parquet footer: row count and physical schema."""
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    schema = parquet.schema_arrow
    return {
        "rows": parquet.metadata.num_rows,
        "row_groups": parquet.metadata.num_row_groups,
        "created_by": parquet.metadata.created_by,
        "columns": {field.name: str(field.type) for field in schema},
    }


def build_schema_drift(files: Dict[str, Dict[str, object]]) -> List[Dict[str, object]]:
    """One row per column (case-insensitive), showing its type in every file."""
    by_column: Dict[str, Dict[str, str]] = {}
    display: Dict[str, str] = {}
    for month, info in files.items():
        for name, dtype in info["columns"].items():  # type: ignore[union-attr]
            key = name.lower()
            display.setdefault(key, name)
            by_column.setdefault(key, {})[month] = f"{name}:{dtype}"
    rows = []
    for key, per_month in sorted(by_column.items()):
        values = {m: per_month.get(m, "MISSING") for m in files}
        distinct = {v for v in values.values()}
        rows.append({"column": display[key], "drift": len(distinct) > 1, "per_file": values})
    return rows


def normalize_to_csv(downloaded: Path, dest_csv: Path) -> int:
    """Turn a CSV, JSON array, Socrata rows.json or GeoJSON download into a plain CSV.

    Geometry columns are dropped (only zone identifiers are needed). Returns the row count.
    """
    csv.field_size_limit(sys.maxsize)
    text = downloaded.read_text(encoding="utf-8-sig", errors="replace").lstrip()
    records: List[Dict[str, object]] = []
    if text.startswith("{"):
        payload = json.loads(text)
        if "features" in payload:                                   # GeoJSON
            records = [f.get("properties") or {} for f in payload["features"]]
        elif "meta" in payload and "data" in payload:               # Socrata rows.json
            names = [c.get("fieldName") or c.get("name") for c in payload["meta"]["view"]["columns"]]
            records = [dict(zip(names, row)) for row in payload["data"]]
    elif text.startswith("["):                                      # SODA JSON array
        records = json.loads(text)
    else:                                                           # already CSV
        if text.lower().startswith("<!doctype") or text.lower().startswith("<html"):
            return 0
        rows = list(csv.DictReader(text.splitlines()))
        records = rows
    records = [{k: v for k, v in r.items() if k and k.lower() not in GEOMETRY_KEYS and not str(k).startswith(":")}
               for r in records]
    records = [r for r in records if r]
    if not records:
        return 0
    header = sorted({k for r in records for k in r})
    dest_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(dest_csv, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for r in records:
            writer.writerow({k: ("" if v is None else v if not isinstance(v, (dict, list)) else json.dumps(v))
                             for k, v in r.items()})
    return len(records)


def fetch_cbd_zones_raw(dest_csv: Path, force: bool) -> Optional[str]:
    """Download the MTA CBD zone list, trying several endpoints. Returns the URL used or None."""
    if dest_csv.exists() and not force:
        print(f"  = keep    {dest_csv.name} (exists; use --force to re-download)")
        return "existing file"
    tmp = dest_csv.with_suffix(".download")
    for url in CBD_ZONES_CANDIDATE_URLS:
        try:
            download_file(url, tmp, force=True, retries=2)
        except RuntimeError:
            continue
        rows = normalize_to_csv(tmp, dest_csv)
        tmp.unlink(missing_ok=True)
        if rows:
            print(f"  + CBD zones: {rows} rows from {url}")
            return url
        print(f"  ! {url} returned no usable rows")
    return None


# Fallback when data.ny.gov is unreachable (it returns 403 to some networks/regions).
# CONFIRMED: `taxi_zone` values read from the official dataset (data.ny.gov yfdc-w5jh, rows.xml
#            export, retrieved 2026-09-24). That export was truncated after zone 232.
# INFERRED:  remaining taxi zones south of 60th St in Manhattan, by geography. NOT official;
#            gold.validate_cbd_zones() checks every zone against congestion fees in the trip data.
CBD_ZONES_CONFIRMED = (4, 12, 13, 45, 48, 50, 68, 79, 87, 88, 90, 100, 107, 113, 114, 125, 137, 144,
                       148, 158, 161, 162, 163, 164, 170, 186, 209, 211, 224, 229, 230, 231, 232)
CBD_ZONES_INFERRED = (233, 234, 246, 249, 261)


def write_fallback_cbd_zones(dest_csv: Path) -> Dict[str, object]:
    dest_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(dest_csv, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["LocationID", "source"])
        writer.writerows([[z, "mta_yfdc_w5jh_partial"] for z in CBD_ZONES_CONFIRMED])
        writer.writerows([[z, "inferred_geography"] for z in CBD_ZONES_INFERRED])
    print(f"  + wrote fallback {dest_csv.name}: {len(CBD_ZONES_CONFIRMED)} confirmed + "
          f"{len(CBD_ZONES_INFERRED)} inferred zones (validated later against trip fees)")
    return {"zones": len(CBD_ZONES_CONFIRMED) + len(CBD_ZONES_INFERRED),
            "method": "fallback list (33 official + 5 inferred)"}


def print_manual_cbd_instructions(dest_csv: Path) -> None:
    print(
        "\n  !! Could not download the MTA CBD zone list automatically.\n"
        f"     1. Open {CBD_DATASET_PAGE} in a browser\n"
        "     2. Export -> CSV (or GeoJSON) and save it as\n"
        f"        {dest_csv}\n"
        "     3. Re-run: python scripts/download_sources.py   (trip files are kept, not re-downloaded)\n"
        "     Meanwhile a built-in fallback list is used (33 official zones + 5 inferred).\n"
    )


# Candidate column names for the MTA CBD zone dataset. The dataset's exact
# header was not verifiable in advance, so we detect and fail loudly.
_ID_CANDIDATES = ("taxi_zone", "locationid", "location_id", "taxi_zone_id", "taxizone_id", "zone_id", "locid", "location")
_NAME_CANDIDATES = ("zone", "zone_name", "zonename")


def derive_cbd_zones(raw_csv: Path, zone_lookup_csv: Path, out_csv: Path) -> Dict[str, object]:
    """Turn the MTA CBD dataset into a clean `LocationID` list.

    Detection order: an ID-like column first, then a zone-name column mapped
    through the TLC lookup. If neither exists the header is printed and the
    function raises, so nobody silently runs the CBD analysis on a guess.
    """
    csv.field_size_limit(sys.maxsize)  # the_geom WKT column can be very long
    with open(raw_csv, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        header = reader.fieldnames or []
        rows = list(reader)
    lowered = {h.lower().strip(): h for h in header}

    ids: List[int] = []
    method = None
    for candidate in _ID_CANDIDATES:
        if candidate in lowered:
            column = lowered[candidate]
            for row in rows:
                value = (row.get(column) or "").strip()
                if value.replace(".0", "").isdigit():
                    ids.append(int(float(value)))
            if ids:
                method = f"id column '{column}'"
                break

    if not ids:
        with open(zone_lookup_csv, newline="", encoding="utf-8-sig") as handle:
            lookup = {r["Zone"].strip().lower(): int(r["LocationID"]) for r in csv.DictReader(handle)}
        for candidate in _NAME_CANDIDATES:
            if candidate in lowered:
                column = lowered[candidate]
                unmatched = []
                for row in rows:
                    name = (row.get(column) or "").strip().lower()
                    if name in lookup:
                        ids.append(lookup[name])
                    elif name:
                        unmatched.append(name)
                if ids:
                    method = f"name column '{column}' mapped via taxi_zone_lookup"
                    if unmatched:
                        print(f"  ! {len(unmatched)} CBD zone names not found in lookup: {unmatched[:5]}")
                    break

    if not ids:
        raise RuntimeError(
            "Could not detect a LocationID or zone-name column in the MTA CBD dataset.\n"
            f"Header was: {header}\n"
            "Add the right column name to _ID_CANDIDATES/_NAME_CANDIDATES and re-run."
        )

    unique_ids = sorted(set(ids))
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["LocationID"])
        writer.writerows([[i] for i in unique_ids])
    print(f"  + derived {out_csv.name}: {len(unique_ids)} CBD zones via {method}")
    return {"zones": len(unique_ids), "method": method, "raw_header": header}


def load_previous_manifest(path: Path) -> Dict[str, object]:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
    return {}


def render_manifest_markdown(manifest: Dict[str, object]) -> str:
    lines = [
        "# Source data manifest",
        "",
        f"Generated: {manifest['generated_at']}  ",
        "Source: NYC TLC Trip Record Data (official CloudFront links) and NY/NYC Open Data.",
        "",
        "## Trip files",
        "",
        "| Month | File | Rows | Bytes | Columns | SHA-256 |",
        "|---|---|---:|---:|---:|---|",
    ]
    for month, info in manifest["trip_files"].items():  # type: ignore[union-attr]
        lines.append(
            f"| {month} | `{info['file']}` | {info['rows']:,} | {info['bytes']:,} | "
            f"{len(info['columns'])} | `{info['sha256']}` |"
        )
    total = sum(i["rows"] for i in manifest["trip_files"].values())  # type: ignore[union-attr]
    lines += ["", f"Total trip rows: **{total:,}**", "", "## Schema drift between files", "",
              "Columns whose name or physical type differs between files. Bronze harmonises names and "
              "widens types; the drift itself is evidence for the report.", "",
              "| Column | " + " | ".join(manifest["trip_files"]) + " |",  # type: ignore[arg-type]
              "|---|" + "---|" * len(manifest["trip_files"])]  # type: ignore[arg-type]
    for row in manifest["schema_drift"]:  # type: ignore[union-attr]
        if row["drift"]:
            lines.append(f"| {row['column']} | " + " | ".join(row["per_file"].values()) + " |")
    if not any(r["drift"] for r in manifest["schema_drift"]):  # type: ignore[union-attr]
        lines.append("| (none) |" + " |" * len(manifest["trip_files"]))  # type: ignore[arg-type]
    lines += ["", "## Reference files", "", "| File | Bytes | SHA-256 | Note |", "|---|---:|---|---|"]
    for name, info in manifest["reference_files"].items():  # type: ignore[union-attr]
        lines.append(f"| `{name}` | {info['bytes']:,} | `{info['sha256']}` | {info.get('note', '')} |")
    events = manifest.get("events") or []
    lines += ["", "## Events", ""]
    lines += [f"- {e['type']}: {e['file']} ({e['detail']})" for e in events] or ["- none"]  # type: ignore[index]
    return "\n".join(lines) + "\n"


def main(argv: Optional[List[str]] = None) -> Dict[str, object]:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--months", nargs="+", default=DEFAULT_MONTHS)
    parser.add_argument("--force", action="store_true", help="re-download even if files exist")
    parser.add_argument("--with-tlc-aggregates", action="store_true",
                        help="also download TLC monthly pickups by zone (external reconciliation)")
    parser.add_argument("--skip-download", action="store_true",
                        help="only (re)build the manifest from files already on disk")
    args = parser.parse_args(argv)

    paths = get_paths()
    previous = load_previous_manifest(paths.source_manifest)
    previous_files = previous.get("trip_files", {}) if isinstance(previous, dict) else {}
    events: List[Dict[str, str]] = []

    print("[1/3] Trip files")
    trip_files: Dict[str, Dict[str, object]] = {}
    for month in sorted(args.months):
        dest = paths.raw_tlc_dir / trip_file_name(month)
        if not args.skip_download:
            download_file(TRIP_URL_TEMPLATE.format(month=month), dest, force=args.force)
        if not dest.exists():
            raise FileNotFoundError(dest)
        info = describe_parquet(dest)
        sha = compute_sha256(dest)
        old = previous_files.get(month) if isinstance(previous_files, dict) else None
        if old and old.get("sha256") != sha:
            events.append({"type": "REPUBLISHED", "file": dest.name,
                           "detail": f"sha256 {old['sha256'][:12]} -> {sha[:12]}; rows {old['rows']} -> {info['rows']}"})
        trip_files[month] = {
            "file": dest.name, "relative_path": str(dest.relative_to(paths.root)),
            "url": TRIP_URL_TEMPLATE.format(month=month),
            "bytes": dest.stat().st_size, "sha256": sha, **info,
        }

    print("[2/3] Reference files")
    ref = paths.raw_ref_dir
    zone_csv = ref / "taxi_zone_lookup.csv"
    cbd_raw = ref / "cbd_zones_raw.csv"
    cbd_csv = ref / "cbd_zones.csv"
    cbd_source: Optional[str] = "existing file" if cbd_raw.exists() else None
    if not args.skip_download:
        download_file(ZONE_LOOKUP_URL, zone_csv, force=args.force)
        cbd_source = fetch_cbd_zones_raw(cbd_raw, force=args.force)
    reference_files: Dict[str, Dict[str, object]] = {
        zone_csv.name: {"bytes": zone_csv.stat().st_size, "sha256": compute_sha256(zone_csv), "url": ZONE_LOOKUP_URL},
    }
    if cbd_source and cbd_raw.exists():
        if cbd_source == "existing file":
            # A manual browser export may be GeoJSON/JSON saved under the .csv name: normalise in place.
            tmp = cbd_raw.with_suffix(".manual")
            cbd_raw.replace(tmp)
            if not normalize_to_csv(tmp, cbd_raw):
                tmp.replace(cbd_raw)
            else:
                tmp.unlink()
        cbd_info = derive_cbd_zones(cbd_raw, zone_csv, cbd_csv)
        reference_files[cbd_raw.name] = {"bytes": cbd_raw.stat().st_size, "sha256": compute_sha256(cbd_raw),
                                         "url": cbd_source}
        reference_files[cbd_csv.name] = {"bytes": cbd_csv.stat().st_size, "sha256": compute_sha256(cbd_csv),
                                         "note": f"{cbd_info['zones']} zones, derived via {cbd_info['method']}"}
    else:
        print_manual_cbd_instructions(cbd_raw)
        cbd_info = write_fallback_cbd_zones(cbd_csv)
        events.append({"type": "CBD_FALLBACK", "file": cbd_csv.name,
                       "detail": "data.ny.gov unreachable; used built-in list (33 official + 5 inferred zones). "
                                 "See docs/evidence/cbd_zone_validation.md"})
        reference_files[cbd_csv.name] = {"bytes": cbd_csv.stat().st_size, "sha256": compute_sha256(cbd_csv),
                                         "note": cbd_info["method"]}

    print("[3/3] Optional external aggregates")
    if args.with_tlc_aggregates:
        agg = ref / "tlc_zone_monthly_raw.csv"
        if not args.skip_download:
            download_file(TLC_ZONE_MONTHLY_URL, agg, force=args.force)
        if agg.exists():
            with open(agg, newline="", encoding="utf-8-sig") as handle:
                header = next(csv.reader(handle))
            reference_files[agg.name] = {"bytes": agg.stat().st_size, "sha256": compute_sha256(agg),
                                         "url": TLC_ZONE_MONTHLY_URL, "note": f"header: {header}"}
    else:
        print("  - skipped (pass --with-tlc-aggregates to enable)")

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "months": sorted(args.months),
        "trip_files": trip_files,
        "schema_drift": build_schema_drift(trip_files),
        "reference_files": reference_files,
        "events": events,
    }
    paths.source_manifest.parent.mkdir(parents=True, exist_ok=True)
    paths.source_manifest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    paths.source_manifest.with_suffix(".md").write_text(render_manifest_markdown(manifest), encoding="utf-8")
    total = sum(int(i["rows"]) for i in trip_files.values())
    print(f"\nManifest written: {paths.source_manifest} ({len(trip_files)} files, {total:,} rows)")
    drift = [r["column"] for r in manifest["schema_drift"] if r["drift"]]
    print(f"Columns with drift across files: {drift or 'none'}")
    if not cbd_csv.exists():
        print("WARNING: data/raw/ref/cbd_zones.csv is missing (see instructions above).")
    return manifest


if __name__ == "__main__":
    main()
