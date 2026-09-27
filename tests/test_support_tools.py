"""Scan-metric extraction, source manifest helpers and _delta_log parsing (no Delta needed)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from optimization_benchmark import collect_scan_metrics, summarize_runs, time_query
from scripts.download_sources import build_schema_drift, derive_cbd_zones, describe_parquet
from src.delta_utils import find_schema_changes, summarize_add_actions
from tests.tlc_like import build_dataset


# ---------------------------------------------------------------- scan metrics
def test_scan_metrics_reflect_file_pruning(spark, tmp_path):
    path = str(tmp_path / "zones")
    df = spark.range(0, 4000).selectExpr("id", "CAST(id % 8 AS INT) AS PULocationID", "CAST(id AS DOUBLE) AS fare_amount",
                                         "CAST(1.0 AS DOUBLE) AS tip_amount")
    df.write.partitionBy("PULocationID").parquet(path)
    full = spark.read.parquet(path).groupBy().count()
    full.collect()
    one = spark.read.parquet(path).filter("PULocationID = 3").groupBy().count()
    one.collect()
    all_files, pruned = collect_scan_metrics(full), collect_scan_metrics(one)
    assert all_files["scan_nodes"] >= 1 and all_files["files_scanned"] >= 8
    assert 1 <= pruned["files_scanned"] < all_files["files_scanned"]
    assert 0 < pruned["bytes_read"] < all_files["bytes_read"]


def test_time_query_and_summary(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.range(100).selectExpr("CAST(id % 3 AS INT) AS PULocationID", "CAST(id AS DOUBLE) AS fare_amount",
                                "CAST(0.5 AS DOUBLE) AS tip_amount", "CAST(1 AS INT) AS payment_type",
                                "TIMESTAMP'2025-02-11 10:00:00' AS tpep_pickup_datetime").write.parquet(path)
    from optimization_benchmark import QUERIES
    runs = []
    for round_no in range(4):
        for variant in ("baseline", "other"):
            m = time_query(spark, path, QUERIES["q1_zone"][1], fmt="parquet")
            runs.append({"round": round_no, "warmup": round_no == 0, "query": "q1_zone", "variant": variant, **m})
    for q in ("q2_zone_week", "q3_control"):
        for round_no in range(4):
            for variant in ("baseline", "other"):
                m = time_query(spark, path, QUERIES[q][1], fmt="parquet")
                runs.append({"round": round_no, "warmup": round_no == 0, "query": q, "variant": variant, **m})
    summary = summarize_runs(runs, ["baseline", "other"])
    assert summary["per_query"]["q1_zone"]["baseline"]["runs"] == 3          # warm-up excluded
    assert summary["results_identical_across_variants"]["q1_zone"] is True
    assert summary["per_query"]["q1_zone"]["baseline"]["improvement_pct"] == 0.0


# ---------------------------------------------------------------- source manifest helpers
def test_describe_parquet_and_schema_drift(tmp_path):
    info = build_dataset(tmp_path, ["2024-01", "2025-01"], clean=40)
    raw = tmp_path / "data/raw/tlc"
    files = {m: describe_parquet(raw / f"yellow_tripdata_{m}.parquet") for m in ("2024-01", "2025-01")}
    assert files["2024-01"]["rows"] == info["manifest"]["2024-01"]["rows"]
    drift = {r["column"]: r for r in build_schema_drift(files)}
    assert drift["cbd_congestion_fee"]["drift"] and drift["cbd_congestion_fee"]["per_file"]["2024-01"] == "MISSING"
    assert drift["passenger_count"]["drift"]                        # double vs int64
    assert drift["airport_fee"]["drift"] or drift.get("Airport_fee", {}).get("drift")  # name case differs
    assert not drift["trip_distance"]["drift"]


def test_derive_cbd_zones_by_id_or_name(tmp_path):
    lookup = tmp_path / "lookup.csv"
    lookup.write_text("LocationID,Borough,Zone,service_zone\n161,Manhattan,Midtown Center,Yellow Zone\n"
                      "230,Manhattan,Times Sq/Theatre District,Yellow Zone\n1,EWR,Newark Airport,EWR\n")
    by_id = tmp_path / "by_id.csv"
    by_id.write_text("the_geom,LocationID,zone\n\"MULTIPOLYGON(...)\",161,Midtown Center\n\"MULTIPOLYGON(...)\",230,x\n")
    out = tmp_path / "cbd.csv"
    assert derive_cbd_zones(by_id, lookup, out)["zones"] == 2
    assert out.read_text().split() == ["LocationID", "161", "230"]
    by_name = tmp_path / "by_name.csv"
    by_name.write_text("the_geom,Zone\n\"MULTIPOLYGON(...)\",Midtown Center\n\"MULTIPOLYGON(...)\",Times Sq/Theatre District\n")
    assert derive_cbd_zones(by_name, lookup, out)["zones"] == 2
    bad = tmp_path / "bad.csv"
    bad.write_text("the_geom,shape_area\nx,1\n")
    with pytest.raises(RuntimeError, match="Header was"):
        derive_cbd_zones(bad, lookup, out)


# ---------------------------------------------------------------- _delta_log parsing
def _write_commit(log: Path, version: int, actions):
    log.mkdir(parents=True, exist_ok=True)
    (log / f"{version:020d}.json").write_text("\n".join(json.dumps(a) for a in actions) + "\n")


def _schema(*names):
    return json.dumps({"type": "struct", "fields": [{"name": n, "type": "double", "nullable": True, "metadata": {}}
                                                   for n in names]})


def test_find_schema_changes_and_add_stats(tmp_path):
    log = tmp_path / "table" / "_delta_log"
    _write_commit(log, 0, [{"commitInfo": {"operation": "WRITE"}}, {"metaData": {"schemaString": _schema("a", "b")}},
                           {"add": {"path": "p0.parquet", "size": 10, "dataChange": True,
                                    "stats": json.dumps({"numRecords": 5, "minValues": {"a": 1}, "maxValues": {"a": 9}})}}])
    _write_commit(log, 1, [{"commitInfo": {"operation": "WRITE"}},
                           {"add": {"path": "p1.parquet", "size": 10, "dataChange": True}}])
    _write_commit(log, 2, [{"commitInfo": {"operation": "MERGE"}},
                           {"metaData": {"schemaString": _schema("a", "b", "cbd_congestion_fee")}}])
    changes = find_schema_changes(str(tmp_path / "table"))
    assert changes[0]["event"] == "CREATE"
    assert changes[1] == {"version": 2, "operation": "MERGE", "event": "SCHEMA_CHANGE",
                          "added": ["cbd_congestion_fee"], "removed": [], "retyped": []}
    summary = summarize_add_actions(str(tmp_path / "table"), 0)
    assert summary["sample_add_actions"][0]["stats"]["maxValues"]["a"] == 9


# ---------------------------------------------------------------- CBD download fallbacks
def test_normalize_to_csv_handles_portal_formats(tmp_path):
    from scripts.download_sources import normalize_to_csv

    geojson = tmp_path / "a.download"
    geojson.write_text(json.dumps({"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]},
         "properties": {"locationid": "161", "zone": "Midtown Center"}}]}))
    out = tmp_path / "a.csv"
    assert normalize_to_csv(geojson, out) == 1
    assert out.read_text().splitlines()[0] == "locationid,zone"

    soda = tmp_path / "b.download"
    soda.write_text(json.dumps([{"location_id": "230", "the_geom": {"type": "MultiPolygon"}}]))
    assert normalize_to_csv(soda, tmp_path / "b.csv") == 1
    assert "the_geom" not in (tmp_path / "b.csv").read_text()

    rows_json = tmp_path / "c.download"
    rows_json.write_text(json.dumps({"meta": {"view": {"columns": [{"fieldName": ":sid"}, {"fieldName": "LocationID"}]}},
                                     "data": [[1, "162"], [2, "163"]]}))
    assert normalize_to_csv(rows_json, tmp_path / "c.csv") == 2
    assert (tmp_path / "c.csv").read_text().split() == ["LocationID", "162", "163"]

    html = tmp_path / "d.download"
    html.write_text("<!DOCTYPE html><html>blocked</html>")
    assert normalize_to_csv(html, tmp_path / "d.csv") == 0


def test_cbd_fetch_falls_back_then_gives_up_cleanly(tmp_path, monkeypatch):
    import scripts.download_sources as ds

    calls = []

    def fake_download(url, dest, force=False, retries=3):
        calls.append(url)
        if "resource/yfdc-w5jh.json" in url:
            dest.write_text(json.dumps([{"locationid": "161"}]))
            return True
        raise RuntimeError("403")

    monkeypatch.setattr(ds, "download_file", fake_download)
    dest = tmp_path / "cbd_zones_raw.csv"
    assert "resource/yfdc-w5jh.json" in ds.fetch_cbd_zones_raw(dest, force=False)
    assert len(calls) == 2 and dest.read_text().split() == ["locationid", "161"]

    monkeypatch.setattr(ds, "download_file", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("403")))
    assert ds.fetch_cbd_zones_raw(tmp_path / "other.csv", force=False) is None


def test_real_mta_header_taxi_zone_is_an_id(tmp_path):
    lookup = tmp_path / "lookup.csv"
    lookup.write_text("LocationID,Borough,Zone,service_zone\n161,Manhattan,Midtown Center,Yellow Zone\n")
    raw = tmp_path / "raw.csv"
    raw.write_text('taxi_zone,polygon\n4,"POLYGON ((...))"\n161,"POLYGON ((...))"\n')
    info = derive_cbd_zones(raw, lookup, tmp_path / "cbd.csv")
    assert info["zones"] == 2 and "taxi_zone" in info["method"]


def test_fallback_cbd_list_has_provenance(tmp_path):
    from scripts.download_sources import CBD_ZONES_CONFIRMED, CBD_ZONES_INFERRED, write_fallback_cbd_zones

    out = tmp_path / "cbd_zones.csv"
    assert write_fallback_cbd_zones(out)["zones"] == 38
    rows = out.read_text().splitlines()
    assert rows[0] == "LocationID,source"
    assert sum(r.endswith("mta_yfdc_w5jh_partial") for r in rows) == len(CBD_ZONES_CONFIRMED) == 33
    assert sum(r.endswith("inferred_geography") for r in rows) == len(CBD_ZONES_INFERRED) == 5
    assert not set(CBD_ZONES_CONFIRMED) & set(CBD_ZONES_INFERRED)
