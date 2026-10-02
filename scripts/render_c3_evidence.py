"""Export compact evidence and readable HTML views of a completed C3 run.

HTML contains actual recorded values/log actions, not simulated terminal output.
No Spark work is needed. Open the pages in a browser to capture PNG screenshots.
"""

import argparse
import hashlib
import html
import json
from pathlib import Path
import shutil
import subprocess
import xml.etree.ElementTree as ET


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def dump(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def pretty(value):
    return html.escape(json.dumps(value, indent=2, ensure_ascii=False, default=str))


def page(title, subtitle, content):
    return f'''<!doctype html><html lang="vi"><meta charset="utf-8"><title>{title}</title>
<style>
*{{box-sizing:border-box}}body{{margin:0;background:#edf2f7;color:#102435;font:17px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}}
main{{max-width:1360px;margin:auto;padding:35px 42px}}h1{{font-size:34px;line-height:1.2;margin:8px 0}}h2{{font-size:21px;margin:0 0 10px}}
.eyebrow{{font-size:13px;letter-spacing:2px;color:#176459;font-weight:700}}.sub{{color:#4c6274;margin:10px 0 24px}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:20px}}.card{{background:white;padding:23px;border-radius:14px;border:1px solid #dce5ec;margin-bottom:20px;overflow-wrap:anywhere}}
.timeline{{display:flex;align-items:center;gap:16px;margin:20px 0}}.step{{flex:1;background:#103949;color:white;padding:20px;border-radius:12px}}.step b{{font-size:32px;display:block}}
.arrow{{color:#527884;font-size:30px}}table{{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}}th,td{{text-align:left;padding:9px 10px;border-bottom:1px solid #e1e8ec}}th{{font-size:14px;color:#436071}}
pre{{background:#112532;color:#d8eeeb;border-radius:8px;padding:18px;font:13px/1.45 Menlo,Consolas,monospace;white-space:pre-wrap;overflow-wrap:anywhere;margin:12px 0}}
.good{{color:#006951;font-weight:700}}.note{{background:#fff5df;border-left:4px solid #c68415;padding:14px;margin:12px 0}}.small{{font-size:13px;color:#516877}}a{{color:#176b8a}}p{{margin:9px 0}}code{{font-size:0.9em}}
</style><main><div class="eyebrow">NYC TLC · LAKEHOUSE MIDTERM · CHECKPOINT C3</div>
<h1>{title}</h1><p class="sub">{subtitle}</p>{content}</main></html>'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--tests", type=Path, help="JUnit XML from the regression run")
    args = parser.parse_args()
    source, output = args.source.resolve(), args.output.resolve()
    run = read(source / "pipeline_run.json")
    if run["status"] != "passed":
        raise ValueError("Only a completed passing run can be exported")
    output.mkdir(parents=True, exist_ok=False)
    files = ["pipeline_run.json", "audit.json", "vacuum.json", "cdc_fixture.json", "bronze_history.json", "silver_history.json"]
    for name in files:
        shutil.copy2(source / name, output / name)
    shutil.copytree(source / "delta_log", output / "delta_log")
    audit, fixture = read(output / "audit.json"), read(output / "cdc_fixture.json")
    history = read(output / "silver_history.json")
    vacuum = read(output / "vacuum.json")
    stages = [(key, audit["snapshots"][key]) for key in ("baseline", "merge", "evolution")]
    timeline = '<div class="arrow">→</div>'.join(
        f'<div class="step">Silver v{snap["version"]} · {label}<b>{snap["count"]:,}</b>dòng</div>'
        for label, (_, snap) in zip(("B3", "MERGE", "Schema evolution"), stages))
    changed = fixture["updates"][0]
    amount_rows = "".join(f'<tr><td>{name}</td><td>{changed["before"][name]}</td><td>{changed["after"][name]}</td></tr>'
                          for name in ("fare_amount", "tip_amount", "total_amount"))
    history_rows = "".join(f'<tr><td>{row["version"]}</td><td>{row["operation"]}</td><td>{html.escape(str(row["timestamp"]))}</td></tr>'
                           for row in history)
    stats = audit["data_skipping"]
    content = f'''<div class="timeline">{timeline}</div><div class="grid">
<div class="card"><h2>B1 → B3: đối chiếu dữ liệu</h2><p>Bronze <b>{run["b2"]["bronze_count"]:,}</b> = Silver <b>{run["b3"]["silver_count"]:,}</b> + quarantine <b>{run["b3"]["rejected_count"]}</b>.</p>
<p class="small">3 batch từ TLC Jan–Mar 2025; SHA-256 khớp B1 contract. 30 lỗi ngày · 60 location · 30 fare · 60 duplicate.</p>
<h2>C1: {run["c1"]["updated_count"]} update + {run["c1"]["inserted_count"]} insert</h2><p class="small">Trip thật: {changed["trip_id"]} · cùng một MERGE</p>
<table><tr><th>Cột</th><th>v0</th><th>v1 / v2</th></tr>{amount_rows}</table></div>
<div class="card"><h2>C2: surcharge_fee mới</h2><table><tr><th>Bảng</th><th>Old NULL</th><th>New non-NULL</th></tr>
<tr><td>Bronze v{run["c2"]["evidence"]["BRONZE"]["latest_version"]}</td><td>{run["c2"]["evidence"]["BRONZE"]["null_rows"]:,}</td><td>{run["c2"]["evidence"]["BRONZE"]["not_null_rows"]}</td></tr><tr><td>Silver v{run["c2"]["evidence"]["SILVER"]["latest_version"]}</td><td>{run["c2"]["evidence"]["SILVER"]["null_rows"]:,}</td><td>{run["c2"]["evidence"]["SILVER"]["not_null_rows"]}</td></tr></table>
<p>Append + mergeSchema, không rebuild. Replay cùng batch: 0 dòng mới, version giữ nguyên.</p>
<h2>DESCRIBE HISTORY · Silver</h2><table><tr><th>Version</th><th>Operation</th><th>Timestamp UTC</th></tr>{history_rows}</table>
<p class="small">Schema change = WRITE + metaData.schemaString trong JSON v2.</p></div></div>
<div class="card"><h2>C3: VACUUM chỉ trên bản sao vật lý</h2><div class="grid"><div>
<p>Đã xóa <b>{len(vacuum["deleted_data_files"])} file Parquet obsolete</b>. Bản copy v{vacuum["copy_old_version"]} lỗi thiếu file như dự kiến; current vẫn {vacuum["copy_latest_count"]:,} dòng.</p>
<div class="note">RETAIN 0 + retentionDurationCheck=false là hack demo-only. Cấu hình đã được khôi phục.</div></div><div>
<p class="good">PASS · Bảng gốc v0, v1, v2 vẫn đọc đủ dữ liệu</p><p>Hash toàn bộ file gốc và fingerprint từng snapshot không đổi.</p>
<p class="small">Audit đọc toàn bộ giá trị, không chỉ count từ metadata. Run: {run["elapsed_seconds"]} giây; Spark {run["runtime"]["spark"]}, Delta {run["runtime"]["delta_spark"]}.</p></div></div></div>
<p class="small">Nguồn: <a href="pipeline_run.json">pipeline_run.json</a> · <a href="audit.json">audit.json</a> · <a href="vacuum.json">vacuum.json</a>. Bản trình bày từ run thật, không phải terminal giả. D1–F1 chưa thuộc checkpoint này.</p>'''
    (output / "summary.html").write_text(page("Time Travel & Audit — kết quả kiểm chứng", run["started_at_utc"], content), encoding="utf-8")
    merge_version, evolution_version = audit["versions"]["merge"], audit["versions"]["evolution"]
    merge_path = output / "delta_log" / "silver" / f"{merge_version:020d}.json"
    evolution_path = output / "delta_log" / "silver" / f"{evolution_version:020d}.json"
    merge = [json.loads(line) for line in merge_path.read_text().splitlines()]
    evolution = [json.loads(line) for line in evolution_path.read_text().splitlines()]
    commit = next(action["commitInfo"] for action in merge if "commitInfo" in action)
    addition = next(action["add"] for action in evolution if "add" in action)
    metadata = next(action["metaData"] for action in evolution if "metaData" in action)
    field = next(field for field in json.loads(metadata["schemaString"])["fields"] if field["name"] == "surcharge_fee")
    excerpt = {"commitInfo": {key: commit[key] for key in ("operation", "readVersion", "isolationLevel")},
               "action_counts": audit["log_summary"]["silver"][1]["action_counts"]}
    stats_excerpt = {"numRecords": stats["numRecords"], "minValues": {"PULocationID": stats["minValues"]["PULocationID"]},
                     "maxValues": {"PULocationID": stats["maxValues"]["PULocationID"]},
                     "nullCount": {"PULocationID": stats["nullCount"]["PULocationID"]}}
    log_content = f'''<div class="grid"><div class="card"><h2>1 · MERGE tạo snapshot mới</h2><p class="small">Silver / {merge_path.name}</p><pre>{pretty(excerpt)}</pre>
<p><b>add</b> đưa file mới vào snapshot; <b>remove</b> là tombstone, không xóa vật lý ngay. Xem số action thực tế trong khung trên.</p>
<p class="small">SHA-256: {hashlib.sha256(merge_path.read_bytes()).hexdigest()}</p></div>
<div class="card"><h2>2 · C2 ghi schema cùng batch append</h2><p class="small">Silver / {evolution_path.name}</p>
<pre>{pretty({"metaData.schemaString.fields": [field], "add.dataChange": addition["dataChange"], "action_counts": audit["log_summary"]["silver"][-1]["action_counts"]})}</pre>
<p>History ghi <b>WRITE</b>; field mới trong <b>metaData</b> chứng minh schema evolution. Không có remove: file cũ giữ nguyên.</p>
<p class="small">SHA-256: {hashlib.sha256(evolution_path.read_bytes()).hexdigest()}</p></div></div>
<div class="card"><h2>3 · add.stats: min/max là cơ sở data skipping</h2><p class="small">File thật: {html.escape(addition["path"])}</p><div class="grid"><pre>{pretty(stats_excerpt)}</pre><div>
<p>Predicate minh họa: <b>{html.escape(stats["example_predicate"])}</b>.</p>
<p>{stats["maxValues"]["PULocationID"] + 1} nằm ngoài [{stats["minValues"]["PULocationID"]}, {stats["maxValues"]["PULocationID"]}], nên file này không thể chứa dòng khớp. Audit đã đọc Parquet tương ứng để kiểm tra.</p>
<div class="note">Đây là bound cấp file, không phải B-tree. Chưa đo files_scanned, bytes_read hoặc tốc độ — các phép đo đó thuộc D2.</div>
<p class="small">Trong log gốc, stats/schemaString là chuỗi JSON. Trang này parse và trích trường để chú thích; không sửa file gốc.</p></div></div></div>
<p class="small">Xem nguyên bản: <a href="delta_log/silver/{merge_path.name}">JSON v{merge_version}</a> · <a href="delta_log/silver/{evolution_path.name}">JSON v{evolution_version}</a> · <a href="audit.json">audit.json</a>. Các số và hash lấy từ run đã PASS.</p>'''
    (output / "delta_log.html").write_text(page("Đọc một Delta commit — log thật, chú thích rõ", "MERGE v1 · Schema evolution v2 · Snapshot và data skipping", log_content), encoding="utf-8")
    repo = Path(__file__).resolve().parents[1]
    code_paths = [repo / "lakehouse_pipeline.py", repo / "config/pipeline.json", *sorted((repo / "src").glob("*.py")),
                  *sorted((repo / "tests").glob("*.py")), Path(__file__).resolve(), repo / "requirements.txt", repo / "requirements-dev.txt"]
    test_summary = None
    if args.tests:
        suites = ET.parse(args.tests).getroot().findall(".//testsuite")
        test_summary = {name: sum(int(suite.attrib.get(name, 0)) for suite in suites)
                        for name in ("tests", "failures", "errors", "skipped")}
        if not test_summary["tests"] or any(test_summary[name] for name in ("failures", "errors", "skipped")):
            raise ValueError("Expected a complete passing regression run")
        shutil.copy2(args.tests, output / "tests.xml")
    manifest = {"base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
                "branch": subprocess.check_output(["git", "branch", "--show-current"], cwd=repo, text=True).strip(),
                "note": "Working-tree source hashes identify the code tested; base_commit is not a claim these edits were committed.",
                "source_evidence": str(source), "tests": test_summary, "source_sha256": {str(path.relative_to(repo)): hashlib.sha256(path.read_bytes()).hexdigest() for path in code_paths},
                "evidence_sha256": {str(path.relative_to(output)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(output.rglob("*")) if path.is_file()}}
    dump(output / "manifest.json", manifest)
    print(output)


if __name__ == "__main__":
    main()
