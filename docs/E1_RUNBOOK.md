# E1 — Chạy từ raw JSON đến Gold và benchmark

E1 ghép các module đã có theo đúng dependency; một stage chỉ được ghi PASS sau
khi các kiểm tra của stage đó hoàn tất. Manifest cuối cùng ở
[`docs/pipeline_run.json`](pipeline_run.json) là đầu mối đọc kết quả mới. Các
file `docs/evidence/c3/*` và `docs/evidence/merge_20260924/*` là evidence lịch sử.

Run nghiệm thu `official_20260924_verified` ngày 24/09/2026 đã PASS: 45,337 giây
sau Spark startup, Gold 152 nhóm `zone_hourofday` từ 2.983 chuyến; 36 lượt đo D2
chính và 18 lượt compaction. Bộ test riêng: 90 PASS trong 61,00 giây.

Đây là số liệu của fixture nhỏ đã lưu. Các run mở rộng mới đã PASS tới
3.000.000 dòng nguồn, với config/evidence tách riêng; xem
[runbook scaling](SCALING.md). Dùng hướng dẫn đó để lấy mẫu phân tầng, đặt
heap/shuffle và giữ nguyên kết quả các lần chạy trước.
Bộ regression hiện tại có **118 PASS**; [JUnit mới](evidence/scaling/tests.xml)
và [code hashes mới](evidence/scaling/validation.json) đi cùng bản mở rộng.
[Validation/code hashes](evidence/e1/validation.json) và
[manifest bất biến](evidence/e1/pipeline_run.json) giữ provenance của lần này.

## 1. Chuẩn bị

Làm các bước môi trường và tải/sinh dữ liệu trong [README](../README.md).
Cần Java 17, Python 3.11 trong `.venv`, PySpark/Delta 4.0.1 và ba batch JSON có
checksum khớp [`docs/checksums.txt`](checksums.txt). Generator không chạy lại
trong từng stage; JSON là đầu vào đã được đóng băng để so sánh chất lượng.

```bash
export JAVA_HOME="$PWD/data/.runtime/jdk-17.0.20.1+1/Contents/Home"
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
.venv/bin/python lakehouse_pipeline.py \
  --run-dir data/e1_runs/classroom_run \
  --output docs/pipeline_run.json
```

Thay JAVA_HOME nếu máy dùng JDK ở nơi khác. `classroom_run` phải là tên chưa
được dùng. Nếu không có `--run-dir`, runner tạo tên mới theo timestamp trong
`run_root`; nếu không có `--output`, dùng `manifest_output` của config.
Không chạy lại với cùng output directory để tránh ghi đè state/evidence cũ.

## 2. Config và giao diện module

Config mặc định là [`config/pipeline.json`](../config/pipeline.json).

| Key | Mục đích |
|---|---|
| `raw_dir`, `manifest`, `checksums` | Ba JSON đầu vào, contract và SHA-256 |
| `run_root`, `manifest_output` | Output các lần chạy và manifest E1 |
| `master` | Spark master local cho demo |
| `cdc_seed`, `cdc_updates`, `cdc_inserts` | Fixture deterministic, mặc định 42 / 5 / 3 |
| `evolution_rows` | Số dòng batch schema mới, mặc định 100 |
| `gold_grain` | `zone_hourofday` cho zone × giờ trong ngày UTC (0–23) |
| `benchmark_enabled` | Bật D2 trong run nghiệm thu E1 |
| `benchmark_iterations` | Ít nhất 3 lượt đo cho mỗi điều kiện |
| `benchmark_target_file_size` | Target size của bài thử local; được ghi cùng results |

Các entry point trả về `dict` có count/metrics để runner kiểm tra và ghi log:

```python
bronze.run(spark, months=None, overwrite=False, *, raw_dir=..., bronze_dir=...)
silver.build(spark, bronze_dir=..., silver_dir=..., rejected_dir=..., ...)
silver.apply_cdc(spark, silver_dir=..., cdc_path=..., ...)
silver.apply_evolution(spark, bronze_dir=..., silver_dir=..., batch_path=..., rows=...)
gold.run(spark, silver_dir=..., gold_dir=..., grain=..., source_version=..., ...)
```

Dùng đường dẫn tường minh khi chạy từng module. `bronze.run(overwrite=True)` bị từ chối để giữ Bronze append-only. Runner E1
luôn tạo thư mục mới và không reset Silver đã có C1/C2. Gold là derived output
nên có thể rebuild có kiểm soát; xem tham số overwrite của API trước khi ghi
vào một Gold directory đã tồn tại.

## 3. Invariant và output theo stage

| Stage | Điều kiện cần đúng ở run mới |
|---|---|
| B1 | SHA-256 ba JSON khớp; mỗi file 1.020 dòng |
| B2 | Bronze 3.060; cả ba metadata có giá trị ở mọi dòng |
| B3 | Silver 2.880 + rejected 180 = Bronze 3.060 |
| C1 | 5 matched update + 3 unmatched insert trong một MERGE; Silver 2.883 |
| C2 | Bronze 3.160 / Silver 2.983; dòng cũ NULL, 100 dòng mới có fee |
| C2 replay | Không đổi count/version khi batch ID và nội dung giống nhau |
| C3 | v0/v1 là hai version cũ đọc được; history có MERGE và schema evidence |
| VACUUM demo | Copy mất file cũ thật; current copy còn đọc được; original v0/v1/v2 nguyên vẹn |
| D1 | 152 nhóm zone × hour-of-day; SUM(trip_count)=2.983; khớp query SQL |
| D2 | Các layout cùng dữ liệu; query result giống nhau; 4 điều kiện × ≥3 lượt |
| E1 | Tất cả stage hoàn tất; manifest `status=passed`; có Gold và benchmark results |

B3 reject theo reason: `INVALID_PICKUP_DATETIME=30`, `MISSING_LOCATION=60`,
`INVALID_FARE=30`, `DUPLICATE_TRIP=60`. C2 append thêm 100 dòng hợp lệ không làm
tăng quarantine. Gold bucket count phụ thuộc grain; không so con số từ baseline
B3 với kết quả sau CDC/evolution.

Manifest chứa `paths`, `runtime`, các khối `b1` đến `d2`, `counts`, `stages`,
`elapsed_seconds` và trạng thái. Trong D2, `results.json` giữ 36 lượt đo của bộ
bốn layout và 18 lượt của phase compaction đối chiếu; mỗi query/layout có một
warmup không tính giờ. Hai phase có baseline riêng và không trộn median. Mỗi run còn giữ evidence dưới `<run-dir>/evidence/`
và các bảng thí nghiệm dưới `<run-dir>/benchmark/`. Đọc đường dẫn trong manifest
để tìm kết quả đúng thay vì mặc định `data/silver/taxi_trips`.

## 4. Cách kiểm tra sau chạy

```bash
.venv/bin/python - <<'PY'
import json
from pathlib import Path
r = json.loads(Path('docs/pipeline_run.json').read_text())
assert r['status'] == 'passed', r.get('status')
assert 'd1' in r and 'd2' in r
print('Scope:', r['scope'])
print('Runtime:', r['runtime'])
print('Counts:', r['counts'])
print('Outputs:', r['paths'])
print('Elapsed seconds:', r['elapsed_seconds'])
PY
```

Để xem lịch sử/giá trị trip được update và hiểu log, dùng
[C3_TIME_TRAVEL.md](C3_TIME_TRAVEL.md) với đường dẫn Silver của manifest mới.
Để xem tip% và earnings, đọc khối D1 cùng [D1_GOLD.md](D1_GOLD.md). Đọc D2 cùng
cache policy, cách lấy metrics và threats trong [D2_PERFORMANCE.md](D2_PERFORMANCE.md).
Không chỉ nhìn % improvement: file skipping, byte đọc và latency là các phép đo
khác nhau, và một layout có thể nhanh hơn ở query này nhưng chậm hơn ở query khác.

## 5. Khi chạy lỗi và giới hạn

Runner lưu tiến độ đã có; stage thất bại không được chuyển thành PASS. Giữ run
lỗi để chẩn đoán. Lỗi checksum cần kiểm lại nguồn/generator; lỗi JVM thường cần
kiểm JAVA_HOME và Python environment; lỗi thiếu file ở original là lỗi thật,
khác exception thiếu file có chủ đích ở bản VACUUM copy.

Sau sửa, dùng một run directory mới. E1 chưa cam kết resume tại chỗ hoặc
exactly-once đa writer. B2 append không idempotent khi cố ý gọi lại cùng bảng;
E2 cần kiểm replay toàn pipeline theo chính sách output mới. Tắt D2 để debug
các stage trước không đủ nghiệm thu E1 theo dependency mới.

Với khoảng 3.000 dòng, đây là demo correctness và cơ chế storage local. Target
file size nhỏ giúp quan sát layout; không phải bằng chứng có file ~1 GB, phân
tích đầy đủ ba tháng TLC hay bảo đảm cùng mức tăng tốc trên cluster production.
