# Delta Lakehouse — NYC Yellow Taxi

Checkpoint hiện tại: **đến C3**. Code được tích hợp trên branch
`feat/c3-time-travel-audit`, xuất phát từ `main@18ac8a0`.
B1–B3, CDC/MERGE, schema evolution và time travel có runner kiểm chứng;
Gold, performance benchmark và bài nộp cuối thuộc D1–F1 tiếp theo.

## Chạy nhanh

Môi trường: Python 3.11, Java 17, PySpark 4.0.1, Delta Lake 4.0.1.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
export JAVA_HOME="/duong/dan/toi/jdk-17"
export SPARK_LOCAL_IP=127.0.0.1
```

Trên máy đang làm project, JDK đã có sẵn:

```bash
export JAVA_HOME="$PWD/data/.runtime/jdk-17.0.20.1+1/Contents/Home"
export SPARK_LOCAL_IP=127.0.0.1
```

Dữ liệu local đã được tải và B1 đã sinh batch. Khi clone repo sang máy mới,
tải ba Parquet chính thức rồi sinh JSON (data không commit lên Git):

```bash
mkdir -p data/raw
for month in 2025-01 2025-02 2025-03; do
  curl --fail --location --output "data/raw/yellow_tripdata_${month}.parquet" \
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${month}.parquet"
done
.venv/bin/python -m src.generator --output-dir data/raw
```

Đối chiếu [nguồn và checksums](docs/source_data_manifest.md). Generator lấy
1.000 dòng sạch đầu tiên mỗi tháng và chèn lỗi có kiểm soát; các batch JSON
được runner kiểm SHA-256 trước khi ingest. Spark lần đầu cần mạng để lấy JAR
Delta tương ứng; runtime không trộn JAR Delta 3.x/Scala 2.12 với Spark 4.

```bash
.venv/bin/python lakehouse_pipeline.py
```

Mỗi lần gọi tạo một thư mục mới `data/c3_demo/<timestamp>/`. Có thể chỉ định
`--run-dir data/c3_demo/my_run`; thư mục đó phải chưa tồn tại. Runner thực hiện:

1. Kiểm B1 → Bronze 3.060 → Silver 2.880 + quarantine 180.
2. Sinh CDC seed 42 → một MERGE cập nhật 5, thêm 3 → Silver 2.883.
3. Append batch 04 có `surcharge_fee` vào hai tầng → Bronze 3.160, Silver 2.983.
4. Kiểm replay C2, đọc Silver v0/v1/v2, history và JSON log.
5. VACUUM trên bản copy mới, rồi xác nhận bảng gốc và các version cũ nguyên vẹn.

Đầu vào Silver cho D1 tiếp theo nằm ở `paths.silver` trong `pipeline_run.json`
của run mới. Run đã nghiệm thu dùng `data/c3_demo/official_20260915_final/silver`;
đường dẫn `data/silver/taxi_trips` cũ vẫn chỉ là baseline B3.

C3 chủ đích gây lỗi đọc version cũ **trên bản copy đã VACUUM** và lưu exception
thiếu file. Lỗi này được kiểm tra; chỉ khi tất cả điều kiện đạt mới in `[PASS]`.
Không chạy lại B3 trên một Silver đã có C1/C2. Bronze B2 là append-only;
không gọi B2 lần hai trên cùng bảng để “kiểm idempotency”.

## Kết quả và tài liệu

- [REPORT.md](REPORT.md): lý thuyết, kiến trúc, C1–C3 và khung D1–F1.
- [Demo C3](docs/C3_TIME_TRAVEL.md): lệnh đọc lịch sử và trình bày log/VACUUM.
- [C1](docs/C1_CDC.md), [C2](docs/C2_SCHEMA_EVOLUTION.md), [B3](docs/B3_SILVER.md).
- [Evidence tích hợp](docs/evidence/c3/pipeline_run.json), [audit](docs/evidence/c3/audit.json),
  [VACUUM](docs/evidence/c3/vacuum.json).
- [Ảnh kết quả](docs/evidence/c3/summary.png), [ảnh log có chú thích](docs/evidence/c3/delta_log.png).
- [Khung slide A1](docs/SLIDE_OUTLINE.md); PowerPoint cuối làm ở F1.

Evidence gắn với đúng lần chạy và phiên bản code trong
`docs/evidence/c3/manifest.json`. Các đường dẫn tuyệt đối trong JSON ghi lại
máy chạy; trên máy khác dùng thư mục run mới của mình.

## Kiểm thử

```bash
.venv/bin/python -m pytest -q tests
```

Test dùng bảng tạm riêng: B3 contract/ANSI/quarantine; C1 update/insert/replay,
schema thiếu cột, duplicate và invalid fare; C2 retry sau Bronze, replay,
nội dung batch bị đổi, invalid date/fee; guard đường dẫn VACUUM.
Kiểm chứng VACUUM thật và lịch sử xuyên C1/C2 nằm trong runner dữ liệu TLC.

## Cấu trúc

```text
config/pipeline.json       Các đường dẫn và kích thước demo
lakehouse_pipeline.py      Runner đến C3
src/generator.py           B1: Parquet -> dirty JSON
src/bronze.py              B2: append raw + metadata
src/silver.py              B3: typed Silver + quarantine
src/cdc_merge.py           C1: fixture + MERGE
src/schema_evolution.py    C2: append schema mới + replay checks
src/time_travel.py         C3: snapshot/log audit + VACUUM copy
src/delta_runtime.py       Spark/Delta runtime và helpers
scripts/render_c3_evidence.py  Xuất evidence và trang minh họa từ kết quả thật
tests/                    Regression tests
REPORT.md                 Báo cáo qua C3, khung phần còn lại
docs/                     Contracts, hướng dẫn, evidence
data/                     Dữ liệu local, không commit
```

## Quy ước làm việc

Tạo branch từ main mới nhất: `feat/<milestone>-<description>`; commit dùng
`feat(c3): ...`, `fix(c2): ...`, `docs: ...`. Ghi rõ command và evidence
trong PR. Không commit .venv/Parquet/Delta tables, không đưa số liệu run cũ
vào báo cáo như thể vừa xác minh. Các module trả dict để tiếp tục tích hợp E1;
runner hiện chưa phải pipeline JSON → Gold hoàn chỉnh.
