# Delta Lakehouse — NYC Yellow Taxi

Pipeline của bài midterm Big Data: **A1–E1**, từ ba batch JSON qua Bronze,
Silver, CDC, schema evolution, time travel, Gold và Performance Lab. Các output
được tạo trong một thư mục mới cho mỗi lần chạy. Xem
[bảng nghiệm thu](docs/MILESTONE_STATUS.md) và [runbook E1](docs/E1_RUNBOOK.md)
để phân biệt kết quả đã kiểm chứng với E2/F1 còn ngoài phạm vi.

Run fixture nghiệm thu ngày 24/09/2026 đã **PASS A1–E1**: Bronze 3.160 và Silver 2.983
sau C1/C2; Gold 152 nhóm giờ trong ngày, bao phủ đủ 2.983 chuyến; D2 đủ bốn
layout và compaction control. **90 test pass**. Xem
[evidence và code hashes](docs/evidence/e1/validation.json).

Bộ mở rộng đã **PASS B1–E1 ở 300.000, 1.020.000 và 3.000.000 dòng nguồn**.
Mức lớn nhất tạo 3.060.000 dòng raw, 2.880.103 dòng Silver cuối cùng và 5.680
nhóm Gold; toàn pipeline mất 353,568 giây, RSS đỉnh quan sát 3,24 GiB.
Cả 270 lượt đo ở ba mức đều khớp kiểm tra độc lập bằng PyArrow/Decimal.
Bộ regression hiện tại đạt **118/118 test**, kể cả lấy mẫu và fingerprint mới.
Xem [lấy mẫu, cấu hình, tài nguyên và benchmark](docs/SCALING.md),
[evidence mới](docs/evidence/scaling/validation.json).
Generator hỗ trợ `--sampling stratified` để lấy mẫu theo ngày/giờ trong từng
tháng. Mặc định vẫn là fixture nhỏ để chạy demo nhanh; không ghi đè fixture
hoặc thay số liệu lịch sử bằng kết quả của bộ lớn hơn.

## Môi trường

Python 3.11, Java 17, PySpark 4.0.1, Delta Lake 4.0.1:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
export JAVA_HOME="/duong/dan/toi/jdk-17"
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
```

Trên máy đang làm project, JDK đã có sẵn ở:

```bash
export JAVA_HOME="$PWD/data/.runtime/jdk-17.0.20.1+1/Contents/Home"
```

Spark lần đầu cần mạng để resolve JAR Delta tương ứng. Không trộn JAR Delta
3.x/Scala 2.12 với Spark 4. Môi trường Python được ghim ở
[requirements.txt](requirements.txt); bộ test thêm ở
[requirements-dev.txt](requirements-dev.txt).

## Chuẩn bị dữ liệu một lần

Ba Parquet TLC chính thức và các JSON đã có trên máy làm project. Khi clone
sang máy mới, tải dữ liệu và tạo batch bằng:

```bash
mkdir -p data/raw
for month in 2025-01 2025-02 2025-03; do
  curl --fail --location --output "data/raw/yellow_tripdata_${month}.parquet" \
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${month}.parquet"
done
.venv/bin/python -m src.generator --output-dir data/raw
```

Đối chiếu [nguồn và SHA-256](docs/source_data_manifest.md). B1 lấy 1.000 dòng
hợp lệ đầu tiên của mỗi file rồi chèn lỗi có kiểm soát, tạo tổng 3.060 JSON
records. Đây là fixture để đối chiếu chính xác chất lượng dữ liệu; không phải
mẫu ngẫu nhiên đại diện cho tất cả chuyến taxi NYC. Các Parquet/Delta, runtime
và log đầy đủ nằm trong `data/`, không commit lên Git.

## Chạy từ đầu tới E1 bằng một lệnh

```bash
.venv/bin/python lakehouse_pipeline.py
```

Hoặc đặt tên output rõ ràng:

```bash
.venv/bin/python lakehouse_pipeline.py \
  --run-dir data/e1_runs/my_run \
  --output docs/pipeline_run.json
```

`--run-dir` phải chưa tồn tại. Config mặc định là
[config/pipeline.json](config/pipeline.json). Runner thực hiện:

1. Kiểm checksum B1; ingest Bronze **3.060** dòng cùng đủ ba cột metadata.
2. Làm sạch theo đúng bốn rule B1: **2.880** Silver + **180** quarantine.
3. Một CDC MERGE với seed 42: **5 update + 3 insert**, Silver thành **2.883**.
4. Thêm 100 dòng có `surcharge_fee`: Bronze **3.160**, Silver **2.983**; kiểm C2 replay.
5. Đọc các version cũ, lịch sử và log; VACUUM bản copy, xác nhận bảng gốc nguyên vẹn.
6. Rebuild Gold theo zone × giờ trong ngày (0–23, Spark session UTC) từ Silver sau C1/C2;
   đối chiếu aggregation bằng SQL.
7. Benchmark các bản sao Silver: baseline, Z-ORDER 1 cột, Z-ORDER 2 cột,
   liquid clustering + OPTIMIZE FULL; có nhánh compaction bổ sung.
8. Ghi log từng bước và xuất [docs/pipeline_run.json](docs/pipeline_run.json).

Lần chạy thành công phải có `status: passed` trong JSON và log kết thúc
`[PASSED]`. Chi tiết đầu vào, bảng Delta, runtime, từng stage, Gold và benchmark
nằm trong manifest. Không coi việc import được script hoặc có một số stage
PASS là nghiệm thu E1. [Runbook](docs/E1_RUNBOOK.md) giải thích cách kiểm tra.

C3 cố ý kiểm chứng lỗi đọc version cũ **ở bản copy đã VACUUM**; lỗi thiếu file
này được phân loại và lưu trong evidence. Bảng gốc vẫn phải đọc được cả ba
version. `retentionDurationCheck=false` chỉ là hack demo trong phạm vi copy.
Bronze B2 append-only; không gọi B2 lần hai trên cùng bảng để kiểm idempotency.

## Chạy riêng Gold và benchmark

Dùng `paths.silver` của run vừa hoàn tất; không dùng Silver B3 cũ nếu cần kết quả
sau C1/C2:

```bash
.venv/bin/python -m src.gold \
  --silver-dir data/e1_runs/my_run/silver \
  --gold-dir data/gold/standalone_run \
  --grain zone_hourofday \
  --output data/gold/standalone_run.json

.venv/bin/python optimization_benchmark.py \
  --silver-dir data/e1_runs/my_run/silver \
  --output-dir data/benchmark/standalone_run \
  --iterations 3 \
  --target-file-size 32768
```

Chọn output benchmark mới và tách khỏi đầu vào. Median latency, số file thực
sự scan và số byte đọc phải được diễn giải cùng cache policy, file size và các
giới hạn trong [D2 Performance](docs/D2_PERFORMANCE.md). Kết quả chậm hơn baseline
vẫn phải được ghi trung thực.

## Kiểm thử và quy ước làm việc

```bash
.venv/bin/python -m pytest -q
```

Bộ test của các module không tự thay thế E2: mốc E2 yêu cầu chạy lại toàn
pipeline và đối chiếu kết quả. F1 còn cần deck hoàn chỉnh và rehearsal 10–15 phút.
Khung slide A1 đã có ở [SLIDE_OUTLINE.md](docs/SLIDE_OUTLINE.md).

Tạo branch từ `main` mới nhất theo `feat/<milestone>-<noi-dung>` hoặc
`fix/<milestone>-<noi-dung>`. Commit theo thay đổi có thể review, ví dụ
`feat(e1): integrate Gold and benchmark stages`; không commit dữ liệu tải về,
`.venv`, secrets hoặc log Spark đầy đủ. Trước merge, kiểm diff, chạy test tương
ứng và gắn đúng evidence với code. Không force-push nhánh dùng chung.

## Tài liệu

- [REPORT.md](REPORT.md): báo cáo kỹ thuật chính A1–E1.
- [Kiến trúc và luồng commit](docs/architecture.md), [bảng milestone](docs/MILESTONE_STATUS.md).
- [B3](docs/B3_SILVER.md), [CDC C1](docs/C1_CDC.md), [schema C2](docs/C2_SCHEMA_EVOLUTION.md).
- [Time travel C3](docs/C3_TIME_TRAVEL.md), [Gold D1](docs/D1_GOLD.md), [benchmark D2](docs/D2_PERFORMANCE.md).
- [E1 runbook](docs/E1_RUNBOOK.md), [manifest lần chạy E1](docs/pipeline_run.json).
- [Ảnh C3 từ run E1](docs/evidence/e1/c3/summary.png),
  [Delta log có chú thích](docs/evidence/e1/c3/delta_log.png).
- [Evidence C3 lịch sử](docs/evidence/c3/pipeline_run.json) và
  [biên bản merge lịch sử](docs/MERGE_VALIDATION.md): giữ làm provenance, không thay thế run E1 mới.
