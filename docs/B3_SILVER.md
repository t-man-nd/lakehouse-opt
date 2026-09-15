# B3 — Silver Cleaning + Quarantine

Phạm vi: thực hiện B3 theo bảng phân công và `docs/schema.md` (B1-v1.0).
Entry point tích hợp là `src.silver.build(spark)`, trả về dict số đếm.
CDC/MERGE và schema evolution thuộc C1/C2, chạy sau bước này.

## 1. Review B1 và B2

Kiểm tra checkout tại commit `1a2e219a0b6b7a8ac69293a183c734b4bc53585b`:

| Hạng mục | Kết quả review |
|---|---|
| B1 contract | Có đủ schema, key `trip_id`, 4 rule và manifest 3.060 dòng |
| Generator | Code thực tế là `src/generator.py`; một số hướng dẫn cũ gọi `generate_batches.py` |
| B1 checksums | Bytes của generator, schema và manifest khớp `docs/checksums.txt` |
| Fixture B1 | 18 dòng: 2 duplicate excess, 2 lỗi pickup, 4 thiếu location, 2 fare không hợp lệ; kỳ vọng 8 Silver + 10 rejected |
| Ba batch gốc | Đã tải TLC Yellow Taxi 01–03/2025, sinh lại bằng B1; checksum JSON trùng `docs/checksums.txt` |
| Bronze gốc | Đã chạy B2 trên ba batch thật; 3.060 dòng, 1.020 dòng/batch, metadata không NULL |
| B2 ingestion | Đọc NDJSON, không filter/dedup, thêm `_ingest_ts`, `_source_file`, `_batch_id`, append và bật `mergeSchema` |
| B2 runtime | Bỏ JAR Delta 3.2.0/Scala 2.12 hardcode; dùng JAR tương ứng package `delta-spark` đã cài |
| B2 chạy lại | Tiếp tục append là đúng yêu cầu B2; chạy lại trên cùng Bronze sẽ tăng số dòng |
| Signature B2 | Hiện là `run_bronze(spark, raw_dir, bronze_dir)`, chưa phải `bronze.run(spark, months, overwrite)` nêu ở E1 |

`bronze.run(..., overwrite)` cần được nhóm chốt ở E1 vì xung đột với nguyên tắc
append-only nếu dùng để ghi đè Bronze. B3 không thay đổi API của người phụ trách B2.
Thông báo nghiệm thu B2 hiện chỉ là warning khi count khác 3.060;
B3 tự kiểm tra metadata và có tùy chọn kiểm tra manifest trước khi ghi output.

Một số tài liệu nền cần đồng bộ ở mốc tích hợp: ví dụ key 2024 trong schema trong
khi generator dùng 2025; README đang mô tả đầu vào Parquet trực tiếp và benchmark
Gold, còn phân công hiện tại yêu cầu JSON qua B1 và benchmark Silver.

## 2. Bốn rule và cách đối soát

| Rule | Điều kiện | `reject_reason` | Kỳ vọng 3 batch gốc |
|---|---|---|---:|
| DQ-02 | Pickup không parse được theo `yyyy-MM-dd HH:mm:ss` | `INVALID_PICKUP_DATETIME` | 30 |
| DQ-03 | Pickup hoặc drop-off location sau chuyển kiểu là NULL | `MISSING_LOCATION` | 60 |
| DQ-04 | `fare_amount <= 0` | `INVALID_FARE` | 30 |
| DQ-01 | Dòng dư theo `trip_id` trong các dòng qua validation | `DUPLICATE_TRIP` | 60 |

Các lỗi của B1 được sinh trên tập chỉ số rời nhau. B3 kiểm tra pickup, location,
fare theo thứ tự trên, rồi gọi `dropDuplicates(["trip_id"])`. Nếu dữ liệu ngoài
B1 có nhiều lỗi trên một dòng, chỉ ghi lý do đầu tiên; dòng lỗi không loại bỏ
một dòng hợp lệ cùng key. B3 không chọn bản cập nhật mới nhất; đó là trách nhiệm C1.

Các cột nguồn được chuyển bằng `try_cast`. Hai datetime dùng `try_to_timestamp`
với format tường minh sau `try_cast("string")`, parser `CORRECTED`, timezone UTC.
Giữ ANSI mode bật khi kiểm thử để chứng minh dữ liệu sai không làm chết job.

Không thêm rule cho dropoff, khoảng cách, số hành khách, tip, phạm vi location
hoặc giá trị NULL của fare. Theo đúng predicate B1-v1.0, fare NULL/không parse
được sẽ thành NULL và không thỏa `fare <= 0`, nên không bị reject bởi DQ-04.
Đây là giới hạn của contract hiện tại, cần thống nhất đổi contract trước nếu
nhóm muốn thêm yêu cầu `fare IS NOT NULL`. Generator gốc không tạo trường hợp đó.

Silver chứa các cột đã chuyển kiểu, ba metadata và cột mở rộng nếu có trong Bronze.
Quarantine giữ **giá trị gốc**, metadata và `reject_reason`, giúp xem lại chính
chuỗi sai như `not-a-date`. Giữ survivor đã cache cho cả Silver và phép trừ
đa tập `exceptAll`, để thu hồi đủ N−1 bản sao, kể cả khi các dòng giống hệt nhau.

```text
Bronze (version cố định)
  ├── sai pickup/location/fare ───────────→ silver_rejected
  └── qua validation
       ├── dropDuplicates(trip_id) ──────→ taxi_trips (Silver)
       └── các bản sao dư ───────────────→ silver_rejected

bronze_count = silver_count + rejected_count
3060        = 2880         + 180          [kỳ vọng B1]
```

Thiếu cột B1 hoặc metadata B2 là lỗi đầu vào và dừng build, không phải một rule
quarantine bổ sung. Tùy chọn `--manifest` kiểm tra tổng và từng loại lỗi trước
khi ghi bảng. Sau ghi, B3 đọc lại cả hai bảng để kiểm tra số dòng thực lưu.

## 3. Cài đặt và chạy trên dữ liệu nhóm

Dùng Python 3.11 và JDK 17 hoặc 21; dependencies đã pin Spark/Delta 4.0.1.

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -r requirements-dev.txt
source .venv/bin/activate
export JAVA_HOME="/duong/dan/toi/jdk/Contents/Home"
export SPARK_LOCAL_IP=127.0.0.1
```

Máy hiện tại có JDK dùng riêng cho lần kiểm thử ở
`data/.runtime/jdk-17.0.20.1+1/Contents/Home`; có thể đặt:

```bash
export JAVA_HOME="$PWD/data/.runtime/jdk-17.0.20.1+1/Contents/Home"
```

Lần khởi động Spark đầu tiên cần mạng để tải JAR Delta qua Maven.

Nếu có ba JSON B1, đặt chúng ở `data/raw/batch_01.json` … `batch_03.json`.
Nếu chỉ có Parquet gốc, sinh JSON bằng đúng tên module hiện có:

```bash
python -m src.generator --output-dir data/raw
```

Nếu chưa có Bronze, chạy B2 **một lần** trên đường dẫn Bronze mới:

```bash
python -m src.bronze
```

Nếu đã có Bronze thì chạy B3 trực tiếp:

```bash
python -m src.silver \
  --bronze-dir data/bronze/taxi_trips \
  --silver-dir data/silver/taxi_trips \
  --rejected-dir data/silver/silver_rejected \
  --manifest docs/error_manifest.json \
  --output docs/b3_run.json
```

Đường dẫn tương đối được tính từ thư mục chạy lệnh. Các tham số có thể đổi sang
`/data/...` trong môi trường của nhóm. API dùng SparkSession đã cấu hình Delta:

```python
from src import silver

counts = silver.build(spark)
assert counts["silver_count"] + counts["rejected_count"] == counts["bronze_count"]
```

`build` ghi đè snapshot Silver/quarantine để chạy lại B3 không tích lũy output.
Nó không sửa Bronze. **Chỉ rebuild trước C1/C2**, hoặc khi chủ động muốn dựng lại
Silver từ Bronze: rebuild sau C1 sẽ thay thế trạng thái CDC hiện tại. Hai bảng
output là hai transaction riêng; nếu việc ghi bảng thứ hai thất bại, sửa nguyên
nhân và chạy lại B3 từ cùng Bronze trước khi chuyển sang C1/C2.

## 4. Kiểm thử và evidence

```bash
python -m pytest -q tests/test_silver.py \
  --basetemp=data/b3_validation \
  --junitxml=docs/b3_test_results.xml
```

`--basetemp` là thư mục dành riêng cho kiểm thử; pytest sẽ làm mới nó ở lần chạy
kế tiếp. Không đặt dữ liệu nhóm vào thư mục này.

Bộ test chạy PySpark/Delta thật, gồm parsing ANSI với dữ liệu sai; đúng bốn rule;
B2 bảo toàn raw/metadata; fixture 8 + 10 = 18; quarantine giữ nội dung gốc;
Silver không trùng key; rebuild B3 giữ nguyên count và không ghi thêm version
Bronze; kiểm thử tổng hợp 2.880 + 180 = 3.060 và số lượng từng batch.

Bộ 3.060 dòng của test dùng bản ghi sạch từ fixture và logic chèn lỗi của B1,
**là dữ liệu tổng hợp**, không thay thế ba batch gốc/checksum của nhóm.
Các bảng kiểm thử được lưu riêng dưới `data/b3_validation/`; evidence nghiệm thu
dữ liệu nhóm là `docs/b3_run.json`, chỉ được tạo khi chạy lệnh với Bronze thật.

Kết quả kiểm thử ngày 09/09/2026: **5 passed, 0 failed, 0 skipped** trên Python
3.11.15, Spark 4.0.1, Delta 4.0.1 và JDK 17. Có thể tái tạo bằng lệnh pytest ở trên.
Fixture B1 cho **8 Silver + 10 rejected = 18**; bộ tổng hợp cho
**2.880 Silver + 180 rejected = 3.060**, đúng các số đếm từng reason và từng batch.

Lệnh CLI đã chạy trên Bronze thật của nhóm. Kết quả đếm chính thức nằm ở
`docs/b3_run.json`, còn hai bảng Delta local là
`data/silver/taxi_trips` và `data/silver/silver_rejected`. Kết quả này dùng
đúng ba JSON được sinh từ Parquet TLC; không phải fixture 18 dòng.

Thông tin tải nguồn và SHA-256 được ghi ở `docs/source_data_manifest.md`.
Các Parquet và JSON raw nằm trong `data/` (được ignore khỏi Git vì kích thước),
nhưng có thể tái tạo bằng lệnh generator trong mục 3.

## 5. Tham chiếu runtime

- [Spark 4.0 runtime requirements](https://spark.apache.org/docs/4.0.0/)
- [Spark ANSI mode và các hàm try](https://spark.apache.org/docs/4.0.0/sql-ref-ansi-compliance.html)
- [Delta/Spark compatibility](https://docs.delta.io/releases/)
- [Delta Python setup](https://docs.delta.io/quick-start/)
