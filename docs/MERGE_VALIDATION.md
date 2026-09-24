# Tích hợp các branch vào main — 24/09/2026

## Phạm vi merge

Đã fetch GitHub, fast-forward main local từ `18ac8a0` đến `9d4a491`, rồi merge
C3 và D2 bằng merge commit để giữ lịch sử đóng góp. Branch D2 đã chứa C1 và D1;
không cherry-pick lại các commit đó. Các branch nguồn được giữ nguyên trên GitHub.

| Branch từ origin | Commit nguồn | Cách đưa vào main |
|---|---|---|
| main | `9d4a491` | Pull fast-forward, giữ khung `docs/REPORT.md` |
| feature/b1-data-generator | `7e68957` | Đã là ancestor của main |
| silver-b3 | `3110c3f` | Đã là ancestor của main |
| feat/c1-cdc-merge | `8f52b73` | Qua lịch sử D1/D2 |
| feat/c3-time-travel-audit | `29b6768` | Merge trực tiếp |
| feat/d1_gold | `3fdea78` | Qua lịch sử D2 |
| feat/d2_performance | `5df8609` | Merge trực tiếp |

Xung đột duy nhất là add/add ở `src/cdc_merge.py`. Giữ phiên bản C3: fixture đủ
cột cho insert, chỉ update fare/tip/total, kiểm tra replay và dùng Delta 4.0.1
tương thích Spark 4.0.1. Không khôi phục cấu hình JAR Delta 3.2.0/Scala 2.12
từ C1 cũ. Các file code D1/D2 được nhận nguyên bản từ branch nguồn.

README được cập nhật với lệnh Gold/benchmark và các đường dẫn đầu vào. Report
tại root giữ số liệu C3 cũ; `docs/REPORT.md` giữ khung của nhóm, có liên kết qua
lại. Số liệu D1 trên 2.880 dòng và benchmark D2 ngày 16/09 là evidence lịch sử,
không được gán thành kết quả mới sau merge hoặc sau C1/C2.

## Kiểm chứng

| Kiểm tra sau merge | Kết quả |
|---|---|
| Regression suite B3/C1/C2/D1/D2 | **27 passed**, 0 failed/errors/skipped |
| B1 → C3 trên dữ liệu TLC | PASS; Bronze cuối 3.160, Silver cuối 2.983, quarantine 180 |
| C1 và C2 | 5 update + 3 insert, thêm 100 dòng/cột fee; C2 replay không trùng |
| C3 | Original v0/v1/v2 đọc được sau VACUUM copy; 2 file obsolete xóa ở copy |
| Gold sau C3 | **273 nhóm**, SUM(trip_count) = **2.983**, total_revenue = **74.707,43** |
| Benchmark chạy thực tế | **4 điều kiện × 3 query × 3 lượt = 36 lần đo** |
| Bảo toàn dữ liệu qua tối ưu | So sánh toàn bộ giá trị Parquet active: cả 4 layout bằng Silver 2.983 dòng |
| Số file trong bốn layout | 16 / 1 / 1 / 1; không suy ra lợi ích skipping từ việc giảm thời gian |
| Lịch sử Git | Mọi remote branch được liệt kê ở trên đều là ancestor của main đã merge |

Evidence: [validation.json](evidence/merge_20260924/validation.json),
[JUnit](evidence/merge_20260924/tests.xml),
[pipeline](evidence/merge_20260924/pipeline_run.json),
[Gold](evidence/merge_20260924/gold_run.json),
[benchmark](evidence/merge_20260924/benchmark_run.json).
Manifest ghi hash code/tests và commit merge đã kiểm chứng. Thay đổi tiếp sau
trong lần bàn giao này chỉ cập nhật tài liệu/evidence, không sửa thuật toán D1/D2.

Các lệnh dùng Python 3.11, Java 17, Spark/Delta 4.0.1, local[2].
Đặt `PYSPARK_PYTHON` và `PYSPARK_DRIVER_PYTHON` cùng trỏ tới `.venv/bin/python`
để fixture Python không chạy worker bằng Python hệ thống khác phiên bản.

```bash
export JAVA_HOME="$PWD/data/.runtime/jdk-17.0.20.1+1/Contents/Home"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_SUBMIT_ARGS='--conf spark.databricks.delta.snapshotPartitions=2 pyspark-shell'

.venv/bin/python -m pytest -q tests
.venv/bin/python lakehouse_pipeline.py \
  --run-dir data/merge_validation_20260924/pipeline
.venv/bin/python -m src.gold \
  --silver-dir data/merge_validation_20260924/pipeline/silver \
  --gold-dir data/merge_validation_20260924/gold \
  --show 0 --output data/merge_validation_20260924/gold_run.json
.venv/bin/python optimization_benchmark.py \
  --silver-dir data/merge_validation_20260924/pipeline/silver \
  --benchmark-dir data/merge_validation_20260924/benchmark --iterations 3 \
  --output data/merge_validation_20260924/benchmark_run.json \
  --report data/merge_validation_20260924/benchmark_report.md
```

Các đường dẫn trên ghi lại lần kiểm chứng. Chọn thư mục run mới khi chạy lại
vì runner C3 từ chối ghi đè run đã có. Log đầy đủ ở `data/merge_validation_20260924/`,
ngoài Git; JSON/JUnit gọn ở `docs/evidence/merge_20260924/`.

## Giới hạn còn lại sau merge

- `lakehouse_pipeline.py` vẫn điều phối đến C3. Gold và benchmark chạy bằng
  entry point riêng; chưa hoàn thành một runner E1 chung hoặc E2 toàn pipeline.
- D2 có baseline / OPTIMIZE / Z-ORDER một cột / Z-ORDER hai cột. Chưa có điều kiện
  `CLUSTER BY + OPTIMIZE FULL` như cột Verify trong bảng phân công.
- `files_scanned`/`bytes_read` của D2 là ước lượng file/size từ log; chưa lấy
  Spark task I/O metrics. Hàm thống kê chưa hỗ trợ khôi phục checkpoint log và
  đang so sánh timestamp dạng chuỗi; cần rà soát trước khi nghiệm thu D2.
- Query Q2 dùng khoảng giữa tháng 1, trong khi mẫu B1 tập trung quanh đầu tháng.
  Kết quả aggregate có thể có COUNT(*) = 0 dù collect vẫn trả một dòng kết quả.
  Trường `result_count` trong benchmark hiện đếm dòng output, không đếm chuyến.
- Clear Spark cache và đổi đường dẫn không loại bỏ OS page cache/JIT. Các bảng
  tối ưu nhỏ có thể chỉ còn một file, nên thời gian nhanh hơn không tự chứng minh
  lợi ích data skipping của Z-ORDER.
- Gold cần tiếp tục kiểm chứng các trường hợp tip/fare NULL khi đối chiếu trung
  bình toàn cục: AVG bỏ NULL nhưng hàm verify hiện dùng trọng số toàn bộ trip_count.
- F1 vẫn cần hợp nhất report theo khung nhóm, dựng deck và rehearsal toàn bài.

Đây là nghiệm thu thao tác merge và khả năng chạy chung của các module hiện có,
không phải xác nhận mọi tiêu chí rubric đã hoàn thành.
