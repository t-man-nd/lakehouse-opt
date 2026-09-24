# C2 — Schema Evolution tại Bronze và Silver

Phiên bản tích hợp trên main giữ ý tưởng C2 của nhóm: batch_04 có
`surcharge_fee`, append với `mergeSchema=true` ở cả Bronze và Silver.
Cột này là giả lập, khác `cbd_congestion_fee` của TLC 2025.

## Chạy riêng

Dùng runner chung trong README để có cả C1 và C3. Khi cần chạy riêng:

```bash
.venv/bin/python -m src.schema_evolution \
  --raw-dir data/raw \
  --batch-file data/raw/batch_04.json \
  --bronze-dir data/bronze/taxi_trips \
  --silver-dir data/silver/taxi_trips \
  --rejected-dir data/silver/silver_rejected
```

Nếu chưa có bảng baseline, code khởi tạo B2/B3. Nếu đã có C1 thì giữ Silver,
chỉ append batch mới; không gọi lại B3. Khi cần timeline C3 có MERGE, phải chạy
C1 trước C2 hoặc dùng runner chung.

## Các điểm đã sửa khi tích hợp

- Tái sử dụng parser/rules B3 để không lệch contract; dữ liệu fixture lỗi bị từ
  chối trước ghi thay vì bị filter âm thầm.
- Kiểm unique key và overlap với bảng đích.
- Cùng batch ID/nội dung: no-op. Batch ID đã dùng nhưng nội dung đổi: báo lỗi.
- Retry sau khi Bronze đã ghi có thể tiếp tục Silver; hai bảng là hai transaction.
- Truyền đúng đường dẫn quarantine khi dựng baseline tùy chỉnh.
- Generator tính dropoff bằng cộng 12 phút, kể cả qua giờ tiếp theo.
- Runtime chia sẻ Spark 4.0.1/Delta 4.0.1, local workers dùng cùng Python driver.

## Kết quả tích hợp được xác minh

| Bảng | Trước C2 | Thêm mới | Sau C2 | Dòng cũ NULL | Version schema change |
|---|---:|---:|---:|---:|---:|
| Bronze | 3.060 | 100 | 3.160 | 3.060 | 3 |
| Silver (đã MERGE) | 2.883 | 100 | 2.983 | 2.883 | 2 |

Dòng mới có fee khác NULL. Tập dữ liệu cũ được so sánh bằng multiset; cả giá trị
tiền đã sửa ở C1 được bảo toàn. Chạy lại C2 không tăng count hoặc version.

History ghi operation `WRITE`. Chứng minh schema change bằng cả schema cũ/mới
và `metaData.schemaString` trong commit, không tìm operation tên “SCHEMA EVOLUTION”.
JSON C2 không có `remove`: không rebuild các file cũ.

Xem [pipeline_run.json](evidence/c3/pipeline_run.json),
[audit.json](evidence/c3/audit.json), [ảnh summary](evidence/c3/summary.png)
và [log có chú thích](evidence/c3/delta_log.png).
Ảnh trong `docs/screenshots/c2_*.png` được giữ làm lịch sử phần việc C2 riêng
trước khi tích hợp; các con số 200 dòng của lần chạy cũ không dùng cho run này.

Giới hạn: replay check dành cho một writer. Nếu nhiều job đồng thời cùng nhận
batch chưa xử lý, cần transaction/application ID hoặc điều phối bổ sung; kiểm
tra batch ID trước append không tự tạo exactly-once đa writer.
