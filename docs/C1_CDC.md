# C1 — CDC / MERGE

Tích hợp ý tưởng từ `origin/feat/c1-cdc-merge@8f52b73` vào baseline B3 trên main.
Phiên bản tích hợp dùng runtime Delta 4.0.1, không nạp JAR 3.2.0/Scala 2.12.
Source cung cấp đủ cột cho insert, không chỉ bốn cột key/fare/tip/total.

## Demo riêng

Chạy B3 trên bảng mới trước, hoặc dùng runner chung ở README. Với bảng Silver
baseline chưa có C1/C2:

```bash
.venv/bin/python -m src.cdc_merge \
  --silver-path data/silver/taxi_trips \
  --cdc-path data/cdc/late_updates.parquet \
  --generate
```

`--generate` chỉ dùng lần đầu; file đã tồn tại sẽ bị từ chối. Để replay fixture,
chạy lại cùng lệnh bỏ `--generate`. Không regenerate fixture từ state đã update.
Mặc định output là `data/cdc/merge_result.json`; dùng `--output` để đổi.

Fixture chọn 8 dòng theo thứ tự hash `42:trip_id`, cập nhật 5 khóa thật, tạo 3 khóa
insert. Cả 8 dòng mang đủ schema Silver. Matched chỉ đổi fare/tip/total; metadata
và mọi cột còn lại của target giữ nguyên. Số tiền trong source là giá trị tuyệt
đối; replay không cộng thêm lần nữa. Kiểm tra dữ liệu fixture nghiêm hơn B3 vì
đây là contract riêng cho sự kiện điều chỉnh tiền, không phải thêm rule B1.

## Nghiệm thu

Lần đầu: before=2.880, matched=5, updated=5, unmatched/inserted=3, after=2.883.
Code đối chiếu cả giá trị persisted, cột không được sửa và `operationMetrics`.
Test replay: updated=0, inserted=0, tập dữ liệu không đổi; history vẫn có thể
ghi thêm MERGE no-op. Runner chính không replay C1 trước C3 để giữ timeline
ba version dễ trình bày.

Xem [kết quả tích hợp](evidence/c3/pipeline_run.json),
[fixture trước/sau](evidence/c3/cdc_fixture.json) và REPORT mục 5.
Đây là batch mô phỏng một writer; chưa có source offset/delete/event ordering.
