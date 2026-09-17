# C3 — Time Travel, Delta Log và VACUUM

## Chạy đầy đủ

Thiết lập `.venv`, Java 17 và `SPARK_LOCAL_IP` theo README, rồi:

```bash
.venv/bin/python lakehouse_pipeline.py
```

Runner in thư mục evidence cuối cùng. Từng lần chạy giữ nguyên thư mục trước.
Run đã nghiệm thu: `data/c3_demo/official_20260915_final`; snapshot Silver:

| Stage | Version | Dòng | Nội dung |
|---|---:|---:|---|
| B3 | 0 | 2.880 | Schema gốc, số tiền trước CDC |
| C1 | 1 | 2.883 | MERGE: 5 update + 3 insert |
| C2 | 2 | 2.983 | WRITE append + metaData chứa surcharge_fee |

Bronze schema change ở v3; version được đánh số riêng cho từng bảng.

## Đọc lại trực tiếp — không thay đổi bảng

```bash
.venv/bin/python - <<'PY'
from src.delta_runtime import get_spark
spark = get_spark()
path = "data/c3_demo/official_20260915_final/silver"
try:
    spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select(
        "version", "timestamp", "operation", "operationMetrics"
    ).show(truncate=False)
    for version in (0, 1, 2):
        past = spark.read.format("delta").option("versionAsOf", version).load(path)
        print("VERSION", version, "COUNT", past.count(), "SCHEMA", past.schema.simpleString())
        past.select("trip_id", "fare_amount", "tip_amount", "total_amount").orderBy("trip_id").show(3)
finally:
    spark.stop()
PY
```

Để minh họa đúng chuyến bị sửa, lấy `updates[0].trip_id` từ
`docs/evidence/c3/cdc_fixture.json` rồi filter ID này. `audit.json` đã ghi sẵn
giá trị ở cả ba version. Các đọc lịch sử trong runner materialize tất cả dòng,
schema và hash; không chỉ dựa vào count có thể lấy từ metadata.

## Log thật và chú thích

- [History Silver](evidence/c3/silver_history.json) và [History Bronze](evidence/c3/bronze_history.json).
- [MERGE JSON v1](evidence/c3/delta_log/silver/00000000000000000001.json): commitInfo, add, remove.
- [Schema JSON v2](evidence/c3/delta_log/silver/00000000000000000002.json): metaData + add, không remove.
- [Ảnh chú thích](evidence/c3/delta_log.png): render từ chính JSON trên; stats được parse để đọc được.
- [Trang log](evidence/c3/delta_log.html) liên kết file gốc, ghi version và SHA-256.

`remove` là tombstone của snapshot, chưa xóa Parquet. `add.stats.minValues` và
`maxValues` giúp loại file không thể khớp predicate. `metaData.schemaString`
xác nhận thay đổi schema; một dòng `WRITE` trong history tự nó chưa đủ.

## VACUUM an toàn cho demo

Không copy/paste RETAIN 0 lên bảng chính. Runner tự tạo `vacuum_copy` bằng copy
vật lý data + log; chỉ bản này được VACUUM. Kiểm tra đường dẫn không trùng/lồng
bảng gốc, không symlink hoặc data path trỏ ra ngoài. Sau DRY RUN, tạm tắt
retention check, VACUUM 0 giờ, rồi khôi phục cấu hình. Đây là **hack demo-only**.

Lần nghiệm thu xóa 2 Parquet obsolete. Copy v0 lỗi thiếu file như dự kiến; copy
current vẫn 2.983 dòng. Original v0/v1/v2 đọc được, hash file và dữ liệu không
đổi. `vacuum.json` ghi danh sách file, exception rút gọn và hash các snapshot.
Version 1 của copy có thể vẫn đọc được nếu các file của nó còn thuộc current;
VACUUM không đồng nghĩa mọi version cũ đều mất ngay.

## Flow trình bày C3, khoảng 4–5 phút

1. Mở ảnh summary và chỉ timeline 2.880 → 2.883 → 2.983.
2. Mở CDC fixture, chỉ một ID và fare/tip/total trước/sau.
3. Đọc v0 và v1 bằng đoạn lệnh trên; chỉ schema v2 có surcharge_fee.
4. Mở ảnh log: MERGE add/remove, C2 metaData, stats min/max.
5. Mở vacuum.json: lỗi ở copy, bản gốc giữ nguyên, giải thích retention.

Nếu muốn chạy live cả pipeline thì chạy trong thư mục mới và dùng runtime đo
trong `pipeline_run.json` để chuẩn bị. Đây là flow riêng C3, không thay rehearsal
10–15 phút có cả Gold/benchmark ở F1.

## Xuất lại evidence

```bash
.venv/bin/python scripts/render_c3_evidence.py \
  --source data/c3_demo/official_20260915_final/evidence \
  --output docs/evidence/c3_new \
  --tests data/c3_validation_logs/tests_final.xml
```

Script từ chối thư mục output đã tồn tại để tránh ghi đè evidence cũ; chọn output
mới khi xuất lần khác. Hai trang HTML là phần trình bày dữ liệu run thật, không
phải ảnh giả terminal. Có thể mở trong browser và chụp toàn trang để đưa vào slide.
