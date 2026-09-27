# Trạng thái nghiệm thu A1–E1

Đối chiếu bảng phân công mới nhất người dùng gửi ngày 24/09/2026. Tiêu chí lấy
phần VIỆC/Verify chi tiết; D2 thêm compaction để bao phủ cả Done when ba trạng
thái. Nguồn kỹ thuật và mô hình thiết kế nằm ở [REPORT.md](../REPORT.md).

**PASS A1–E1**, nghiệm thu ngày 24/09/2026 tại
`data/e1_runs/official_20260924_verified`. Runner hoàn tất trong 45,337 giây sau
Spark startup; bộ test riêng có **90 PASS / 0 failure / 0 error / 0 skip** trong
61,00 giây. Code SHA-256 và evidence được khóa trong
[validation.json](evidence/e1/validation.json). Các artifact này xác nhận phạm vi
kỹ thuật A1–E1; E2 và F1 vẫn là milestone riêng.

**Mở rộng đã PASS B1–E1 ở ba mức 300.000 / 1.020.000 / 3.000.000 dòng nguồn.**
Mức lớn nhất có 3.060.000 raw → 2.880.000 Silver B3 + 180.000 rejected →
2.880.103 Silver sau C1/C2; Gold 5.680 nhóm. Tổng cộng 270 lượt đo benchmark
được đối chiếu độc lập bằng PyArrow/Decimal. Xem [scaling](SCALING.md) và
[evidence mới](evidence/scaling/validation.json). Bảng dưới giữ số liệu fixture
nhỏ để đối chiếu lịch sử; ba quy mô khác nhau không thay nghiệm thu E2 chạy
hai lần trên cùng input.

| Mốc | Sản phẩm và đối chiếu | Kết quả nghiệm thu |
|---|---|---|
| A1 | README, requirements, .gitignore, config, report, branch convention, khung slide | PASS: artifact có đủ; CLI/import chạy được |
| A2 | REPORT mục 2–3, CIDR 2021, multi-hop/producer-centric, architecture | PASS: nội dung và nguồn chính thức được rà soát |
| B1 | Generator, schema, manifest, ba JSON; 4 nhóm lỗi | PASS: ba Parquet checksum đúng; tái sinh JSON/manifest byte-identical; 3 × 1.020 dòng |
| B2 | Append-only Bronze, mergeSchema, ba metadata | PASS: 3.060 dòng; metadata đầy đủ |
| B3 | CORRECTED/UTC/try_cast, đúng 4 rule, dedup/quarantine | PASS: 2.880 + 180 = 3.060; đủ reason counts |
| C1 | Fixture seed 42, key thật, chỉ update fare/tip/total; CDC theory | PASS: 5 update + 3 insert cùng một MERGE → 2.883 |
| C2 | surcharge_fee giả lập ở Bronze/Silver; cũ NULL, mới có fee | PASS: Bronze 3.160 / Silver 2.983; history/schema đúng; replay no-op |
| C3 | Hai version cũ, history/log/ACID/OCC, VACUUM copy, ảnh annotate | PASS: original v0/v1/v2 nguyên vẹn; copy xóa 2 file, old read lỗi như dự kiến |
| D1 | Tip% per trip, guard fare, earnings theo contract, UTC, SQL cross-check | PASS: 152 nhóm zone × hour-of-day; tổng 2.983 chuyến; khớp SQL và kiểm tra Python độc lập |
| D2 | 4 layout + compaction, ≥3 lượt, median, real metrics, before/after, threats | PASS: 36 lượt chính + 18 đối chiếu; mọi query không rỗng và kết quả bằng nhau; 5 layout giữ nguyên dữ liệu |
| E1 | Một lệnh B1→Gold + D2, log stage, docs/pipeline_run.json | PASS: tất cả stage; source Silver v2 không bị benchmark thay đổi |
| E2 | Hai lần toàn pipeline có cùng kết quả; try_cast/tip% | Chưa nghiệm thu riêng E2; unit/replay tests không thay điều kiện này |
| F1 | Deck cuối, rehearsal ≤15 phút, ảnh dự phòng | Chưa nghiệm thu F1; đã có khung slide A1 và ảnh C3 mới |

## Nguồn bằng chứng

- Bất biến của run đã nghiệm thu: [pipeline](evidence/e1/pipeline_run.json),
  [Gold](evidence/e1/gold_run.json), [D2](evidence/e1/benchmark_run.json),
  [JUnit](evidence/e1/tests.xml), [source/B1/hash validation](evidence/e1/validation.json).
- [Ảnh C3](evidence/e1/c3/summary.png), [log annotate](evidence/e1/c3/delta_log.png),
  [C3 manifest](evidence/e1/c3/manifest.json).
- Mới nhất: [pipeline_run.json](pipeline_run.json); file này thay đổi khi chạy
  lần tiếp theo, nên luôn đọc `status`, `run_dir` và runtime.
- Lịch sử C3: [pipeline run](evidence/c3/pipeline_run.json),
  [manifest](evidence/c3/manifest.json), [JUnit](evidence/c3/tests.xml).
- Lịch sử merge: [biên bản](MERGE_VALIDATION.md),
  [pipeline](evidence/merge_20260924/pipeline_run.json),
  [Gold](evidence/merge_20260924/gold_run.json),
  [benchmark cũ](evidence/merge_20260924/benchmark_run.json).

Counts trong bảng là invariant fixture. Benchmark latency phải lấy từ đúng
lần chạy mới; không đặt target % cải thiện rồi chọn bỏ các lượt không thuận lợi.
Dữ liệu mẫu khoảng 3.000 dòng không nghiệm thu throughput toàn TLC hoặc target
file ~1 GB. E1 PASS là kết quả kỹ thuật của phạm vi tích hợp, không phải điểm
rubric hay hoàn thành F1.
