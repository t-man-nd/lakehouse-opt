# Mở rộng mẫu và kiểm chứng tài nguyên

Bộ demo 3.060 dòng raw vẫn là fixture có checksum để đối chiếu các invariant
B1–E1. Bộ mở rộng có thư mục raw, manifest lỗi, checksum, config và output
riêng. Không thay số liệu demo trong báo cáo bằng số liệu từ một run khác.

## Phương pháp

Nguồn là ba file TLC Yellow Taxi tháng 01–03/2025 đã tải. Generator chọn
không hoàn lại theo từng **ngày × giờ pickup**, seed 42. Trong mỗi tháng,
quota của từng giờ tỷ lệ với số dòng đủ điều kiện trong giờ đó; phần làm
tròn được phân bổ theo Hamilton để tổng đúng bằng `--base-rows`. Các dòng
trong cùng giờ có cơ hội chọn như nhau. Cả ba tháng được lấy **cùng số dòng**,
nên mẫu gộp không giữ tỷ trọng tổng số chuyến giữa các tháng của toàn quý.
Gold tổng hợp trên bộ mẫu đã chèn lỗi và làm sạch; không suy rộng các tổng
trong Gold thành tổng lượt chuyến hay doanh thu của toàn NYC.

Dòng đủ điều kiện có pickup/dropoff khác NULL, location hữu hạn và khác NULL,
fare hữu hạn và > 0. Với tên file TLC chuẩn, pickup còn phải nằm trong đúng
tháng của file. Các dòng nguồn không đủ điều kiện được đếm trong manifest;
chúng không phải lỗi được chèn vào bộ B1. Manifest ghi hash nguồn, số dòng
đủ điều kiện/được chọn của từng giờ, seed và hash vị trí nguồn đã chọn.
Không coi phương pháp này là mẫu ngẫu nhiên đơn giản của toàn NYC.

Ba Parquet nguồn có **11.198.026 dòng**, trong đó **10.657.950 dòng** qua
bộ lọc đủ điều kiện nêu trên: 3.329.688 / 3.393.416 / 3.934.846 theo tháng.
Pickup của cả ba file là Arrow `timestamp[us]` **không có timezone**. Phân
tầng dùng ngày/giờ được ghi trong nguồn, không thực hiện chuyển New York
sang UTC. Tháng 03 có 743 giờ quan sát được, không có `2025-03-09 02:00`;
điều này phù hợp với một khoảng giờ chuyển DST, nhưng không chứng minh
đã có bước đổi múi giờ. Spark dùng session UTC để xử lý chuỗi nhất quán;
đó là quy ước kỹ thuật của pipeline, không bổ sung thông tin timezone
vốn không có trong Parquet.

Sampler đọc Parquet theo Arrow batch trong hai lượt để tránh biến toàn
nguồn thành Python records. Tuy nhiên, các chỉ số chọn mẫu và danh sách
dictionary của **mẫu đã chọn trong một tháng** vẫn nằm trong RAM, rồi
generator tạo danh sách có lỗi để ghi JSON. Vì vậy bộ nhớ tăng theo số
dòng được chọn mỗi tháng, không phải hằng số theo quy mô mẫu. Mức triệu
dòng cần có phép đo tài nguyên của chính generator, không chỉ Spark.

[Protocol đã chọn](evidence/scaling/protocol.json) giữ cấu hình sau cho cả
ba mức. Đây là **kế hoạch chạy**, trạng thái thực tế được ghi ở cuối tài liệu:

| Mức | Cơ sở mỗi tháng | Tổng cơ sở | Raw dự kiến | Silver B3 dự kiến | Silver sau C1/C2 dự kiến |
|---|---:|---:|---:|---:|---:|
| `300k` | 100.000 | 300.000 | 306.000 | 288.000 | 288.103 |
| `1020k` | 340.000 | 1.020.000 | 1.040.400 | 979.200 | 979.303 |
| `3000k` | 1.000.000 | 3.000.000 | 3.060.000 | 2.880.000 | 2.880.103 |

Mỗi tháng chèn duplicate excess bằng 2% số cơ sở và bốn nhóm lỗi bằng 1%
mỗi nhóm: datetime, thiếu PU, thiếu DO, fare. Các tập chỉ số lỗi tách nhau.
CDC thêm 3 dòng, schema evolution thêm 100 dòng; 5 update không tăng count.
Các con số dự kiến phải được đối chiếu với manifest và dữ liệu thực tế.
Mỗi mức phải vượt qua các kiểm tra đúng dữ liệu và giới hạn tài nguyên
trước khi chuyển sang mức kế tiếp. Mức lớn hơn 3 triệu chỉ được cân nhắc
nếu còn dư tài nguyên; không cần dùng hết nguồn để có phép đo hữu ích.

| Tham số cố định | Giá trị |
|---|---|
| Spark master / driver heap | `local[4]` / `4g` |
| Shuffle và default parallelism | 16 |
| Baseline partitions | 256 |
| Target cho Z-ORDER/liquid clustering | 1 MiB = 1.048.576 byte |
| Target compaction đối chứng | 1 GiB, báo cáo ở phase riêng |
| Lượt đo | 5 mỗi query/layout, sau 1 warmup mỗi query/layout |
| Guard mỗi command | RSS process tree 12 GiB; disk trống tối thiểu 12 GiB; timeout 1.800 giây |

Giữ nguyên query, seed, số core, cấu hình Spark và chính sách cache khi so
sánh các layout **trong cùng một mức**. Lưu từng lượt, median và độ phân tán.
D2 chọn predicate từ snapshot trước khi đo để đảm bảo có dòng khớp: PU 161
khi tồn tại, Q2 chọn ngày nhiều chuyến nhất của PU đó, Q3 dùng vùng quanh
PU/DO đã chọn. Vì predicate cụ thể có thể khác giữa các mức, so thời gian
giữa các mức chỉ mang tính mô tả; đây không phải phép đo tăng dữ liệu với
cùng một query cố định xuyên mọi mức. Các predicate và số dòng khớp được
lưu trong `benchmark_run.json`. Không chọn query, số lượt hay file target
sau khi nhìn thấy layout nào chạy nhanh nhất. Tất cả các layout trong một
mức xuất phát từ cùng version Silver và phải trả về cùng kết quả.

File target phải tạo đủ nhiều file để quan sát việc bỏ qua file. Báo cáo
số file thực tế trước/sau từng phép tối ưu, kích thước file và độ chọn lọc
của từng truy vấn. Nếu layout chỉ còn một file hoặc query không khớp dòng
nào, vẫn giữ bằng chứng nhưng không dùng trường hợp đó để khẳng định hiệu
quả pruning trên một bảng lớn. Các lần thử điều chỉnh cấu hình phải giữ
danh tính riêng; không trộn median giữa các cấu hình.

## Sinh mẫu có kiểm soát

Ví dụ tạo bộ `300k` trong thư mục mới. `--base-rows` và các số lỗi đều tính
**cho một tháng**, không phải tổng của ba tháng:

```bash
.venv/bin/python -m src.generator \
  --sampling stratified --sampling-seed 42 --seed 42 \
  --base-rows 100000 --duplicates 2000 \
  --malformed-dates 1000 --missing-pu 1000 --missing-do 1000 --invalid-fares 1000 \
  --output-dir data/scale_inputs/300k
```

Stratified mode sinh cả `error_manifest.json` và `checksums.txt` bên cạnh
ba batch JSON. Nếu đã có bộ này, dùng lại các file có checksum đã kiểm chứng;
generator không ghi đè. Mức `1020k` dùng `340000 / 6800 / 3400`; mức `3000k`
dùng `1000000 / 20000 / 10000` tương ứng base / duplicate / mỗi nhóm lỗi.
Các config [`300k`](../config/scale_300k.json),
[`1020k`](../config/scale_1020k.json), [`3000k`](../config/scale_3000k.json)
trỏ đến thư mục input riêng của từng mức.

## Đo tài nguyên một lần chạy

[`scripts/run_scale_trial.py`](../scripts/run_scale_trial.py) chạy một command
ở foreground, không qua shell. Wrapper ghi log đầy đủ, metrics JSON và CSV
mẫu tài nguyên vào các file mới. Đường dẫn đã tồn tại bị từ chối để giữ
bằng chứng cũ. Môi trường Java/Python/Spark được kế thừa từ shell gọi lệnh.

Heap và shuffle là biến môi trường của runtime; chỉ chọn config `300k`
không tự đặt hai giá trị đó. Thiết lập môi trường Python/Java như README,
rồi đặt:

```bash
export LAKEHOUSE_DRIVER_MEMORY=4g
export LAKEHOUSE_SHUFFLE_PARTITIONS=16
```

Ví dụ chạy lại `300k` với output mới và giữ nguyên evidence trước đó. Cần
đổi **mọi đường dẫn công bố** trong config, không chỉ `--run-dir`/`--output`:

```bash
.venv/bin/python - <<'PY'
import json
from pathlib import Path
trial = Path("data/scale_repeats/300k_run_02")
trial.mkdir(parents=True, exist_ok=False)
config = json.loads(Path("config/scale_300k.json").read_text())
for key, name in {
    "manifest_output": "pipeline_run.json",
    "gold_summary_output": "gold_run.json",
    "benchmark_summary_output": "benchmark_run.json",
    "benchmark_report_output": "benchmark_report.md",
}.items():
    config[key] = str(trial / "published" / name)
(trial / "config.json").write_text(json.dumps(config, indent=2) + "\n")
PY
```

Sau đó bọc command E1. Nếu `300k_run_02` đã tồn tại, dùng tên khác nhất quán
trong cả hai đoạn lệnh; các file kết quả/log phải mới:

```bash
.venv/bin/python scripts/run_scale_trial.py \
  --output data/scale_repeats/300k_run_02/resources.json \
  --log data/scale_repeats/300k_run_02/pipeline.log \
  --pipeline-manifest data/scale_repeats/300k_run_02/published/pipeline_run.json \
  --timeout 1800 \
  --max-rss-gib 12 \
  --min-free-gib 12 \
  -- .venv/bin/python lakehouse_pipeline.py \
  --config data/scale_repeats/300k_run_02/config.json \
  --run-dir data/scale_repeats/300k_run_02/tables
```

Các ngưỡng là cấu hình của lần chạy, không phải yêu cầu tối thiểu của project.
Điều chỉnh theo RAM và dung lượng trống thực tế trước khi bắt đầu. Wrapper
không tự tăng giới hạn heap Java hay thay `master` của Spark. Nếu muốn chỉ
đo bước sinh mẫu hoặc một command độc lập, bỏ `--pipeline-manifest` và thay
command sau `--`.

Metrics được ghi như sau:

| Trường | Ý nghĩa |
|---|---|
| `status`, `returncode` | Kết quả của child command; nếu guard dừng sẽ có `stop_reason` |
| `wall_seconds` | Thời gian từ trước khi khởi chạy child đến khi wrapper thu kết quả; gồm Spark startup |
| `peak_process_tree_rss_bytes` | Giá trị lớn nhất quan sát được của tổng RSS Python và các descendant, gồm JVM |
| `sample_count`, `sampling_error_count` | Mức bao phủ và lỗi lấy mẫu tài nguyên |
| `disk_free_before_bytes`, `disk_free_min_bytes`, `disk_free_after_bytes` | Dung lượng trống filesystem trước, thấp nhất quan sát được và sau run |
| `pipeline.stages` | Trạng thái và thời gian từng stage lấy từ manifest mới do E1 sinh |
| `pipeline.counts` | Counts đã kiểm chứng trong manifest E1 |

Wrapper đọc `ps` mỗi giây theo mặc định (`--interval`), cộng RSS của PID
child và các PID có chuỗi parent nối tới child. Đây là **đỉnh quan sát theo
mẫu**, không phải phép đo liên tục hay riêng Java heap. Các trang nhớ chia
sẻ có thể bị tính nhiều lần và đỉnh rất ngắn giữa hai mẫu có thể bị bỏ lỡ.
RSS guard và disk guard là giới hạn phát hiện theo mẫu, không phải quota
của hệ điều hành. Guard chỉ gửi tín hiệu đến process group do wrapper tạo.

Chênh lệch dung lượng trống có thể bị ảnh hưởng bởi công việc khác chạy
cùng filesystem. Wall time toàn pipeline cũng không thay thế thời gian
query D2: query metrics phải lấy từ results D2 của đúng run. Không coi
`clearCache()` là việc làm lạnh cache hệ điều hành; kết quả local được
diễn giải theo chính sách cache và phần giới hạn trong báo cáo benchmark.

## Điều kiện để tăng mức tiếp theo

- Pipeline trả `status=passed`; mọi layout giữ nguyên dữ liệu và query result.
- RSS còn dư so với RAM khả dụng, guard không kích hoạt, không có lỗi đo RAM.
- Dung lượng trống còn đủ cho một bộ raw mới, các bản Delta và output tiếp theo.
- Query có số dòng khớp, còn nhiều file để kiểm tra pruning; các lượt đo có
  phân bố được công bố, kể cả khi một layout không nhanh hơn baseline.

Không tự xoá run cũ, bảng Silver chính hay dữ liệu của công việc khác để
tăng quy mô. Nếu không còn đủ tài nguyên, dừng ở mức đã kiểm chứng và ghi
rõ mức đó; dữ liệu lớn hơn không tự làm kết luận tốt hơn.

## Kết quả thực nghiệm

Tất cả ba mức đã **PASS toàn pipeline B1–E1**. Trong mỗi mức, 60 lượt đo D2
chính và 30 lượt đối chứng đều có kết quả đúng; mỗi query có dòng khớp,
mọi layout giữ nguyên dữ liệu của snapshot Silver. Tổng chuyến trong Gold
khớp Silver. Không run nào kích hoạt guard hoặc có lỗi lấy mẫu tài nguyên.

| Mức | Raw B1 | Silver sau C1/C2 | Nhóm Gold | Wall E1 (s) | Đỉnh RSS E1 (GiB) | File baseline → mỗi layout tối ưu |
|---|---:|---:|---:|---:|---:|---:|
| [300k](evidence/scaling/300k/pipeline_run.json) | 306,000 | 288,103 | 4,599 | 90.942 | 2.728 | 256 → 14 |
| [1020k](evidence/scaling/1020k/pipeline_run.json) | 1,040,400 | 979,303 | 5,342 | 140.845 | 2.963 | 256 → 42 |
| [3000k](evidence/scaling/3000k/pipeline_run.json) | 3,060,000 | 2,880,103 | 5,680 | 353.568 | 3.240 | 256 → 118 |

Dấu phẩy trong bảng là phân tách hàng nghìn, dấu chấm là phần thập phân.
Wall E1 gồm Spark startup, kiểm tra đúng dữ liệu, tạo layout và benchmark;
không gồm bước sinh raw. Các lần chạy giữ số core/heap như protocol, không
cố dùng hết tài nguyên máy. Mức 3 triệu kết thúc còn **38,88 GiB** disk trống.

| Mức | Sinh input (s) | Đỉnh RSS generator (GiB) | Evidence tài nguyên |
|---|---:|---:|---|
| 300k | 9.304 | 0.840 | [Generator](evidence/scaling/300k/generator_resources.json), [E1](evidence/scaling/300k/pipeline_resources.json) |
| 1020k | 18.483 | 1.126 | [Generator](evidence/scaling/1020k/generator_resources.json), [E1](evidence/scaling/1020k/pipeline_resources.json) |
| 3000k | 73.274 | 1.208 | [Generator](evidence/scaling/3000k/generator_resources.json), [E1](evidence/scaling/3000k/pipeline_resources.json) |

Mẫu ở cả ba mức bao phủ 744 / 672 / 743 giờ có dòng đủ điều kiện của
ba tháng. Hash, quota từng giờ và mức bao phủ nằm trong sampling manifest
được trỏ đến từ `pipeline_run.json`. Các báo cáo đầy đủ theo từng mức:
[300k](evidence/scaling/300k/benchmark_report.md),
[1020k](evidence/scaling/1020k/benchmark_report.md),
[3000k](evidence/scaling/3000k/benchmark_report.md).

### Benchmark lớn nhất: 2.880.103 dòng Silver

Bảng sau giữ đủ ba query và bốn điều kiện của mức 3 triệu. Mỗi ô thời gian
là thống kê của 5 lượt sau warmup; không bỏ lượt chậm. `Files` và `Bytes`
là median metrics thực đo của Spark trong query. Byte không phải dung lượng
đọc vật lý từ SSD vì cache hệ điều hành có thể phục vụ dữ liệu.

| Query | Layout | Median (ms) | Min–max (ms) | Stddev (ms) | Giảm median so baseline | Files | Bytes |
|---|---|---:|---:|---:|---:|---:|---:|
| Q1 | Baseline | 153.687 | 98.458–307.176 | 91.036 | 0.00% | 256 | 57,279,248 |
| Q1 | Z-ORDER 1 cột | 45.269 | 39.500–88.028 | 21.232 | 70.54% | 7 | 1,656,752 |
| Q1 | Z-ORDER 2 cột | 73.722 | 67.084–112.038 | 17.929 | 52.03% | 34 | 9,349,882 |
| Q1 | Liquid FULL | 70.937 | 51.620–147.565 | 38.745 | 53.84% | 35 | 10,285,892 |
| Q2 | Baseline | 177.631 | 137.674–282.324 | 60.784 | 0.00% | 256 | 90,853,648 |
| Q2 | Z-ORDER 1 cột | 38.684 | 31.069–47.782 | 5.932 | 78.22% | 7 | 3,532,376 |
| Q2 | Z-ORDER 2 cột | 24.313 | 22.276–26.283 | 1.606 | 86.31% | 3 | 1,205,110 |
| Q2 | Liquid FULL | 25.234 | 21.094–28.184 | 2.660 | 85.79% | 2 | 1,122,916 |
| Q3 | Baseline | 125.493 | 104.603–139.251 | 13.852 | 0.00% | 256 | 57,279,248 |
| Q3 | Z-ORDER 1 cột | 48.566 | 42.813–72.038 | 11.520 | 61.30% | 25 | 6,341,360 |
| Q3 | Z-ORDER 2 cột | 61.225 | 58.627–102.713 | 18.462 | 51.21% | 66 | 19,393,332 |
| Q3 | Liquid FULL | 59.422 | 57.646–63.433 | 2.142 | 52.65% | 63 | 18,522,376 |

Q1 lọc `PULocationID = 161`, khớp **134.289 chuyến**. Q2 thêm khoảng ngày
`2025-01-23 00:00:00 ≤ pickup < 2025-01-24 00:00:00`, khớp **2.309 chuyến**.
Q3 lọc PU trong `[146, 176]` và DO trong `[222, 252]`, khớp **176.001 chuyến**.
Chi tiết các lượt và predicate nằm trong [JSON D2](evidence/scaling/3000k/benchmark_run.json).

Z-ORDER 1 cột giảm median Q1 **70,54%**, từ 153,687 xuống 45,269 ms, đồng
thời giảm file scan 256 → 7 và byte đọc 57.279.248 → 1.656.752. Q2 của
Z-ORDER 2 cột giảm **86,31%**, từ 177,632 xuống 24,313 ms, scan 256 → 3 file.
Liquid FULL ở Q2 scan 2 file nhưng median 25,234 ms; ít file hơn không tự
động đồng nghĩa latency thấp hơn trong một run ngắn.

Có biến động rõ ở một số nhóm: Q1 baseline trải từ 98,458 đến 307,176 ms,
stddev 91,036 ms. Chỉ có 5 lượt và thứ tự layout cố định, nên các phần trăm
là kết quả mô tả của máy/run này, không phải khoảng tin cậy hay cam kết
cải thiện trên mọi máy. Cache OS ấm, scheduling/JIT và compaction cùng ảnh
hưởng. Phase compaction có median riêng; không trộn vào bảng bốn điều kiện.

Cả ba layout tối ưu còn **118 file**, so với baseline 256 file. Thời gian
chuẩn bị tối ưu lần lượt khoảng **9,779 / 8,046 / 7,677 giây** cho Z-ORDER
1 cột / Z-ORDER 2 cột / liquid FULL; chi phí này nằm ngoài thời gian query
trong bảng, nhưng có trong wall time toàn E1. Các file target 1 MiB phục vụ
phép thử này, không phải đề xuất kích thước file production.

### Đối chiếu độc lập và provenance

[Validator PyArrow/Python Decimal](evidence/scaling/validate_scale_results.py)
đã đọc trực tiếp các Parquet active của Silver version 2, không import Spark
hay code xử lý dữ liệu của project, và kiểm kết quả COUNT/AVG/SUM của cả
90 lượt đo cùng 18 warmup mỗi mức. Cả ba đối chiếu đều PASS:
[300k](evidence/scaling/300k/independent_validation.json),
[1020k](evidence/scaling/1020k/independent_validation.json),
[3000k](evidence/scaling/3000k/independent_validation.json).

Manifest [validation](evidence/scaling/validation.json) lưu code hash và
danh tính evidence của lần mở rộng; [JUnit](evidence/scaling/tests.xml)
lưu kết quả bộ test cuối. Đây là revision có sampler và xử lý quy mô lớn
đã cập nhật; evidence demo cũ trong `docs/evidence/e1/` gắn với các code hash
lịch sử riêng. Không chuyển nhãn thành công của một revision cho bytes của
revision khác. Bằng chứng mở rộng dùng các đường dẫn `docs/evidence/scaling/`.

### Quy mô dùng cho bài nộp

Dùng **3 triệu dòng cơ sở** (3.060.000 raw, 2.880.103 Silver cuối) cho phần
benchmark chính: mức này đã chạy đầy đủ và có 118 file sau tối ưu để quan sát
pruning. Dùng **1,02 triệu dòng cơ sở** khi cần kiểm tra thay đổi thường xuyên,
và fixture **3.060 raw** cho demo nhanh cùng các invariant của phân công.
Đây là mức lớn nhất đã kiểm chứng trong đợt này, không phải giới hạn phần
cứng tối đa hay phép đo trên cluster production.
