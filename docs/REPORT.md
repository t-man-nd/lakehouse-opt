# Báo cáo kỹ thuật của nhóm

Bản báo cáo chính được duy trì tại **[REPORT.md ở root](../REPORT.md)** để
tránh hai bản có số liệu và trạng thái khác nhau. File này là mục lục dẫn đến
bản đó; khung đề mục ban đầu của nhóm đã được hợp nhất vào các phần dưới đây.

| Yêu cầu | Phần trong báo cáo chính | Evidence / tài liệu chi tiết |
|---|---|---|
| Bối cảnh, mục tiêu, rubric | 1 | [Milestone status](MILESTONE_STATUS.md) |
| Warehouse / Lake / Lakehouse, Delta | 2 | Armbrust et al., CIDR 2021 được dẫn trong report |
| Medallion và phản biện | 3 | [Architecture](architecture.md) |
| Nguồn, contract, Bronze/Silver | 4 | [Manifest](source_data_manifest.md), [B3](B3_SILVER.md) |
| CDC / MERGE | 5 | [C1](C1_CDC.md) |
| Schema evolution | 6 | [C2](C2_SCHEMA_EVOLUTION.md) |
| Delta log, ACID/OCC/snapshot, time travel, VACUUM | 7 | [C3](C3_TIME_TRAVEL.md) |
| Tái lập và provenance | 8 | [E1 runbook](E1_RUNBOOK.md) |
| Lấy mẫu phân tầng và benchmark nhiều quy mô | 8 và phần mở rộng riêng | [Scaling](SCALING.md) |
| Gold aggregation | 9 | [D1](D1_GOLD.md) |
| OPTIMIZE, Z-ORDER, liquid clustering, benchmark, threats | 10 | [D2](D2_PERFORMANCE.md) |
| Tích hợp E1 và ranh giới E2 | 11 | [pipeline_run.json](pipeline_run.json) |
| Slides / rehearsal F1 | 12 | [Khung A1](SLIDE_OUTLINE.md) |
| Nguồn tham khảo | 13 và citation tại đoạn sử dụng | CIDR, Delta Lake, TLC |

Các output và ảnh của run cũ được giữ nguyên tên và ngày. Khi trình bày một
con số, dẫn đúng manifest của run tạo ra con số đó, không trộn baseline B3
2.880 dòng với Silver sau C1/C2 2.983 dòng.
