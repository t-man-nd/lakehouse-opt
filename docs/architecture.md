# Kiến trúc A1–E1 và ranh giới transaction

```mermaid
flowchart TD
    TLC["TLC Yellow Taxi Parquet · Jan–Mar 2025"] --> B1["B1 generator · contract + lỗi seed 42"]
    B1 --> JSON["batch_01/02/03.json · 3.060 records"]
    JSON --> B2["B2 Bronze append-only · raw + provenance"]
    B2 --> B3["B3 try_cast + bốn rule B1"]
    B3 --> REJECT["Quarantine · 180 + reject_reason"]
    B3 --> SILVER["Silver v0 · 2.880"]
    CDC["Fixture CDC · 5 update, 3 insert"] --> MERGE["C1 MERGE INTO"]
    SILVER --> MERGE
    MERGE --> S1["Silver v1 · 2.883"]
    NEW["batch_04 · 100 + surcharge_fee giả lập"] --> EB["C2 Bronze append + mergeSchema"]
    B2 --> EB
    EB --> ES["C2 chỉ batch mới → Silver append"]
    S1 --> ES
    ES --> S2["Silver v2 · 2.983"]
    S2 --> C3["C3 history / versionAsOf / annotated log"]
    S2 --> COPY["Copy vật lý riêng → VACUUM demo"]
    S2 --> GOLD["D1 Gold · zone × UTC hour of day"]
    S2 --> D2["D2 các bản sao cùng snapshot"]
    D2 --> BASE["1 baseline"]
    D2 --> Z1["2 Z-ORDER(PU)"]
    D2 --> Z2["3 Z-ORDER(PU, pickup time)"]
    D2 --> LIQ["4 CLUSTER BY + OPTIMIZE FULL"]
    D2 --> OPT["Compaction đối chiếu bổ sung"]
    C3 --> E1["E1 manifest + log từng stage"]
    COPY --> E1
    GOLD --> E1
    BASE --> E1
    Z1 --> E1
    Z2 --> E1
    LIQ --> E1
    OPT --> E1
```

Version ở trên áp dụng cho Silver trong một run mới theo thứ tự cố định.
Bronze có version riêng: ba append đầu tiên là v0/v1/v2, schema change C2 ở v3.
Gold và mỗi bảng benchmark là các Delta table riêng; không cộng version giữa
các bảng. E1 điều phối nhiều transaction, không tạo một transaction ACID xuyên
Bronze, Silver, quarantine, Gold và benchmark.

```mermaid
sequenceDiagram
    participant R as Reader
    participant W as Writer
    participant D as Data files
    participant L as Delta log
    R->>L: Đọc snapshot vN
    W->>L: Đọc vN và metadata
    W->>D: Ghi Parquet mới chưa thuộc snapshot công bố
    W->>L: Kiểm tra commit mới / xung đột
    alt Không xung đột
      W->>L: Commit nguyên tử vN+1 (add/remove/metadata)
    else Xung đột
      L-->>W: Thất bại transaction; cần retry phù hợp
    end
    R->>D: Tiếp tục đọc các file của snapshot vN
```

Đây là mô hình giải thích optimistic concurrency và snapshot isolation;
project kiểm chứng history theo thứ tự, không tự nhận đã chạy stress test nhiều
writer. Reader lịch sử cần cả log lẫn file dữ liệu của version đó. `remove` chỉ
loại file khỏi snapshot mới; VACUUM mới có thể xóa file vật lý hết retention.
[Delta concurrency control](https://docs.delta.io/concurrency-control/).

Bronze giữ provenance để reprocess; Silver áp contract chung; Gold phục vụ chỉ
số nghiệp vụ. Mỗi hop tăng I/O, độ trễ và điểm có thể lỗi. Một Silver chỉ phản
chiếu schema của producer sẽ khiến consumer vẫn phải tự thống nhất định nghĩa.
Vì vậy các lớp phải có contract và mục đích phân tích rõ ràng, không chỉ đổi tên
thư mục. [REPORT mục 2–3](../REPORT.md) trình bày nguồn học thuật và giới hạn.
