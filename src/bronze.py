import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from delta import configure_spark_with_delta_pip

def run_bronze(
    spark: SparkSession, 
    raw_dir: str = "data/raw", 
    bronze_dir: str = "data/bronze/taxi_trips"
) -> dict:
    """
    Nhiệm vụ B2: Đọc dữ liệu JSON thô, thêm 3 cột metadata và ghi vào Delta Bronze Table.
    Giữ nguyên 100% bản ghi lỗi (tuyệt đối không filter).

    Tham số:
        spark (SparkSession): Phiên làm việc Spark đang chạy.
        raw_dir (str): Đường dẫn thư mục chứa dữ liệu JSON thô.
        bronze_dir (str): Đường dẫn lưu trữ bảng Delta Bronze.

    Trả về:
        dict: Tóm tắt kết quả nghiệm thu {"bronze_count": int}
    """
    print("=== [TASK B2] BẮT ĐẦU TIẾN TRÌNH BRONZE INGEST ===")

    # Danh sách 3 batch dữ liệu thô
    batch_files = ["batch_01.json", "batch_02.json", "batch_03.json"]

    # Lặp qua từng batch file để nạp vào Bronze
    for batch_file in batch_files:
        file_path = os.path.join(raw_dir, batch_file)
        batch_id = os.path.splitext(batch_file)[0]  # Trích xuất 'batch_01' từ 'batch_01.json'
        
        print(f"--> Đang nạp file nguồn: {file_path} (Batch ID: {batch_id})...")

        # Đọc dữ liệu Line-delimited JSON (Mỗi dòng là 1 record -> KHÔNG dùng multiline=true)
        df_raw = spark.read.json(file_path)

        # Bổ sung 3 cột Metadata bắt buộc
        df_bronze_batch = df_raw \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_source_file", input_file_name()) \
            .withColumn("_batch_id", lit(batch_id))

        # Ghi Append-only vào Delta Table và bật mergeSchema cho phép mở rộng schema sau này
        df_bronze_batch.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(bronze_dir)

    # Đọc lại bảng Delta Bronze vừa tạo để kiểm tra và nghiệm thu
    print("\n=== ĐANG KIỂM TRA VÀ NGHIỆM THU BẢNG BRONZE ===")
    df_bronze_result = spark.read.format("delta").load(bronze_dir)
    bronze_count = df_bronze_result.count()

    # Kiểm tra số lượng bản ghi bị khuyết (NULL) ở các cột Metadata
    null_metadata_count = df_bronze_result.filter(
        "_ingest_ts IS NULL OR _source_file IS NULL OR _batch_id IS NULL"
    ).count()

    print(f"-> Tổng số bản ghi (bronze_count): {bronze_count}")
    print(f"-> Số bản ghi bị thiếu thông tin metadata: {null_metadata_count}")

    # Đánh giá tiêu chuẩn nghiệm thu B2
    if bronze_count == 3060 and null_metadata_count == 0:
        print("\n[SUCCESS] NGHIỆM THU THÀNH CÔNG (B2): Đạt đủ 3060 bản ghi và 3 cột metadata có giá trị ở mọi dòng!")
    else:
        print(f"\n[WARNING] Kết quả chưa đạt mục tiêu (Kỳ vọng: 3060 dòng, Thực tế: {bronze_count} dòng).")

    return {"bronze_count": bronze_count}


if __name__ == "__main__":
    # 1. Khai báo Builder cấu hình SparkSession
    builder = SparkSession.builder \
        .appName("Bronze_Ingest_Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "2")

    # 2. Khởi tạo phiên làm việc Spark cùng gói Delta Spark 3.2.0
    spark = configure_spark_with_delta_pip(
        builder, 
        extra_packages=["io.delta:delta-spark_2.12:3.2.0"]
    ).getOrCreate()

    # Giảm bớt mức độ log dư thừa trên console
    spark.sparkContext.setLogLevel("WARN")

    # 3. Thực thi tiến trình nạp dữ liệu Bronze
    run_bronze(spark)