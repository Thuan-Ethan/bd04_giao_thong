"""
spark_jobs/ingest/nap_thang.py

Nạp 1 tháng dữ liệu Yellow Taxi (Parquet trên HDFS) vào bảng Delta bronze.chuyen_di.
Idempotent: chạy lại cùng một tháng không làm nhân đôi dữ liệu.

Cách chạy (trên hadoop-master):
    spark-submit --master yarn spark_jobs/ingest/nap_thang.py --nam 2024 --thang 1

Luồng xử lý:
    1. Đọc file Parquet của tháng
    2. Chuẩn hóa kiểu 5 cột (để các năm dùng chung một schema Delta)
    3. Sinh khóa trip_id (SHA-256 của 9 cột) + cột dup_seq, nam, thang, source_file, ingested_at
       Các dòng giống hệt nhau (bản ghi lặp của TLC) được giữ lại và đánh số dup_seq = 1, 2, ...
       trip_id của dòng đầu tiên không đổi, từ dòng thứ hai trở đi được băm thêm dup_seq
    4. Kiểm tra trùng khóa trong tháng -> dừng nếu vẫn còn trùng
    5. Bảng chưa có -> tạo mới; đã có -> MERGE INTO (chỉ chèn dòng chưa tồn tại)
    6. Đối chiếu số dòng nguồn với số dòng trong bảng cho đúng tháng đó
"""
import argparse
import sys

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAW_DIR = "hdfs://hadoop-master:9000/data/raw/tlc/yellow"
BRONZE_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/bronze/chuyen_di"

# Kiểu chuẩn theo dữ liệu 2024-2025. Năm 2022-2023 lưu các cột này kiểu bigint/double
# nên phải ép về cùng kiểu trước khi ghi vào một bảng Delta duy nhất.
INT_COLS = ["VendorID", "PULocationID", "DOLocationID"]
LONG_COLS = ["passenger_count", "RatecodeID"]

# 9 cột tạo khóa. 5 cột đầu chưa đủ vì TLC có các cặp bản ghi trùng 5 cột nhưng
# khác quãng đường/cước (xem docs). Không dùng dropDuplicates ở bronze để tránh mất dữ liệu:
# bản ghi lặp hoàn toàn được đánh dup_seq, việc loại bỏ (kèm thống kê) làm ở bronze_to_silver.
KEY_COLS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "PULocationID", "DOLocationID", "passenger_count",
    "trip_distance", "fare_amount", "total_amount",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Nạp 1 tháng Yellow Taxi vào bronze.chuyen_di")
    parser.add_argument("--nam", type=int, required=True, help="Năm, ví dụ 2024")
    parser.add_argument("--thang", type=int, required=True, choices=range(1, 13),
                        metavar="[1-12]", help="Tháng, 1-12")
    return parser.parse_args()


def build_spark(year, month):
    return (
        SparkSession.builder
        .appName(f"nap_thang_{year}_{month:02d}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Cho phép MERGE tự thêm cột mới (cbd_congestion_fee của 2025)
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )


def path_exists(spark, path):
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(jvm_path)


def latest_history(spark):
    return (
        spark.sql(f"DESCRIBE HISTORY delta.`{BRONZE_PATH}` LIMIT 1")
        .select("version", "operation", "operationMetrics")
        .collect()[0]
    )


def normalize_types(df):
    for col in INT_COLS:
        df = df.withColumn(col, F.col(col).cast("int"))
    for col in LONG_COLS:
        df = df.withColumn(col, F.col(col).cast("long"))
    return df


def add_trip_id(df):
    data_cols = df.columns
    # coalesce: NULL thành chuỗi "NULL" để không bị concat_ws bỏ qua âm thầm
    # "|" làm dấu phân cách để (1, 23) và (12, 3) không cho cùng một chuỗi
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in KEY_COLS]
    df = df.withColumn("trip_id", F.sha2(F.concat_ws("|", *parts), 256))

    # Đánh số các dòng cùng khóa. Sắp xếp theo mọi cột dữ liệu để thứ tự luôn xác định,
    # nhờ đó chạy lại cho ra đúng cùng trip_id (idempotent)
    window = Window.partitionBy("trip_id").orderBy(*[F.col(c) for c in data_cols])
    df = df.withColumn("dup_seq", F.row_number().over(window))

    # Dòng đầu tiên giữ nguyên trip_id; dòng lặp thứ 2, 3, ... băm thêm dup_seq
    return df.withColumn(
        "trip_id",
        F.when(F.col("dup_seq") == 1, F.col("trip_id"))
         .otherwise(F.sha2(F.concat_ws("|", F.col("trip_id"), F.col("dup_seq").cast("string")), 256)),
    )


def main():
    args = parse_args()
    year, month = args.nam, args.thang
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    source_path = f"{RAW_DIR}/{file_name}"

    spark = build_spark(year, month)
    exit_code = 0
    df = None

    try:
        if not path_exists(spark, source_path):
            print(f"[LOI] Không thấy file nguồn: {source_path}")
            return 1

        print(f"[INFO] Đọc {source_path}")
        df = (
            add_trip_id(normalize_types(spark.read.parquet(source_path)))
            # nam/thang lấy từ tham số, không lấy từ ngày đón: file TLC thường lẫn
            # vài dòng có ngày nằm ngoài tháng, lấy từ dữ liệu sẽ làm sai partition
            .withColumn("nam", F.lit(year).cast("int"))
            .withColumn("thang", F.lit(month).cast("int"))
            .withColumn("source_file", F.lit(file_name))
            .withColumn("ingested_at", F.current_timestamp())
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        source_count = df.count()
        repeated_rows = df.filter(F.col("dup_seq") > 1).count()
        dup_groups = df.groupBy("trip_id").count().filter("count > 1").count()
        print(f"[INFO] Số dòng nguồn: {source_count:,} | dòng lặp hoàn toàn (dup_seq > 1): "
              f"{repeated_rows:,} | nhóm trùng trip_id: {dup_groups:,}")

        if dup_groups > 0:
            print("[LOI] Vẫn còn trip_id trùng sau khi đánh dup_seq, dừng để không ghi dữ liệu sai.")
            return 1

        version_before = None
        if not path_exists(spark, BRONZE_PATH + "/_delta_log"):
            print(f"[INFO] Bảng bronze chưa tồn tại, tạo mới tại {BRONZE_PATH}")
            (df.write.format("delta")
               .partitionBy("nam", "thang")
               .save(BRONZE_PATH))
        else:
            print("[INFO] MERGE INTO bronze.chuyen_di (chỉ chèn dòng chưa có)")
            version_before = latest_history(spark)["version"]
            df.createOrReplaceTempView("source_month")
            spark.sql(f"""
                MERGE INTO delta.`{BRONZE_PATH}` AS t
                USING source_month AS s
                ON  t.nam = s.nam AND t.thang = s.thang AND t.trip_id = s.trip_id
                WHEN NOT MATCHED THEN INSERT *
            """)

        # Chỉ số của lần ghi vừa rồi (best-effort, không ảnh hưởng kết quả nạp)
        try:
            last = latest_history(spark)
            if version_before is not None and last["version"] == version_before:
                # MERGE không có dòng nào để chèn nên Delta không tạo commit mới
                print(f"[INFO] MERGE không chèn dòng nào, bảng giữ nguyên version {last['version']}")
            else:
                print(f"[INFO] {last['operation']} (version {last['version']}): "
                      f"{dict(last['operationMetrics'])}")
        except Exception as exc:  # noqa: BLE001
            print(f"[CANH BAO] Không đọc được lịch sử Delta: {exc}")

        table_count = (
            spark.read.format("delta").load(BRONZE_PATH)
            .filter((F.col("nam") == year) & (F.col("thang") == month))
            .count()
        )
        print(f"[INFO] Số dòng trong bảng cho {year}-{month:02d}: {table_count:,}")

        if table_count != source_count:
            print(f"[LOI] Lệch số dòng: nguồn {source_count:,} vs bảng {table_count:,}")
            exit_code = 1
        else:
            print(f"[OK] {year}-{month:02d} đã nạp xong, số dòng khớp.")
    finally:
        if df is not None:
            df.unpersist()
        spark.stop()

    return exit_code


if __name__ == "__main__":
    sys.exit(main())