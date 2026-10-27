"""
spark_jobs/ingest/nap_thang.py

Nạp 1 tháng Yellow Taxi (Parquet trên HDFS) vào bảng Delta bronze.chuyen_di.

Yêu cầu:
  - Giữ nguyên schema từng tháng, hợp nhất bằng schema evolution (mergeSchema)
  - Phân vùng theo nam, thang (generated column sinh từ thời điểm đón)
  - Idempotent: chạy lại cùng tháng thì COUNT(*) không đổi (MERGE theo ma_chuyen)

Chạy:
  spark-submit spark_jobs/ingest/nap_thang.py --nam 2023 --thang 1
"""
import argparse

from pyspark.sql import SparkSession, Window, functions as F
from delta.tables import DeltaTable

HDFS = "hdfs://master:9000"
RAW_DIR = f"{HDFS}/data/tlc_raw/yellow"
BRONZE_PATH = f"{HDFS}/lakehouse/bd04_giao_thong/bronze/chuyen_di"

PICKUP_COL = "tpep_pickup_datetime"
DROPOFF_COL = "tpep_dropoff_datetime"

# Các cột dùng để tạo ma_chuyen. TLC không có mã chuyến nên phải tự sinh bằng hash.
# Chỉ chọn cột có ở mọi tháng. Cột nào thay đổi giữa các năm (airport_fee,
# congestion_surcharge...) thì không đưa vào để mã không đổi khi schema đổi.
KEY_COLS = [
    "VendorID", PICKUP_COL, DROPOFF_COL, "PULocationID", "DOLocationID",
    "trip_distance", "fare_amount", "total_amount", "payment_type",
]


def parse_args():
    p = argparse.ArgumentParser(description="Nạp 1 tháng Yellow Taxi vào bronze.chuyen_di")
    p.add_argument("--nam", type=int, required=True, help="Ví dụ 2023")
    p.add_argument("--thang", type=int, required=True, help="1..12")
    args = p.parse_args()
    if not 1 <= args.thang <= 12:
        p.error("--thang phải từ 1 đến 12")
    return args


def duong_dan_input(nam, thang):
    return f"{RAW_DIR}/{nam}/yellow_tripdata_{nam}-{thang:02d}.parquet"


def kiem_tra_cot_khoa(df):
    """Báo lỗi rõ ràng nếu tháng này thiếu cột dùng để sinh ma_chuyen."""
    co = {c.lower() for c in df.columns}
    thieu = [c for c in KEY_COLS if c.lower() not in co]
    if thieu:
        raise ValueError(f"File thiếu cột cần cho ma_chuyen: {thieu}. Cột hiện có: {df.columns}")


def thong_ke_nguon(df, nam, thang):
    """In thống kê file nguồn. Không sửa dữ liệu (bronze giữ nguyên bản gốc)."""
    tong = df.count()
    trong_thang = df.filter(
        (F.year(PICKUP_COL) == nam) & (F.month(PICKUP_COL) == thang)
    ).count()
    print(f"[NGUON] tổng dòng: {tong:,}")
    print(f"[NGUON] dòng có thời điểm đón NGOÀI {nam}-{thang:02d}: {tong - trong_thang:,} "
          f"(giữ nguyên ở bronze, lọc ở silver)")
    return tong


def them_ma_chuyen(df):
    """
    ma_chuyen = sha256(co_so | stt)
      - co_so: hash các KEY_COLS (NULL thay bằng chuỗi rỗng để tránh va chạm do concat_ws bỏ NULL)
      - stt  : số thứ tự trong nhóm có cùng co_so, đánh theo hash toàn dòng
    Nhờ có stt, hai chuyến giống hệt nhau trên KEY_COLS vẫn có mã khác nhau, nên không mất dòng nào,
    và chạy lại cùng file luôn cho cùng tập mã (idempotent).
    """
    cot_goc = df.columns

    def chuoi(c):
        return F.coalesce(F.col(c).cast("string"), F.lit(""))

    co_so = F.sha2(F.concat_ws("|", *[chuoi(c) for c in KEY_COLS]), 256)
    ca_dong = F.sha2(F.concat_ws("|", *[chuoi(c) for c in cot_goc]), 256)

    w = Window.partitionBy("_co_so").orderBy("_ca_dong")
    return (
        df.withColumn("_co_so", co_so)
          .withColumn("_ca_dong", ca_dong)
          .withColumn("_stt", F.row_number().over(w))
          .withColumn("ma_chuyen",
                      F.sha2(F.concat_ws("|", F.col("_co_so"), F.col("_stt").cast("string")), 256))
          .drop("_co_so", "_ca_dong", "_stt")
    )


def tao_bang_neu_chua_co(spark, df):
    """Tạo bảng Delta rỗng với generated column nam, thang và phân vùng theo hai cột này."""
    if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        return False

    b = DeltaTable.createIfNotExists(spark).location(BRONZE_PATH)
    for f in df.schema.fields:
        b = b.addColumn(f.name, f.dataType)
    b = b.addColumn("nam", "INT", generatedAlwaysAs=f"YEAR(`{PICKUP_COL}`)")
    b = b.addColumn("thang", "INT", generatedAlwaysAs=f"MONTH(`{PICKUP_COL}`)")
    b.partitionedBy("nam", "thang").execute()
    print(f"[BANG] đã tạo bảng mới tại {BRONZE_PATH}")
    return True


def can_chinh_kieu(spark, df):
    """
    Nếu một cột đã có trong bảng nhưng tháng này đọc ra kiểu khác (ví dụ passenger_count
    lúc long, lúc double), ép về kiểu của bảng để MERGE không lỗi. In cảnh báo để bạn ghi
    vào bảng đối chiếu schema trong báo cáo.
    """
    kieu_bang = {f.name.lower(): f.dataType
                 for f in spark.read.format("delta").load(BRONZE_PATH).schema.fields}
    for f in df.schema.fields:
        kieu_cu = kieu_bang.get(f.name.lower())
        if kieu_cu is not None and kieu_cu != f.dataType:
            print(f"[SCHEMA] cột {f.name}: file là {f.dataType.simpleString()}, "
                  f"bảng là {kieu_cu.simpleString()} -> ép về kiểu của bảng")
            df = df.withColumn(f.name, F.col(f.name).cast(kieu_cu))
    return df


def nap_bang_merge(spark, df):
    """MERGE chỉ chèn: bản ghi đã có ma_chuyen thì bỏ qua, nên chạy lại không nhân đôi."""
    # Cho phép MERGE tự thêm cột mới của tháng này vào bảng (schema evolution).
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    nguon = (df.withColumn("nam", F.year(PICKUP_COL))
               .withColumn("thang", F.month(PICKUP_COL)))

    bang = DeltaTable.forPath(spark, BRONZE_PATH)
    (bang.alias("t")
         .merge(nguon.alias("s"), "t.ma_chuyen = s.ma_chuyen")
         .whenNotMatchedInsertAll()
         .execute())

    metrics = bang.history(1).select("operationMetrics").first()[0]
    return int(metrics.get("numTargetRowsInserted", 0))


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName(f"nap_thang_{args.nam}_{args.thang:02d}")
        # Đọc timestamp TLC thành TIMESTAMP thường (không phải NTZ) để generated column hoạt động.
        .config("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
        # TLC lưu giờ địa phương không kèm múi giờ. Dùng UTC để YEAR/MONTH khớp giờ gốc,
        # không bị lệch sang tháng khác theo múi giờ của máy (Asia/Saigon).
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    path = duong_dan_input(args.nam, args.thang)
    print(f"[INPUT] {path}")

    df = spark.read.parquet(path)
    print("[SCHEMA FILE]")
    df.printSchema()
    kiem_tra_cot_khoa(df)
    tong_nguon = thong_ke_nguon(df, args.nam, args.thang)

    df = them_ma_chuyen(df)
    vua_tao = tao_bang_neu_chua_co(spark, df)
    if not vua_tao:
        df = can_chinh_kieu(spark, df)

    so_chen = nap_bang_merge(spark, df)

    bang = spark.read.format("delta").load(BRONZE_PATH)
    tong_bang = bang.count()
    tong_thang = bang.filter((F.col("nam") == args.nam) & (F.col("thang") == args.thang)).count()
    print("=" * 60)
    print(f"[KET QUA] file nguồn           : {tong_nguon:,} dòng")
    print(f"[KET QUA] dòng được chèn lần này: {so_chen:,}")
    print(f"[KET QUA] tổng dòng của bảng    : {tong_bang:,}")
    print(f"[KET QUA] dòng của phân vùng {args.nam}-{args.thang:02d}: {tong_thang:,}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()