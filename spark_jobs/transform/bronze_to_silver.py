"""
spark_jobs/transform/bronze_to_silver.py

Chuẩn hóa bronze.chuyen_di -> silver.chuyen_di theo đúng đặc tả đề cương (mục 4.4, 4.7, 4.8).
Bản ghi vùng đón/trả ngoài 1-263 ("Unknown"/264/265) đi vào _quarantine.chuyen_di thay vì
bị xóa. Các luật khác nếu vi phạm thì loại hẳn (không lưu ở đâu, chỉ đếm vào thống kê).

Idempotent: silver *tính lại toàn bộ* một tháng mỗi lần chạy (không MERGE như bronze),
nên ghi đè đúng partition (nam, thang) bằng replaceWhere.

nam, thang, ngay là Delta generated column, tính từ thoi_diem_don -- không nằm trong
DataFrame khi ghi, Delta tự tính. Nhờ vậy partition pruning vẫn hoạt động khi truy vấn
lọc theo thoi_diem_don (Delta tự suy ra khoảng nam/thang liên quan), như đề cương yêu cầu
chứng minh (mục 4.7).

Cách chạy (trên hadoop-master):
    spark-submit --master yarn spark_jobs/transform/bronze_to_silver.py --nam 2024 --thang 1
"""
import argparse
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/bronze/chuyen_di"
SILVER_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/silver/chuyen_di"
QUARANTINE_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/_quarantine/chuyen_di"
STATS_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/silver/thong_ke_loai_bo"

VUNG_MIN, VUNG_MAX = 1, 263  # 264/265 la "Unknown"/ngoai NYC -> quarantine, khong phai loi

RULES_LOAI = [
    ("dup_seq > 1", "ban_ghi_lap_hoan_toan"),
    ("thoi_diem_tra <= thoi_diem_don", "thoi_diem_tra_khong_sau_don"),
    ("thoi_gian_phut < 1 OR thoi_gian_phut > 300", "thoi_gian_phut_ngoai_1_300"),
    ("quang_duong_mile <= 0 OR quang_duong_mile > 100", "quang_duong_ngoai_0_100"),
    ("van_toc_tb_mph > 80", "toc_do_tb_qua_80"),
    ("so_khach = 0 OR so_khach > 8", "so_khach_ngoai_1_8"),
    ("tong_tien < 0", "tong_tien_am"),
]
PAYMENT_MAP = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided trip"}


def parse_args():
    parser = argparse.ArgumentParser(description="Chuan hoa + lam sach bronze -> silver theo dac ta de cuong")
    parser.add_argument("--nam", type=int, required=True)
    parser.add_argument("--thang", type=int, required=True, choices=range(1, 13), metavar="[1-12]")
    return parser.parse_args()


def build_spark(year, month):
    return (
        SparkSession.builder
        .appName(f"bronze_to_silver_{year}_{month:02d}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def path_exists(spark, path):
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(jvm_path)


def payment_label(col):
    mapping = F.create_map(*[x for k, v in PAYMENT_MAP.items() for x in (F.lit(k), F.lit(v))])
    return F.coalesce(mapping[col], F.concat(F.lit("Unknown("), col.cast("string"), F.lit(")")))


def select_columns(df):
    return df.select(
        F.col("trip_id").alias("ma_chuyen"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("thoi_diem_don"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("thoi_diem_tra"),
        F.col("PULocationID").alias("vung_don"),
        F.col("DOLocationID").alias("vung_tra"),
        F.col("passenger_count").cast("int").alias("so_khach"),
        F.col("trip_distance").alias("quang_duong_mile"),
        (
            (F.col("tpep_dropoff_datetime").cast("timestamp").cast("long")
             - F.col("tpep_pickup_datetime").cast("timestamp").cast("long")) / 60.0
        ).alias("thoi_gian_phut"),
        F.col("total_amount").alias("tong_tien"),
        payment_label(F.col("payment_type")).alias("hinh_thuc_tt"),
        F.col("dup_seq").alias("_dup_seq"),
    ).withColumn(
        "van_toc_tb_mph",
        F.when(F.col("thoi_gian_phut") > 0,
               F.col("quang_duong_mile") / (F.col("thoi_gian_phut") / 60.0))
    )


def ensure_table(spark, path, add_ngay):
    if path_exists(spark, path + "/_delta_log"):
        return
    builder = (
        DeltaTable.createOrReplace(spark)
        .location(path)
        .addColumn("ma_chuyen", "STRING")
        .addColumn("thoi_diem_don", "TIMESTAMP")
        .addColumn("thoi_diem_tra", "TIMESTAMP")
        .addColumn("vung_don", "INT")
        .addColumn("vung_tra", "INT")
        .addColumn("so_khach", "INT")
        .addColumn("quang_duong_mile", "DOUBLE")
        .addColumn("thoi_gian_phut", "DOUBLE")
        .addColumn("van_toc_tb_mph", "DOUBLE")
        .addColumn("tong_tien", "DOUBLE")
        .addColumn("hinh_thuc_tt", "STRING")
        .addColumn("nam", "INT", generatedAlwaysAs="YEAR(thoi_diem_don)")
        .addColumn("thang", "INT", generatedAlwaysAs="MONTH(thoi_diem_don)")
    )
    if add_ngay:
        builder = builder.addColumn("ngay", "DATE", generatedAlwaysAs="CAST(thoi_diem_don AS DATE)")
    builder.partitionedBy("nam", "thang").execute()


def main():
    args = parse_args()
    year, month = args.nam, args.thang
    spark = build_spark(year, month)
    exit_code = 0

    try:
        if not path_exists(spark, BRONZE_PATH + "/_delta_log"):
            print("[LOI] Chua co bronze.chuyen_di, chay nap_thang.py truoc.")
            return 1

        print(f"[INFO] Doc bronze {year}-{month:02d}")
        raw = (
            spark.read.format("delta").load(BRONZE_PATH)
            .filter((F.col("nam") == year) & (F.col("thang") == month))
        )
        total = raw.count()
        if total == 0:
            print(f"[LOI] Khong co dong nao trong bronze cho {year}-{month:02d}.")
            return 1
        print(f"[INFO] Tong so dong bronze: {total:,}")

        df = select_columns(raw)

        rule_counts = {}
        loai_bo = F.lit(False)
        for dieu_kien, ten in RULES_LOAI:
            dk = dieu_kien.replace("dup_seq", "_dup_seq")
            # coalesce(..., False): NULL (vd so_khach thiếu) không được tính là "loại",
            # tránh bẫy logic 3 giá trị của SQL (NULL OR False = NULL, không phải False,
            # khiến .filter(~loai_bo) âm thầm loại luôn cả dòng không khớp luật nào)
            dieu_kien_an_toan = F.coalesce(F.expr(dk), F.lit(False))
            n = df.filter(dieu_kien_an_toan).count()
            rule_counts[ten] = n
            print(f"[INFO]   Loai theo '{ten}': {n:,} ({n/total*100:.3f}%)")
            loai_bo = loai_bo | dieu_kien_an_toan

        # nam/thang là generated column tính từ thoi_diem_don, nên khi ghi với replaceWhere
        # "nam=X AND thang=Y", MỌI dòng phải thật sự tính ra đúng X/Y từ thoi_diem_don --
        # nếu không Delta từ chối ghi (CHECK constraint). Một số ít dòng TLC có
        # thoi_diem_don sai lệch (khác tháng khai báo trong tên file, có khi khác cả năm),
        # nên bắt buộc phải loại trước khi ghi, không chỉ để "làm sạch" mà để bảo đảm ghi được.
        lech_thang = F.coalesce(
            (F.year("thoi_diem_don") != year) | (F.month("thoi_diem_don") != month),
            F.lit(True),  # thoi_diem_don NULL -> không xác định được nam/thang -> loại
        )
        n_lech = df.filter(lech_thang).count()
        rule_counts["ngay_don_lech_thang_khai_bao"] = n_lech
        print(f"[INFO]   Loai theo 'ngay_don_lech_thang_khai_bao': {n_lech:,} ({n_lech/total*100:.3f}%)")
        loai_bo = loai_bo | lech_thang

        con_lai = df.filter(~loai_bo)
        vung_loi = F.coalesce(
            (F.col("vung_don") < VUNG_MIN) | (F.col("vung_don") > VUNG_MAX)
            | (F.col("vung_tra") < VUNG_MIN) | (F.col("vung_tra") > VUNG_MAX),
            F.lit(False),
        )
        df_quarantine = con_lai.filter(vung_loi).drop("_dup_seq")
        df_sach = con_lai.filter(~vung_loi).drop("_dup_seq")

        so_quarantine = df_quarantine.count()
        so_sach = df_sach.count()
        so_loai = total - so_sach - so_quarantine
        print(f"[INFO] Sach: {so_sach:,} | Quarantine (vung ngoai 1-263): {so_quarantine:,} "
              f"| Loai han: {so_loai:,} ({so_loai/total*100:.3f}%)")

        ensure_table(spark, SILVER_PATH, add_ngay=True)
        ensure_table(spark, QUARANTINE_PATH, add_ngay=False)

        print(f"[INFO] Ghi silver.chuyen_di cho {year}-{month:02d} (ghi de partition)")
        (df_sach.write.format("delta").mode("overwrite")
         .option("replaceWhere", f"nam = {year} AND thang = {month}")
         .save(SILVER_PATH))

        print(f"[INFO] Ghi _quarantine.chuyen_di cho {year}-{month:02d} (ghi de partition)")
        (df_quarantine.write.format("delta").mode("overwrite")
         .option("replaceWhere", f"nam = {year} AND thang = {month}")
         .save(QUARANTINE_PATH))

        print(f"[INFO] Ghi thong ke loai bo cho {year}-{month:02d}")
        stats_row = spark.createDataFrame(
            [(year, month, total, so_sach, so_quarantine, so_loai, *rule_counts.values())],
            ["nam", "thang", "tong_dong", "so_dong_sach", "so_dong_quarantine",
             "so_dong_loai", *rule_counts.keys()],
        )
        if not path_exists(spark, STATS_PATH + "/_delta_log"):
            stats_row.write.format("delta").partitionBy("nam", "thang").save(STATS_PATH)
        else:
            (stats_row.write.format("delta").mode("overwrite")
             .option("replaceWhere", f"nam = {year} AND thang = {month}")
             .save(STATS_PATH))

        silver_count = (
            spark.read.format("delta").load(SILVER_PATH)
            .filter((F.col("nam") == year) & (F.col("thang") == month)).count()
        )
        print(f"[INFO] So dong trong silver cho {year}-{month:02d}: {silver_count:,}")
        if silver_count != so_sach:
            print(f"[LOI] Lech so dong: ky vong {so_sach:,} vs silver {silver_count:,}")
            exit_code = 1
        else:
            print(f"[OK] {year}-{month:02d} da chuan hoa xong, so dong khop.")
    finally:
        spark.stop()

    return exit_code


if __name__ == "__main__":
    sys.exit(main())