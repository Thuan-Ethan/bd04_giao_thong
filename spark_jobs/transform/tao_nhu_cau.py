"""
spark_jobs/transform/tao_nhu_cau.py

Sinh gold.nhu_cau_theo_vung_gio cho 1 thang: luoi day du (263 vung) x (moi gio trong
thang), cross join voi bang lich, LEFT JOIN voi du lieu that tu silver.chuyen_di.
Khong lam vay se lam mo hinh du bao hoc lech (theo dung canh bao trong de cuong).

Khong co generated column / partition nam,thang o day (de cuong chi yeu cau partition
o bronze va silver). Idempotent bang replaceWhere tren khoang thoi gian cua thang.

Cach chay (tren hadoop-master):
    spark-submit --master yarn spark_jobs/transform/tao_nhu_cau.py --nam 2024 --thang 1
"""
import argparse
import calendar
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/silver/chuyen_di"
GOLD_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/gold/nhu_cau_theo_vung_gio"
LOOKUP_PATH = "hdfs://hadoop-master:9000/data/raw/tlc/lookup/taxi_zone_lookup.csv"

VUNG_MIN, VUNG_MAX = 1, 263  # khop voi luat quarantine cua bronze_to_silver.py


def parse_args():
    parser = argparse.ArgumentParser(description="Sinh gold.nhu_cau_theo_vung_gio cho 1 thang")
    parser.add_argument("--nam", type=int, required=True)
    parser.add_argument("--thang", type=int, required=True, choices=range(1, 13), metavar="[1-12]")
    return parser.parse_args()


def build_spark(year, month):
    return (
        SparkSession.builder
        .appName(f"tao_nhu_cau_{year}_{month:02d}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def path_exists(spark, path):
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(jvm_path)


def danh_sach_vung(spark):
    """263 vung hop le tu file lookup goc -- KHONG lay tu du lieu chuyen di, de giu ca
    cac vung ca thang khong co chuyen nao (da phat hien 9 vung nhu vay o 2024-01)."""
    lookup = (
        spark.read.option("header", True).option("quote", '"').csv(LOOKUP_PATH)
        .withColumn("LocationID", F.col("LocationID").cast("int"))
        .filter((F.col("LocationID") >= VUNG_MIN) & (F.col("LocationID") <= VUNG_MAX))
        .select(F.col("LocationID").alias("vung"))
    )
    return lookup


def bang_lich_gio(spark, year, month):
    """Moi gio trong thang, dang TIMESTAMP lam tron gio (vd 2024-01-01 00:00:00 ... 23:00:00)."""
    so_ngay = calendar.monthrange(year, month)[1]
    dau_thang = f"{year:04d}-{month:02d}-01 00:00:00"
    cuoi_thang = f"{year:04d}-{month:02d}-{so_ngay:02d} 23:00:00"
    return spark.sql(f"""
        SELECT explode(sequence(
            timestamp'{dau_thang}', timestamp'{cuoi_thang}', interval 1 hour
        )) AS khung_gio
    """)


def main():
    args = parse_args()
    year, month = args.nam, args.thang
    spark = build_spark(year, month)
    exit_code = 0

    try:
        if not path_exists(spark, SILVER_PATH + "/_delta_log"):
            print("[LOI] Chua co silver.chuyen_di, chay bronze_to_silver.py truoc.")
            return 1

        vung = danh_sach_vung(spark)
        so_vung = vung.count()
        lich = bang_lich_gio(spark, year, month)
        so_gio = lich.count()
        luoi = vung.crossJoin(lich)
        so_o_day_du = so_vung * so_gio
        print(f"[INFO] Luoi day du: {so_vung} vung x {so_gio} gio = {so_o_day_du:,} o")

        print(f"[INFO] Doc silver {year}-{month:02d}")
        thuc_te = (
            spark.read.format("delta").load(SILVER_PATH)
            .filter((F.col("nam") == year) & (F.col("thang") == month))
            .groupBy(
                F.col("vung_don").alias("vung"),
                F.date_trunc("hour", F.col("thoi_diem_don")).alias("khung_gio"),
            )
            .agg(
                F.count("*").alias("so_chuyen"),
                F.sum("tong_tien").alias("tong_doanh_thu"),
                F.avg("thoi_gian_phut").alias("thoi_gian_chuyen_tb"),
                F.avg("quang_duong_mile").alias("quang_duong_tb"),
            )
        )
        so_o_thuc = thuc_te.count()
        print(f"[INFO] So o co du lieu that: {so_o_thuc:,} ({so_o_thuc/so_o_day_du*100:.1f}% luoi)")

        # LEFT JOIN: o khong co chuyen -> so_chuyen/tong_doanh_thu = 0, hai cot trung binh
        # de NULL (khong co chuyen thi "thoi gian chuyen trung binh" khong co nghia, khac
        # ve ban chat voi "0 phut" -- tao_dac_trung.py se tu quyet dinh cach impute)
        ket_qua = (
            luoi.join(thuc_te, on=["vung", "khung_gio"], how="left")
            .withColumn("so_chuyen", F.coalesce(F.col("so_chuyen"), F.lit(0)).cast("int"))
            .withColumn("tong_doanh_thu", F.coalesce(F.col("tong_doanh_thu"), F.lit(0.0)))
            .select("vung", "khung_gio", "so_chuyen", "tong_doanh_thu",
                    "thoi_gian_chuyen_tb", "quang_duong_tb")
        )

        so_dong = ket_qua.count()
        if so_dong != so_o_day_du:
            print(f"[LOI] Luoi sau join ({so_dong:,}) khong khop luoi day du ({so_o_day_du:,})")
            return 1

        dau_thang = f"{year:04d}-{month:02d}-01 00:00:00"
        so_ngay = calendar.monthrange(year, month)[1]
        cuoi_thang_exclusive = (
            f"{year:04d}-{month+1:02d}-01 00:00:00" if month < 12
            else f"{year+1:04d}-01-01 00:00:00"
        )
        dieu_kien_ghi = (
            f"khung_gio >= timestamp'{dau_thang}' AND khung_gio < timestamp'{cuoi_thang_exclusive}'"
        )

        if not path_exists(spark, GOLD_PATH + "/_delta_log"):
            print(f"[INFO] gold.nhu_cau_theo_vung_gio chua ton tai, tao moi tai {GOLD_PATH}")
            ket_qua.write.format("delta").save(GOLD_PATH)
        else:
            print(f"[INFO] Ghi de (replaceWhere) cho {year}-{month:02d}")
            (ket_qua.write.format("delta").mode("overwrite")
             .option("replaceWhere", dieu_kien_ghi)
             .save(GOLD_PATH))

        gold_count = (
            spark.read.format("delta").load(GOLD_PATH)
            .filter(dieu_kien_ghi).count()
        )
        print(f"[INFO] So dong trong gold cho {year}-{month:02d}: {gold_count:,}")
        if gold_count != so_dong:
            print(f"[LOI] Lech so dong: ky vong {so_dong:,} vs gold {gold_count:,}")
            exit_code = 1
        else:
            print(f"[OK] {year}-{month:02d} da sinh nhu cau xong, so dong khop ({so_o_thuc:,} thuc / "
                  f"{so_dong - so_o_thuc:,} dien 0).")
    finally:
        spark.stop()

    return exit_code


if __name__ == "__main__":
    sys.exit(main())