"""
Functions/tao_nhu_cau_vung_gio.py - sinh gold.nhu_cau_theo_vung_gio cho 1 thang: luoi day du
(263 vung x moi gio trong thang), LEFT JOIN voi du lieu that tu silver.chuyen_di. Khong lam
vay se lam mo hinh hoc lech (dung canh bao trong de cuong muc 4.4).

Gio khong co chuyen -> so_chuyen=0, tong_doanh_thu=0.0 (0 THAT). Nhung thoi_gian_chuyen_tb/
quang_duong_tb de NULL (khong co chuyen thi "thoi gian chuyen trung binh" khong co nghia,
khac ban chat voi "0 phut" -- buoc sau (vd KNN impute so_chuyen cho khung gio nghi mat du
lieu) tu quyet dinh cach xu ly).

Khong co generated column/partition rieng o day (de cuong chi yeu cau partition o bronze
va silver). Idempotent bang replaceWhere tren khoang thoi gian cua thang.

    from tao_nhu_cau_vung_gio import tao_nhu_cau
    ket_qua, thong_ke = tao_nhu_cau(spark, nam=2022, thang=1)

    # hoac chay doc lap:
    spark-submit tao_nhu_cau_vung_gio.py --nam 2022 --thang 1
"""
import argparse
import calendar
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/silver/chuyen_di"
GOLD_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/gold/nhu_cau_theo_vung_gio"
LOOKUP_PATH = "hdfs://hadoop-master:9000/data/raw/tlc/lookup/taxi_zone_lookup.csv"

VUNG_MIN, VUNG_MAX = 1, 263  # khop luat quarantine cua lam_sach_silver.py


def _danh_sach_vung(spark):
    """263 vung hop le tu file lookup goc -- KHONG lay tu du lieu chuyen di, de giu ca cac
    vung mot thang khong co chuyen nao."""
    return (
        spark.read.option("header", True).option("quote", '"').csv(LOOKUP_PATH)
        .withColumn("LocationID", F.col("LocationID").cast("int"))
        .filter((F.col("LocationID") >= VUNG_MIN) & (F.col("LocationID") <= VUNG_MAX))
        .select(F.col("LocationID").alias("vung"))
    )


def _bang_lich_gio(spark, nam, thang):
    so_ngay = calendar.monthrange(nam, thang)[1]
    dau_thang = f"{nam:04d}-{thang:02d}-01 00:00:00"
    cuoi_thang = f"{nam:04d}-{thang:02d}-{so_ngay:02d} 23:00:00"
    return spark.sql(f"""
        SELECT explode(sequence(
            timestamp'{dau_thang}', timestamp'{cuoi_thang}', interval 1 hour
        )) AS khung_gio
    """)


def tao_nhu_cau(spark, nam, thang):
    """
    Tra ve (ket_qua, thong_ke):
        ket_qua  - DataFrame luoi day du (vung, khung_gio, so_chuyen, tong_doanh_thu,
                   thoi_gian_chuyen_tb, quang_duong_tb).
        thong_ke - dict so_vung/so_gio/so_o_day_du/so_o_co_du_lieu_that.
    """
    silver = (
        spark.read.format("delta").load(SILVER_PATH)
        .filter((F.col("nam") == nam) & (F.col("thang") == thang))
    )

    vung = _danh_sach_vung(spark)
    so_vung = vung.count()
    lich = _bang_lich_gio(spark, nam, thang)
    so_gio = lich.count()
    luoi = vung.crossJoin(lich)
    so_o_day_du = so_vung * so_gio

    thuc_te = (
        silver.groupBy(
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
    so_o_co_du_lieu_that = thuc_te.count()

    ket_qua = (
        luoi.join(thuc_te, on=["vung", "khung_gio"], how="left")
        .withColumn("so_chuyen", F.coalesce(F.col("so_chuyen"), F.lit(0)).cast("int"))
        .withColumn("tong_doanh_thu", F.coalesce(F.col("tong_doanh_thu"), F.lit(0.0)))
        .select("vung", "khung_gio", "so_chuyen", "tong_doanh_thu",
                "thoi_gian_chuyen_tb", "quang_duong_tb")
    )

    thong_ke = {
        "so_vung": so_vung,
        "so_gio": so_gio,
        "so_o_day_du": so_o_day_du,
        "so_o_co_du_lieu_that": so_o_co_du_lieu_that,
    }
    return ket_qua, thong_ke


def _duong_dan_ton_tai(spark, duong_dan):
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(duong_dan)
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(jvm_path)


def ghi_gold(spark, ket_qua, nam, thang):
    dau_thang = f"{nam:04d}-{thang:02d}-01 00:00:00"
    so_ngay = calendar.monthrange(nam, thang)[1]
    cuoi_thang_exclusive = (
        f"{nam:04d}-{thang + 1:02d}-01 00:00:00" if thang < 12
        else f"{nam + 1:04d}-01-01 00:00:00"
    )
    dieu_kien_ghi = f"khung_gio >= timestamp'{dau_thang}' AND khung_gio < timestamp'{cuoi_thang_exclusive}'"

    if not _duong_dan_ton_tai(spark, GOLD_PATH + "/_delta_log"):
        ket_qua.write.format("delta").save(GOLD_PATH)
    else:
        (ket_qua.write.format("delta").mode("overwrite")
         .option("replaceWhere", dieu_kien_ghi)
         .save(GOLD_PATH))
    return dieu_kien_ghi


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nam", type=int, required=True)
    ap.add_argument("--thang", type=int, required=True, choices=range(1, 13), metavar="[1-12]")
    a = ap.parse_args()

    spark = (
        SparkSession.builder.appName(f"tao_nhu_cau_vung_gio_{a.nam}_{a.thang:02d}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    try:
        if not _duong_dan_ton_tai(spark, SILVER_PATH + "/_delta_log"):
            print("[LOI] Chua co silver.chuyen_di, chay lam_sach_silver.py truoc.")
            return 1

        ket_qua, thong_ke = tao_nhu_cau(spark, a.nam, a.thang)
        so_dong = ket_qua.count()
        if so_dong != thong_ke["so_o_day_du"]:
            print(f"[LOI] Luoi sau join ({so_dong:,}) khong khop luoi day du ({thong_ke['so_o_day_du']:,})")
            return 1

        dieu_kien_ghi = ghi_gold(spark, ket_qua, a.nam, a.thang)
        gold_count = spark.read.format("delta").load(GOLD_PATH).filter(dieu_kien_ghi).count()
        thong_ke["so_dong_ghi"] = so_dong
        thong_ke["so_dong_trong_gold"] = gold_count
        if gold_count != so_dong:
            print(f"[LOI] Lech so dong: ky vong {so_dong:,} vs gold {gold_count:,}")
            return 1

        print("[KET_QUA] " + str(thong_ke))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
