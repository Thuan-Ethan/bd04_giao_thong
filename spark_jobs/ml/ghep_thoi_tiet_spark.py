"""
Functions/ghep_thoi_tiet_spark.py - ham Spark ghep du lieu thoi tiet + ngay le (theo khung gio)
vao bat ky DataFrame co 1 cot thoi gian (timestamp), doc tu CSV da tai san tren HDFS
(/data/raw/open_meteo/thoi_tiet_open_meteo_nyc_2022_2025.csv - xem README.txt cung thu muc,
pham vi 2022-01-01 -> 2025-12-31 gio New York).

Dung o buoc trich_xuat_dac_trung (xem Context/03-can-lam.md) hoac bat ky job Spark can them
nhiet_do/luong_mua/la_ngay_le/ten_ngay_le theo gio dia phuong New York.

Quy uoc giu dung README.txt: khung_gio 01:00 ngay lui gio thang 11 xuat hien 2 lan trong CSV
(lech_utc_gio -4 va -5, cung 1 gio dia phuong) -> PHAI gop truoc khi join (AVG nhiet_do,
SUM luong_mua), neu khong join se nhan doi dong cho gio do (1 dong du lieu khop 2 dong thoi tiet).
Gio 02:00 ngay tien gio thang 3 khong ton tai trong CSV -> join trai (left) tra ve None, dung
voi thuc te (khong co du lieu thoi tiet cho gio khong ton tai).

    spark-submit ghep_thoi_tiet_spark.py --duong-dan-bronze <delta_path> [--duong-dan-ra <path>]

    # hoac import truc tiep trong job khac:
    from ghep_thoi_tiet_spark import ghep_thoi_tiet
    df_moi = ghep_thoi_tiet(spark, df, cot_thoi_gian="tpep_pickup_datetime")
"""
import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

THOI_TIET_CSV = "hdfs://hadoop-master:9000/data/raw/open_meteo/thoi_tiet_open_meteo_nyc_2022_2025.csv"
TOC_DO_GIO_CSV = "hdfs://hadoop-master:9000/data/raw/open_meteo/toc_do_gio_nyc_2022_2025.csv"
BRONZE_MAC_DINH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/bronze/chuyen_di"


def _doc_thoi_tiet_theo_gio(spark, duong_dan=THOI_TIET_CSV, duong_dan_gio=TOC_DO_GIO_CSV):
    """Doc CSV thoi tiet (nhiet do/mua/ngay le) + CSV toc do gio (file rieng, tai sau), gop
    truoc cac dong khung_gio trung (gio mo ho lui gio thang 11) o moi file truoc khi join."""
    thoi_tiet = (
        spark.read.option("header", True).option("inferSchema", True).csv(duong_dan)
        .withColumn("khung_gio", F.to_timestamp("khung_gio"))
        .groupBy("khung_gio").agg(
            F.avg("nhiet_do").alias("nhiet_do"),
            F.sum("luong_mua").alias("luong_mua"),
            F.max("la_ngay_le").alias("la_ngay_le"),
            F.first("ten_ngay_le", ignorenulls=True).alias("ten_ngay_le"),
        )
    )
    toc_do_gio = (
        spark.read.option("header", True).option("inferSchema", True).csv(duong_dan_gio)
        .withColumn("khung_gio", F.to_timestamp("khung_gio"))
        .groupBy("khung_gio").agg(F.avg("toc_do_gio").alias("toc_do_gio"))
    )
    return thoi_tiet.join(toc_do_gio, on="khung_gio", how="left")


def ghep_thoi_tiet(spark, df, cot_thoi_gian="tpep_pickup_datetime"):
    """Them cot nhiet_do, luong_mua, la_ngay_le, ten_ngay_le vao df, ghep theo gio tron
    (ep tu cot_thoi_gian, gio dia phuong New York) khop voi cot khung_gio cua bang thoi tiet.
    Left join: dong ngoai pham vi 2022-2025 hoac gio khong ton tai (tien gio thang 3) se nhan
    gia tri None cho 4 cot moi, khong mat dong goc."""
    thoi_tiet = _doc_thoi_tiet_theo_gio(spark)
    da_ghep = (
        df.withColumn("_khung_gio_join", F.date_trunc("hour", F.col(cot_thoi_gian)))
        .join(
            thoi_tiet.withColumnRenamed("khung_gio", "_khung_gio_join"),
            on="_khung_gio_join",
            how="left",
        )
        .drop("_khung_gio_join")
    )
    return da_ghep


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duong-dan-bronze", default=BRONZE_MAC_DINH)
    ap.add_argument("--cot-thoi-gian", default="tpep_pickup_datetime")
    ap.add_argument("--duong-dan-ra", default=None, help="neu co: ghi ket qua ra day (parquet)")
    a = ap.parse_args()

    spark = SparkSession.builder.appName("ghep_thoi_tiet_smoke_test").getOrCreate()
    try:
        df = spark.read.format("delta").load(a.duong_dan_bronze)
        da_ghep = ghep_thoi_tiet(spark, df, a.cot_thoi_gian).cache()

        tong_so_dong = da_ghep.count()
        so_dong_khop = da_ghep.where(F.col("nhiet_do").isNotNull()).count()
        mau = [r.asDict() for r in da_ghep.select(
            a.cot_thoi_gian, "nhiet_do", "luong_mua", "la_ngay_le", "ten_ngay_le"
        ).limit(5).collect()]

        if a.duong_dan_ra:
            da_ghep.write.mode("overwrite").parquet(a.duong_dan_ra)

        ket_qua = {
            "duong_dan_bronze": a.duong_dan_bronze,
            "tong_so_dong": tong_so_dong,
            "so_dong_khop_thoi_tiet": so_dong_khop,
            "so_dong_khong_khop": tong_so_dong - so_dong_khop,
            "mau": mau,
        }
        print("[KET_QUA] " + json.dumps(ket_qua, ensure_ascii=False, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
