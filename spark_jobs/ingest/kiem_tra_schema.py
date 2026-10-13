"""
Functions/kiem_tra_schema.py - So sanh schema file Parquet thang moi voi "schema chuan" cua
bronze.chuyen_di (DAG 1 gt_nap_thang_moi, de cuong muc 4.9).

Quy uoc da chot tu phien lam viec truoc (docs/00-nhat-ky-quyet-dinh.md TH-11): schema chuan =
schema HIEN TAI cua bronze.chuyen_di, bo di cac cot he thong tu them trong nap_thang.py
(ma_chuyen, nam, thang, ten_file_nguon, thoi_diem_nap). So sanh ten cot KHONG phan biet hoa
thuong (de cuong khong dinh nghia truoc schema chuan, day la quyet dinh tu chot).

Chi doc SCHEMA (footer file), KHONG doc/quet du lieu -- rat nhanh, khong can executor, nen
chay duoc o --deploy-mode client (khong theo quy uoc cluster-mode cua cac job nang khac trong
du an, vi job nay qua nhe, khong co nguy co chiem RAM master nhu cac job xu ly du lieu that).

    spark-submit kiem_tra_schema.py --nam 2024 --thang 1
In ra dong [KET_QUA] {json} de DAG doc lai tu stdout (chi dung duoc voi --deploy-mode client).
"""
import argparse
import json
import sys

from pyspark.sql import SparkSession

RAW_TLC = "hdfs://hadoop-master:9000/data/raw/tlc/yellow"
BRONZE_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/bronze/chuyen_di"
COT_HE_THONG = {"ma_chuyen", "nam", "thang", "ten_file_nguon", "thoi_diem_nap"}


def lay_schema_chuan(spark):
    cot = spark.read.format("delta").load(BRONZE_PATH).schema.fieldNames()
    return {c.lower() for c in cot if c.lower() not in COT_HE_THONG}


def lay_schema_thang_moi(spark, nam, thang):
    duong_dan = f"{RAW_TLC}/yellow_tripdata_{nam}-{thang:02d}.parquet"
    cot = spark.read.parquet(duong_dan).schema.fieldNames()
    return {c.lower() for c in cot}


def so_sanh_schema(chuan: set, moi: set) -> dict:
    return {
        "khop": chuan == moi,
        "cot_thieu_trong_thang_moi": sorted(chuan - moi),
        "cot_them_trong_thang_moi": sorted(moi - chuan),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nam", type=int, required=True)
    ap.add_argument("--thang", type=int, required=True)
    a = ap.parse_args()

    spark = (
        SparkSession.builder.appName(f"kiem_tra_schema_{a.nam}_{a.thang:02d}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    try:
        chuan = lay_schema_chuan(spark)
        moi = lay_schema_thang_moi(spark, a.nam, a.thang)
        ket_qua = so_sanh_schema(chuan, moi)
        print("[KET_QUA]", json.dumps(ket_qua))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
