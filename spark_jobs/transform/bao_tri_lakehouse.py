"""
Functions/bao_tri_lakehouse.py - DAG 5 Giai doan 2 theo de cuong muc 4.9 (gt_bao_tri, lich
"0 5 * * 0" - 5h sang Chu nhat): OPTIMIZE + ZORDER cac bang lon, VACUUM toan bo lakehouse,
ghi thong ke dung luong.

RETAIN 168 HOURS (= 7 ngay, dung MAC DINH cua Delta) -- KHONG tat
retentionDurationCheck.enabled, vi 168h da bang nguong an toan mac dinh, khong can ha thap.

    spark-submit ... bao_tri_lakehouse.py --buoc optimize
    spark-submit ... bao_tri_lakehouse.py --buoc vacuum
    spark-submit ... bao_tri_lakehouse.py --buoc thong_ke
(tach 3 buoc thanh 3 lenh spark-submit rieng de DAG goi thanh 3 task doc lap, de theo doi/retry
tung buoc rieng neu loi, giong quy uoc .expand()/task rieng da dung o cac DAG truoc.)
"""
import argparse
import json
import sys

from pyspark.sql import SparkSession

LAKEHOUSE = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong"
HDFS_LAKEHOUSE = "/lakehouse/bd04_giao_thong"

# (duong_dan, cot_zorder) -- chi 2 bang LON nhat (bronze/silver) can ZORDER; gold/features
# nho (<250MB), OPTIMIZE thuong (khong ZORDER) la du, xem BANG_CAN_VACUUM.
BANG_CAN_ZORDER = [
    (f"{LAKEHOUSE}/bronze/chuyen_di", ["PULocationID", "tpep_pickup_datetime"]),
    (f"{LAKEHOUSE}/silver/chuyen_di", ["vung_don", "thoi_diem_don"]),
]

BANG_CAN_VACUUM = [
    f"{LAKEHOUSE}/bronze/chuyen_di",
    f"{LAKEHOUSE}/silver/chuyen_di",
    f"{LAKEHOUSE}/gold/nhu_cau_theo_vung_gio",
    f"{LAKEHOUSE}/gold/ma_tran_od",
    f"{LAKEHOUSE}/gold/du_bao_nhu_cau",
    f"{LAKEHOUSE}/features/dac_trung_nhu_cau",
]


def optimize_zorder(spark):
    ket_qua = {}
    for duong_dan, cot in BANG_CAN_ZORDER:
        ten = duong_dan.split("/")[-1]
        df = spark.sql(f"OPTIMIZE delta.`{duong_dan}` ZORDER BY ({', '.join(cot)})")
        ket_qua[ten] = df.collect()[0].asDict()
    return ket_qua


def vacuum(spark, retain_hours=168):
    ket_qua = {}
    for duong_dan in BANG_CAN_VACUUM:
        ten = duong_dan.split("/")[-1]
        so_file = spark.sql(f"VACUUM delta.`{duong_dan}` RETAIN {retain_hours} HOURS").count()
        ket_qua[ten] = {"so_file_xoa": so_file}
    return ket_qua


def _dung_luong(spark, duong_dan):
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(duong_dan)
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.getContentSummary(jvm_path).getLength()


def ghi_thong_ke(spark):
    import datetime

    tang = {}
    for ten_tang in ["bronze", "silver", "gold", "features", "models", "quarantine", "reports"]:
        duong_dan = f"{LAKEHOUSE}/{ten_tang}"
        try:
            tang[ten_tang] = _dung_luong(spark, duong_dan)
        except Exception:
            tang[ten_tang] = None

    ket_qua = {
        "thoi_diem": datetime.datetime.now().isoformat(),
        "dung_luong_byte_theo_tang": tang,
        "tong_byte": sum(v for v in tang.values() if v),
    }

    rdd = spark.sparkContext.parallelize([json.dumps(ket_qua, default=str)])
    df_bao_cao = spark.read.json(rdd)
    (
        df_bao_cao.write.format("delta").mode("append")
        .save(f"{LAKEHOUSE}/reports/bao_tri_thong_ke")
    )
    return ket_qua


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--buoc", choices=["optimize", "vacuum", "thong_ke"], required=True)
    a = ap.parse_args()

    spark = (
        SparkSession.builder.appName(f"bao_tri_lakehouse_{a.buoc}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    try:
        if a.buoc == "optimize":
            ket_qua = optimize_zorder(spark)
        elif a.buoc == "vacuum":
            ket_qua = vacuum(spark)
        else:
            ket_qua = ghi_thong_ke(spark)
        print("[KET_QUA]", json.dumps(ket_qua, default=str))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
