"""
Functions/tao_ma_tran_od.py - sinh gold.ma_tran_od: bang tham chieu lich su on dinh cho tung
cap (vung_don, vung_tra, khung_gio_trong_ngay, thu_trong_tuan) -- dung lam baseline cho
mo hinh ETA (median lich su) va de web app ve heatmap OD (de cuong muc 4.4, 4.8).

Khac voi tao_nhu_cau_vung_gio.py: bang nay KHONG co cot nam/thang, khong chay theo thang --
moi lan chay doc TOAN BO silver.chuyen_di va ghi de toan bo bang. Idempotent tu nhien vi la
tinh lai tron ven moi lan (khong phai MERGE/replaceWhere theo partition).

    from tao_ma_tran_od import tao_ma_tran_od
    ma_tran, thong_ke = tao_ma_tran_od(spark)

    # hoac chay doc lap (KHONG can --nam/--thang):
    spark-submit tao_ma_tran_od.py
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/silver/chuyen_di"
GOLD_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/gold/ma_tran_od"


def tao_ma_tran_od(spark):
    """Tra ve (ma_tran, thong_ke). ma_tran: vung_don, vung_tra, khung_gio_trong_ngay (0-23),
    thu_trong_tuan (1=CN..7=T7, chuan Spark dayofweek), so_chuyen, thoi_gian_trung_vi,
    thoi_gian_p90."""
    silver = spark.read.format("delta").load(SILVER_PATH)
    so_dong_nguon = silver.count()

    ma_tran = (
        silver
        .withColumn("khung_gio_trong_ngay", F.hour("thoi_diem_don"))
        .withColumn("thu_trong_tuan", F.dayofweek("thoi_diem_don"))
        .groupBy("vung_don", "vung_tra", "khung_gio_trong_ngay", "thu_trong_tuan")
        .agg(
            F.count("*").cast("long").alias("so_chuyen"),
            F.percentile_approx("thoi_gian_phut", 0.5).alias("thoi_gian_trung_vi"),
            F.percentile_approx("thoi_gian_phut", 0.9).alias("thoi_gian_p90"),
        )
    )
    thong_ke = {"so_dong_nguon": so_dong_nguon}
    return ma_tran, thong_ke


def main():
    spark = (
        SparkSession.builder.appName("tao_ma_tran_od")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    try:
        jvm_path = spark._jvm.org.apache.hadoop.fs.Path(SILVER_PATH + "/_delta_log")
        fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
        if not fs.exists(jvm_path):
            print("[LOI] Chua co silver.chuyen_di, chay lam_sach_silver.py truoc.")
            return 1

        ma_tran, thong_ke = tao_ma_tran_od(spark)
        so_dong_od = ma_tran.count()
        thong_ke["so_to_hop_od"] = so_dong_od

        ma_tran.write.format("delta").mode("overwrite").save(GOLD_PATH)

        gold_count = spark.read.format("delta").load(GOLD_PATH).count()
        thong_ke["so_dong_trong_gold"] = gold_count
        if gold_count != so_dong_od:
            print(f"[LOI] Lech so dong: ky vong {so_dong_od:,} vs gold {gold_count:,}")
            return 1

        print("[KET_QUA] " + str(thong_ke))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
