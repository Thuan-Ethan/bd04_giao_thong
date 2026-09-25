"""
spark_jobs/transform/tao_ma_tran_od.py

Sinh gold.ma_tran_od: bang tham chieu lich su on dinh cho tung cap (vung_don, vung_tra,
khung_gio_trong_ngay, thu_trong_tuan) -- dung lam baseline cho mo hinh ETA (median lich su).

Khac voi tao_nhu_cau.py: bang nay KHONG co cot nam/thang, khong chay theo thang -- moi
lan chay doc TOAN BO silver.chuyen_di (ca 48 thang) va ghi de toan bo bang. Idempotent
tu nhien vi la tinh lai tron ven moi lan (khong phai MERGE/replaceWhere theo partition).

Cach chay (tren hadoop-master, KHONG can --nam/--thang):
    spark-submit --master yarn spark_jobs/transform/tao_ma_tran_od.py
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/silver/chuyen_di"
GOLD_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/gold/ma_tran_od"


def build_spark():
    return (
        SparkSession.builder
        .appName("tao_ma_tran_od")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def path_exists(spark, path):
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(jvm_path)


def main():
    spark = build_spark()
    exit_code = 0

    try:
        if not path_exists(spark, SILVER_PATH + "/_delta_log"):
            print("[LOI] Chua co silver.chuyen_di, chay bronze_to_silver.py truoc.")
            return 1

        print("[INFO] Doc toan bo silver.chuyen_di (48 thang)")
        silver = spark.read.format("delta").load(SILVER_PATH)
        so_dong_nguon = silver.count()
        print(f"[INFO] So dong nguon: {so_dong_nguon:,}")

        # dayofweek(): 1=Chu nhat ... 7=Thu bay (chuan SQL/Spark)
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

        so_dong_od = ma_tran.count()
        print(f"[INFO] So to hop OD x gio x thu: {so_dong_od:,} "
              f"(ly thuyet toi da 263*263*24*7 = {263*263*24*7:,})")

        print(f"[INFO] Ghi de toan bo gold.ma_tran_od tai {GOLD_PATH}")
        ma_tran.write.format("delta").mode("overwrite").save(GOLD_PATH)

        gold_count = spark.read.format("delta").load(GOLD_PATH).count()
        print(f"[INFO] So dong trong gold sau khi ghi: {gold_count:,}")
        if gold_count != so_dong_od:
            print(f"[LOI] Lech so dong: ky vong {so_dong_od:,} vs gold {gold_count:,}")
            exit_code = 1
        else:
            print(f"[OK] Da sinh ma_tran_od xong, so dong khop ({so_dong_od:,} to hop OD).")
    finally:
        spark.stop()

    return exit_code


if __name__ == "__main__":
    sys.exit(main())