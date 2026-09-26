"""
spark_jobs/ml/tao_dac_trung.py

Sinh features.dac_trung_nhu_cau tu gold.nhu_cau_theo_vung_gio + du lieu ngoai
(ma tran ke tu shapefile, thoi tiet Open-Meteo, lich le My, loai vung).

Giong tao_ma_tran_od.py, KHONG chay theo --nam/--thang: lag_168h va cua so truot
168h can nhin lai 7 ngay truoc, neu chay rieng tung thang thi 7 ngay dau moi thang
se thieu du lieu thang truoc de tinh dung -- phai tinh tren toan bo lich su lien
mach 1 lan, ghi de toan bo bang moi lan chay (dung nhu tao_ma_tran_od.py).

Cac cua so truot (rolling) dung rowsBetween(-N, -1) -- KHONG bao gom gio hien tai,
de tranh data leakage (dung chinh so_chuyen dang du bao de tinh dac trung dau vao).

Cach chay (tren hadoop-master, KHONG can --nam/--thang):
    spark-submit --master yarn spark_jobs/ml/tao_dac_trung.py
"""
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

NHUCAU_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/gold/nhu_cau_theo_vung_gio"
FEATURES_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/features/dac_trung_nhu_cau"
MATRANKE_PATH = "hdfs://hadoop-master:9000/data/raw/tlc/lookup/ma_tran_ke.csv"
THOITIET_PATH = "hdfs://hadoop-master:9000/data/raw/tlc/lookup/thoi_tiet.csv"
LICHLE_PATH = "hdfs://hadoop-master:9000/data/raw/tlc/lookup/lich_le_my.csv"
LOOKUP_PATH = "hdfs://hadoop-master:9000/data/raw/tlc/lookup/taxi_zone_lookup.csv"

# Do tu du lieu that (top 25% gio co so_chuyen trung binh cao nhat, toan bo 2022-2025):
# 18,17,19,15,16,14 -- co dinh cung, khong tinh dong moi lan chay de dac trung on dinh
GIO_CAO_DIEM = {14, 15, 16, 17, 18, 19}

# service_zone (tu taxi_zone_lookup.csv) -> loai_vung
LOAI_VUNG_MAP = {
    "Airports": "san_bay",
    "EWR": "san_bay",
    "Yellow Zone": "trung_tam",
    "Boro Zone": "dan_cu",
    "N/A": "khong_xac_dinh",
}


def build_spark():
    return (
        SparkSession.builder
        .appName("tao_dac_trung")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def path_exists(spark, path):
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(jvm_path)


def them_dac_trung_tre_va_truot(nhu_cau):
    """lag_1h/2h/24h/168h + trung binh/do lech chuan truot 3h/24h/168h.
    Cua so truot dung rowsBetween(-N,-1): KHONG tinh gio hien tai, tranh leakage."""
    w_thu_tu = Window.partitionBy("vung").orderBy("khung_gio")
    w_3h = w_thu_tu.rowsBetween(-3, -1)
    w_24h = w_thu_tu.rowsBetween(-24, -1)
    w_168h = w_thu_tu.rowsBetween(-168, -1)

    return (
        nhu_cau
        .withColumn("lag_1h", F.lag("so_chuyen", 1).over(w_thu_tu))
        .withColumn("lag_2h", F.lag("so_chuyen", 2).over(w_thu_tu))
        .withColumn("lag_24h", F.lag("so_chuyen", 24).over(w_thu_tu))
        .withColumn("lag_168h", F.lag("so_chuyen", 168).over(w_thu_tu))
        .withColumn("trungbinh_truot_3h", F.avg("so_chuyen").over(w_3h))
        .withColumn("dolech_truot_3h", F.stddev("so_chuyen").over(w_3h))
        .withColumn("trungbinh_truot_24h", F.avg("so_chuyen").over(w_24h))
        .withColumn("dolech_truot_24h", F.stddev("so_chuyen").over(w_24h))
        .withColumn("trungbinh_truot_168h", F.avg("so_chuyen").over(w_168h))
        .withColumn("dolech_truot_168h", F.stddev("so_chuyen").over(w_168h))
    )


def them_dac_trung_khong_gian(spark, df):
    """tong_nhu_cau_vung_ke_gio_truoc: tong so_chuyen cua cac vung ke tai gio truoc."""
    ma_tran_ke = spark.read.option("header", True).csv(MATRANKE_PATH) \
        .withColumn("vung", F.col("vung").cast("int")) \
        .withColumn("vung_ke", F.col("vung_ke").cast("int"))

    # Dich khung_gio cua vung nguon toi +1h: so_chuyen tai gio T cua vung_ke se
    # duoc cong vao dac trung cua vung dich tai khung_gio = T + 1h ("gio truoc")
    nguon_dich_1h = df.select(
        F.col("vung").alias("vung_nguon"),
        (F.col("khung_gio") + F.expr("INTERVAL 1 HOUR")).alias("khung_gio_dich"),
        F.col("so_chuyen").alias("so_chuyen_nguon"),
    )

    tong_hang_xom = (
        ma_tran_ke.join(nguon_dich_1h, ma_tran_ke.vung_ke == nguon_dich_1h.vung_nguon)
        .groupBy(ma_tran_ke.vung.alias("vung"), "khung_gio_dich")
        .agg(F.sum("so_chuyen_nguon").alias("tong_nhu_cau_vung_ke_gio_truoc"))
        .withColumnRenamed("khung_gio_dich", "khung_gio")
    )

    return (
        df.join(tong_hang_xom, on=["vung", "khung_gio"], how="left")
        # khong co du lieu hang xom (vung co lap, hoac gio dau tien) -> that su la 0,
        # khac voi cac cot trung binh o tao_nhu_cau.py (0 chuyen la gia tri that, khong phai thieu du lieu)
        .withColumn("tong_nhu_cau_vung_ke_gio_truoc",
                    F.coalesce(F.col("tong_nhu_cau_vung_ke_gio_truoc"), F.lit(0.0)))
    )


def them_dac_trung_thoi_gian(spark, df):
    lich_le = spark.read.option("header", True).csv(LICHLE_PATH) \
        .withColumn("ngay", F.col("ngay").cast("date")) \
        .select("ngay").distinct()

    df = (
        df
        .withColumn("gio", F.hour("khung_gio"))
        .withColumn("thu_trong_tuan", F.dayofweek("khung_gio"))  # 1=CN ... 7=T7
        .withColumn("thang", F.month("khung_gio"))
        .withColumn("la_cuoi_tuan", F.col("thu_trong_tuan").isin(1, 7))
        .withColumn("la_gio_cao_diem", F.col("gio").isin(*GIO_CAO_DIEM))
        .withColumn("sin_gio", F.sin(F.col("gio") * 2 * F.lit(3.14159265358979) / 24))
        .withColumn("cos_gio", F.cos(F.col("gio") * 2 * F.lit(3.14159265358979) / 24))
        .withColumn("sin_thu", F.sin(F.col("thu_trong_tuan") * 2 * F.lit(3.14159265358979) / 7))
        .withColumn("cos_thu", F.cos(F.col("thu_trong_tuan") * 2 * F.lit(3.14159265358979) / 7))
        .withColumn("ngay_cua_khung_gio", F.to_date("khung_gio"))
    )

    df = (
        df.join(lich_le, df.ngay_cua_khung_gio == lich_le.ngay, how="left")
        .withColumn("la_ngay_le", F.col("ngay").isNotNull())
        .drop("ngay", "ngay_cua_khung_gio")
    )
    return df


def them_dac_trung_thoi_tiet(spark, df):
    thoi_tiet = (
        spark.read.option("header", True).csv(THOITIET_PATH)
        .withColumn("khung_gio", F.to_timestamp("khung_gio", "yyyy-MM-dd'T'HH:mm"))
        .withColumn("nhiet_do", F.col("nhiet_do").cast("double"))
        .withColumn("luong_mua", F.col("luong_mua").cast("double"))
        .withColumn("toc_do_gio", F.col("toc_do_gio").cast("double"))
    )
    return df.join(thoi_tiet, on="khung_gio", how="left")


def them_dac_trung_vung(spark, df):
    lookup = spark.read.option("header", True).option("quote", '"').csv(LOOKUP_PATH) \
        .withColumn("LocationID", F.col("LocationID").cast("int"))

    mapping = F.create_map(*[x for k, v in LOAI_VUNG_MAP.items() for x in (F.lit(k), F.lit(v))])
    lookup = lookup.withColumn(
        "loai_vung", F.coalesce(mapping[F.col("service_zone")], F.lit("khong_xac_dinh"))
    ).select(F.col("LocationID").alias("vung"), "loai_vung")

    return df.join(lookup, on="vung", how="left")


def main():
    spark = build_spark()
    exit_code = 0
    df = None

    try:
        if not path_exists(spark, NHUCAU_PATH + "/_delta_log"):
            print("[LOI] Chua co gold.nhu_cau_theo_vung_gio, chay tao_nhu_cau.py truoc.")
            return 1

        print("[INFO] Doc toan bo gold.nhu_cau_theo_vung_gio")
        nhu_cau = spark.read.format("delta").load(NHUCAU_PATH)
        so_dong_nguon = nhu_cau.count()
        print(f"[INFO] So dong nguon: {so_dong_nguon:,}")

        print("[INFO] Tinh dac trung tre/truot (window functions)")
        df = them_dac_trung_tre_va_truot(nhu_cau)

        print("[INFO] Tinh dac trung khong gian (join ma tran ke)")
        df = them_dac_trung_khong_gian(spark, df)

        print("[INFO] Tinh dac trung thoi gian (+ join lich le)")
        df = them_dac_trung_thoi_gian(spark, df)

        print("[INFO] Join thoi tiet")
        df = them_dac_trung_thoi_tiet(spark, df)

        print("[INFO] Join loai vung")
        df = them_dac_trung_vung(spark, df).cache()

        so_dong_ket_qua = df.count()
        print(f"[INFO] So dong sau khi them dac trung: {so_dong_ket_qua:,}")
        if so_dong_ket_qua != so_dong_nguon:
            print(f"[LOI] Lech so dong: nguon {so_dong_nguon:,} vs ket qua {so_dong_ket_qua:,}")
            return 1

        print(f"[INFO] Ghi de toan bo features.dac_trung_nhu_cau tai {FEATURES_PATH}")
        df.write.format("delta").mode("overwrite").save(FEATURES_PATH)

        gold_count = spark.read.format("delta").load(FEATURES_PATH).count()
        print(f"[INFO] So dong sau khi ghi: {gold_count:,}")
        if gold_count != so_dong_ket_qua:
            print(f"[LOI] Lech so dong sau ghi: ky vong {so_dong_ket_qua:,} vs {gold_count:,}")
            exit_code = 1
        else:
            print(f"[OK] Da sinh dac_trung_nhu_cau xong, so dong khop ({gold_count:,}).")
    finally:
        if df is not None:
            df.unpersist()
        spark.stop()

    return exit_code


if __name__ == "__main__":
    sys.exit(main())