"""
Functions/tao_dac_trung.py - sinh features.dac_trung_nhu_cau tu gold.nhu_cau_theo_vung_gio
(sau khi da va KNN - xem va_knn_nhu_cau.py), du 7 nhom dac trung theo de cuong muc 4.4:

    Tre (lag)   : lag_1h, lag_2h, lag_24h, lag_168h (cung gio tuan truoc)
    Truot       : trung binh/do lech chuan cua so 3h, 24h, 168h
    Khong gian  : tong nhu cau cac vung ke (ma_tran_ke.csv) O GIO TRUOC
    Thoi gian   : gio, thu, thang, la_cuoi_tuan, la_ngay_le, la_gio_cao_diem
    Chu ky      : sin/cos cua gio va thu
    Thoi tiet   : nhiet_do, luong_mua, toc_do_gio tai khung_gio
    Vung        : loai_vung (san_bay/trung_tam/dan_cu -- suy tu taxi_zone_lookup.csv)

Tat ca cua so Window deu dung lag(,N) hoac rowsBetween(-N,-1) -- CHI lay du lieu TRUOC thoi
diem du bao, tranh data leakage (gia tri so_chuyen tai chinh khung_gio khong duoc dua vao
dac trung cua chinh no). dac_trung "khong gian" cung dung lag_1h (khong phai so_chuyen hien
tai) cua vung ke, cung vi ly do nay.

Luu y voi du lieu khong lien tuc (vd 2 thang rieng le khi test): Window theo vung, orderBy
khung_gio khong biet ve "khoang trong" giua cac thang -- lag_168h gan dau 1 thang se vo tinh
tham chieu sang cuoi thang truoc do trong du lieu da nap (du no cach xa ve thoi gian that).
Day la dac diem von co cua cua so Spark (sap theo thu tu, khong theo khoang cach thoi gian
that), khong phai loi -- se tu dong dung voi du lieu that lien tuc 24 thang.

    from tao_dac_trung import tao_dac_trung
    df_dac_trung = tao_dac_trung(spark, df_gold)

    # hoac chay doc lap (doc toan bo gold, ghi de toan bo features):
    spark-submit tao_dac_trung.py
"""
import math
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from ghep_thoi_tiet_spark import _doc_thoi_tiet_theo_gio

GOLD_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/gold/nhu_cau_theo_vung_gio"
FEATURES_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/features/dac_trung_nhu_cau"
MA_TRAN_KE_PATH = "hdfs://hadoop-master:9000/data/raw/tlc/lookup/ma_tran_ke.csv"
TAXI_ZONE_PATH = "hdfs://hadoop-master:9000/data/raw/tlc/lookup/taxi_zone_lookup.csv"
LICH_LE_PATH = "hdfs://hadoop-master:9000/data/raw/tlc/lookup/lich_le_my.csv"

SO_GIO_CAO_DIEM = 6  # top 6 gio/24 co trung binh so_chuyen cao nhat toan thanh pho -> gio cao diem


def _doc_thoi_tiet(spark):
    """Dung lai ham chung trong ghep_thoi_tiet_spark.py (nhiet_do, luong_mua, toc_do_gio,
    da gop san cac dong khung_gio trung do gio mo ho lui gio thang 11) -- chi giu lai 3 cot
    thoi tiet, bo la_ngay_le/ten_ngay_le vi tao_dac_trung.py tu xu ly rieng (_doc_ngay_le)."""
    return _doc_thoi_tiet_theo_gio(spark).select("khung_gio", "nhiet_do", "luong_mua", "toc_do_gio")


def _doc_ngay_le(spark):
    return (
        spark.read.option("header", True).option("inferSchema", True).csv(LICH_LE_PATH)
        .withColumn("ngay", F.to_date("ngay"))
        .select("ngay")
        .withColumn("la_ngay_le", F.lit(True))
    )


def _doc_loai_vung(spark):
    """Suy loai vung tu bang lookup goc (de cuong khong dinh nghia luat cu the):
    san_bay = Zone chua 'Airport' hoac service_zone la EWR/Airports;
    trung_tam = service_zone = 'Yellow Zone' (Manhattan loi);
    dan_cu = con lai (Boro Zone)."""
    lookup = spark.read.option("header", True).option("quote", '"').csv(TAXI_ZONE_PATH)
    return (
        lookup
        .withColumn("vung", F.col("LocationID").cast("int"))
        .withColumn(
            "loai_vung",
            F.when(
                F.lower(F.col("Zone")).contains("airport") | F.col("service_zone").isin("EWR", "Airports"),
                F.lit("san_bay"),
            )
            .when(F.col("service_zone") == "Yellow Zone", F.lit("trung_tam"))
            .otherwise(F.lit("dan_cu")),
        )
        .select("vung", "loai_vung")
    )


def _dac_trung_tre_truot(df):
    cua_so = Window.partitionBy("vung").orderBy("khung_gio")
    df = (
        df
        .withColumn("lag_1h", F.lag("so_chuyen", 1).over(cua_so))
        .withColumn("lag_2h", F.lag("so_chuyen", 2).over(cua_so))
        .withColumn("lag_24h", F.lag("so_chuyen", 24).over(cua_so))
        .withColumn("lag_168h", F.lag("so_chuyen", 168).over(cua_so))
    )
    for so_gio, ten in [(3, "3h"), (24, "24h"), (168, "168h")]:
        cua_so_truot = Window.partitionBy("vung").orderBy("khung_gio").rowsBetween(-so_gio, -1)
        df = (
            df
            .withColumn(f"trung_binh_truot_{ten}", F.avg("so_chuyen").over(cua_so_truot))
            .withColumn(f"do_lech_chuan_truot_{ten}", F.stddev("so_chuyen").over(cua_so_truot))
        )
    return df


def _dac_trung_khong_gian(df, ma_tran_ke):
    """Tong nhu cau cac vung ke O GIO TRUOC = tong lag_1h (khong phai so_chuyen hien tai)
    cua cac vung ke -- tranh leak thong tin cung khung_gio dang du bao."""
    nhu_cau_lag1 = df.select(
        F.col("vung").alias("_vung_ke_nguon"), "khung_gio", F.col("lag_1h").alias("_so_chuyen_vung_ke"),
    )
    lang_gieng = (
        ma_tran_ke
        .join(nhu_cau_lag1, ma_tran_ke["vung_ke"] == nhu_cau_lag1["_vung_ke_nguon"])
        .groupBy("vung", "khung_gio")
        .agg(F.sum("_so_chuyen_vung_ke").alias("nhu_cau_vung_ke_gio_truoc"))
    )
    return df.join(lang_gieng, on=["vung", "khung_gio"], how="left")


def _dac_trung_thoi_gian_chu_ky(df, ngay_le):
    df = (
        df
        .withColumn("gio", F.hour("khung_gio"))
        .withColumn("thu", F.dayofweek("khung_gio"))
        .withColumn("thang", F.month("khung_gio"))
        .withColumn("la_cuoi_tuan", F.col("thu").isin(1, 7))
        .withColumn("_ngay", F.to_date("khung_gio"))
        .join(ngay_le.withColumnRenamed("ngay", "_ngay"), on="_ngay", how="left")
        .withColumn("la_ngay_le", F.coalesce(F.col("la_ngay_le"), F.lit(False)))
        .drop("_ngay")
        .withColumn("sin_gio", F.sin(2 * math.pi * F.col("gio") / 24))
        .withColumn("cos_gio", F.cos(2 * math.pi * F.col("gio") / 24))
        .withColumn("sin_thu", F.sin(2 * math.pi * F.col("thu") / 7))
        .withColumn("cos_thu", F.cos(2 * math.pi * F.col("thu") / 7))
    )

    # Gio cao diem: tinh tu du lieu that (khong gia dinh san) -- top SO_GIO_CAO_DIEM gio/24
    # co trung binh so_chuyen toan thanh pho cao nhat.
    tb_theo_gio = (
        df.groupBy("gio").agg(F.avg("so_chuyen").alias("_tb_so_chuyen"))
        .orderBy(F.desc("_tb_so_chuyen"))
        .limit(SO_GIO_CAO_DIEM)
        .select("gio")
        .withColumn("la_gio_cao_diem", F.lit(True))
    )
    df = (
        df.join(tb_theo_gio, on="gio", how="left")
        .withColumn("la_gio_cao_diem", F.coalesce(F.col("la_gio_cao_diem"), F.lit(False)))
    )
    return df


def tao_dac_trung(spark, df_gold):
    ma_tran_ke = spark.read.option("header", True).option("inferSchema", True).csv(MA_TRAN_KE_PATH)
    thoi_tiet = _doc_thoi_tiet(spark)
    ngay_le = _doc_ngay_le(spark)
    loai_vung = _doc_loai_vung(spark)

    df = _dac_trung_tre_truot(df_gold)
    df = _dac_trung_khong_gian(df, ma_tran_ke)
    df = _dac_trung_thoi_gian_chu_ky(df, ngay_le)
    df = df.join(thoi_tiet, on="khung_gio", how="left")
    df = df.join(loai_vung, on="vung", how="left")
    return df


def main():
    spark = (
        SparkSession.builder.appName("tao_dac_trung")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    try:
        jvm_path = spark._jvm.org.apache.hadoop.fs.Path(GOLD_PATH + "/_delta_log")
        fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
        if not fs.exists(jvm_path):
            print("[LOI] Chua co gold.nhu_cau_theo_vung_gio, chay tao_nhu_cau_vung_gio.py truoc.")
            return 1

        df_gold = spark.read.format("delta").load(GOLD_PATH)
        so_dong_nguon = df_gold.count()

        df_dac_trung = tao_dac_trung(spark, df_gold)
        so_dong_ra = df_dac_trung.count()

        df_dac_trung.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(FEATURES_PATH)

        gold_count = spark.read.format("delta").load(FEATURES_PATH).count()

        so_dong_co_du_lag168 = df_dac_trung.where(F.col("lag_168h").isNotNull()).count()
        thong_ke = {
            "so_dong_nguon": so_dong_nguon,
            "so_dong_ra": so_dong_ra,
            "so_dong_trong_features": gold_count,
            "so_dong_co_du_lag_168h": so_dong_co_du_lag168,
            "so_cot": len(df_dac_trung.columns),
        }
        if gold_count != so_dong_ra or so_dong_ra != so_dong_nguon:
            print(f"[LOI] Lech so dong: nguon {so_dong_nguon:,} / ra {so_dong_ra:,} / ghi {gold_count:,}")
            print("[KET_QUA] " + str(thong_ke))
            return 1

        print("[KET_QUA] " + str(thong_ke))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
