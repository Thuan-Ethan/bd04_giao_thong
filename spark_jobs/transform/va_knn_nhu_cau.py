"""
Functions/va_knn_nhu_cau.py - ADR-001: phat hien khung gio nghi ngo mat du lieu TOAN THANH
PHO trong gold.nhu_cau_theo_vung_gio, va bang trung binh cac lang gieng: chinh vung do + vung
ke (ma_tran_ke.csv), cung gio-trong-ngay + thu-trong-tuan, trong cua so +-6 tuan.

Khong dung ham va_knn() tong quat trong va_du_lieu_knn_spark.py: lang gieng o day dinh nghia
qua quan he KE CAN (do thi vung ke) chu khong phai khoang cach so Euclidean, nen can logic
rieng (join theo quan he, khong phai tinh khoang cach).

Phan biet 2 nguyen nhan "so_chuyen = 0" (de cuong muc 4.4 chi dien 0 cho moi gio khong co
chuyen that, khong phan biet voi loi thu thap): bo sung cot nguon_gia_tri
('thuc_te' | 'va_knn') -- BAT BUOC theo ADR-001, khong duoc tron lan 2 loai.

Buoc 1 - Phat hien: tong so_chuyen toan thanh pho theo tung khung_gio, so voi ky vong =
trung binh cung gio-trong-tuan (+-4 ky/tuan truoc-sau, 8 mau). Khung gio co tong thuc te <
ty_le_nguong x ky vong -> nghi ngo mat du lieu dong loat (loi thu thap, khong phai it khach
that -- it khach that se KHONG lam toan bo 263 vung cung tut giam dong thoi).

Buoc 2 - Va: voi moi (vung, khung_gio) thuoc khung gio bi danh dau, lay trung binh so_chuyen
cua tap lang gieng = {vung do + vung ke} x {cung gio-trong-ngay, cung thu-trong-tuan} trong
cua so +-6 tuan, CHI lay lang gieng "sach" (khong thuoc khung gio cung bi danh dau) de tranh
dung du lieu loi lam nguon vai.

Luu y hieu nang: can du lieu nhieu thang lien tuc de co du mau +-4/+-6 tuan quanh moi khung
gio -- chay tren du lieu qua ngan (vd 1 thang le te) se it/khong phat hien duoc gi o gan bien.

    from va_knn_nhu_cau import va_nhu_cau
    df_da_va, thong_ke = va_nhu_cau(spark, df_gold, ma_tran_ke)

    # hoac chay doc lap (doc toan bo gold, ghi de toan bo):
    spark-submit va_knn_nhu_cau.py
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GOLD_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/gold/nhu_cau_theo_vung_gio"
MA_TRAN_KE_PATH = "hdfs://hadoop-master:9000/data/raw/tlc/lookup/ma_tran_ke.csv"

SO_KY_SO_SANH = 4      # +-4 tuan de tinh ky vong
TY_LE_NGUONG = 0.5     # thuc te < 50% ky vong -> nghi ngo
CUA_SO_TUAN = 6        # +-6 tuan tim lang gieng khi va


def _doc_ma_tran_ke(spark):
    return (
        spark.read.option("header", True).option("inferSchema", True).csv(MA_TRAN_KE_PATH)
    )


def _phat_hien_khung_gio_nghi_ngo(spark, df_gold):
    tong_theo_gio = df_gold.groupBy("khung_gio").agg(F.sum("so_chuyen").alias("tong_so_chuyen"))

    do_lech = [d for d in range(-SO_KY_SO_SANH, SO_KY_SO_SANH + 1) if d != 0]
    df_do_lech = spark.createDataFrame([(d,) for d in do_lech], ["do_lech_tuan"])

    ky_vong_tho = (
        tong_theo_gio.alias("goc")
        .crossJoin(df_do_lech)
        .withColumn(
            "khung_gio_lang_gieng",
            F.expr("goc.khung_gio + make_interval(0,0,do_lech_tuan,0,0,0,0)"),
        )
        .join(
            tong_theo_gio.select(
                F.col("khung_gio").alias("khung_gio_lang_gieng"),
                F.col("tong_so_chuyen").alias("so_chuyen_lang_gieng"),
            ),
            on="khung_gio_lang_gieng", how="inner",
        )
    )
    ky_vong = (
        ky_vong_tho.groupBy(F.col("goc.khung_gio").alias("khung_gio"))
        .agg(
            F.avg("so_chuyen_lang_gieng").alias("ky_vong"),
            F.count("so_chuyen_lang_gieng").alias("so_mau_ky_vong"),
        )
    )

    nghi_ngo = (
        tong_theo_gio.join(ky_vong, on="khung_gio", how="inner")
        .where(
            (F.col("so_mau_ky_vong") >= 2)
            & (F.col("tong_so_chuyen") < TY_LE_NGUONG * F.col("ky_vong"))
        )
        .select("khung_gio", "tong_so_chuyen", "ky_vong", "so_mau_ky_vong")
    )
    return nghi_ngo


def _va_cac_khung_gio_nghi_ngo(df_gold, ma_tran_ke, khung_gio_nghi_ngo):
    df = (
        df_gold
        .withColumn("gio_trong_ngay", F.hour("khung_gio"))
        .withColumn("thu_trong_tuan", F.dayofweek("khung_gio"))
        .join(
            khung_gio_nghi_ngo.select("khung_gio").withColumn("la_xau", F.lit(True)),
            on="khung_gio", how="left",
        )
        .withColumn("la_xau", F.coalesce(F.col("la_xau"), F.lit(False)))
    )

    tap_vung = df.select("vung").distinct()
    tap_lang_gieng = (
        ma_tran_ke.select(F.col("vung").alias("vung_trung_tam"), F.col("vung_ke").alias("vung_ung_vien"))
        .unionByName(
            tap_vung.withColumnRenamed("vung", "vung_trung_tam")
            .withColumn("vung_ung_vien", F.col("vung_trung_tam"))
        )
        .distinct()
    )

    # Doi ten cot tuong minh truoc khi join (thay vi dua vao alias Dataset) -- tranh loi
    # "ambiguous self-join" cua Spark khi 2 nhanh cung xuat phat tu 1 DataFrame df.
    xau = (
        df.where("la_xau")
        .select(
            F.col("vung").alias("x_vung"),
            F.col("khung_gio").alias("x_khung_gio"),
            F.col("gio_trong_ngay").alias("x_gio_trong_ngay"),
            F.col("thu_trong_tuan").alias("x_thu_trong_tuan"),
        )
    )
    sach = (
        df.where("NOT la_xau")
        .select(
            F.col("vung").alias("s_vung"),
            F.col("khung_gio").alias("s_khung_gio"),
            F.col("gio_trong_ngay").alias("s_gio_trong_ngay"),
            F.col("thu_trong_tuan").alias("s_thu_trong_tuan"),
            F.col("so_chuyen").alias("s_so_chuyen"),
        )
    )

    ket_hop = (
        xau
        .join(tap_lang_gieng, xau["x_vung"] == tap_lang_gieng["vung_trung_tam"])
        .join(
            sach,
            (F.col("vung_ung_vien") == F.col("s_vung"))
            & (F.col("x_gio_trong_ngay") == F.col("s_gio_trong_ngay"))
            & (F.col("x_thu_trong_tuan") == F.col("s_thu_trong_tuan"))
            & (F.abs(F.datediff(F.col("x_khung_gio"), F.col("s_khung_gio"))) <= CUA_SO_TUAN * 7),
            "inner",
        )
    )

    gia_tri_va = (
        ket_hop.groupBy("x_vung", "x_khung_gio")
        .agg(
            F.avg("s_so_chuyen").alias("so_chuyen_va"),
            F.count("s_so_chuyen").alias("so_lang_gieng_dung"),
        )
        .withColumnRenamed("x_vung", "vung")
        .withColumnRenamed("x_khung_gio", "khung_gio")
    )

    ket_qua = (
        df_gold
        .join(gia_tri_va, on=["vung", "khung_gio"], how="left")
        .withColumn(
            "nguon_gia_tri",
            F.when(F.col("so_chuyen_va").isNotNull(), F.lit("va_knn")).otherwise(F.lit("thuc_te")),
        )
        .withColumn(
            "so_chuyen",
            F.when(F.col("so_chuyen_va").isNotNull(), F.round(F.col("so_chuyen_va")).cast("int"))
            .otherwise(F.col("so_chuyen")),
        )
        .drop("so_chuyen_va", "so_lang_gieng_dung")
    )
    return ket_qua, gia_tri_va


def va_nhu_cau(spark, df_gold, ma_tran_ke=None):
    """
    Tra ve (df_da_va, thong_ke).
    df_da_va: df_gold + cot nguon_gia_tri ('thuc_te' | 'va_knn'); so_chuyen cua khung gio bi
        danh dau duoc thay bang trung binh lang gieng (lam tron ve int).
    thong_ke: dict so_khung_gio_nghi_ngo / so_o_da_va / so_o_khong_tim_duoc_lang_gieng.
    """
    if ma_tran_ke is None:
        ma_tran_ke = _doc_ma_tran_ke(spark)

    khung_gio_nghi_ngo = _phat_hien_khung_gio_nghi_ngo(spark, df_gold).cache()
    so_khung_gio_nghi_ngo = khung_gio_nghi_ngo.count()

    if so_khung_gio_nghi_ngo == 0:
        ket_qua = df_gold.withColumn("nguon_gia_tri", F.lit("thuc_te"))
        return ket_qua, {"so_khung_gio_nghi_ngo": 0, "so_o_da_va": 0}

    ket_qua, gia_tri_va = _va_cac_khung_gio_nghi_ngo(df_gold, ma_tran_ke, khung_gio_nghi_ngo)
    so_o_da_va = gia_tri_va.count()

    so_o_thuoc_khung_gio_xau = (
        df_gold.join(khung_gio_nghi_ngo.select("khung_gio"), on="khung_gio", how="inner").count()
    )
    thong_ke = {
        "so_khung_gio_nghi_ngo": so_khung_gio_nghi_ngo,
        "so_o_thuoc_khung_gio_nghi_ngo": so_o_thuoc_khung_gio_xau,
        "so_o_da_va": so_o_da_va,
        "so_o_khong_tim_duoc_lang_gieng": so_o_thuoc_khung_gio_xau - so_o_da_va,
    }
    khung_gio_nghi_ngo.unpersist()
    return ket_qua, thong_ke


def main():
    spark = (
        SparkSession.builder.appName("va_knn_nhu_cau")
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
        ket_qua, thong_ke = va_nhu_cau(spark, df_gold)

        (ket_qua.write.format("delta").mode("overwrite")
         .option("mergeSchema", "true")
         .save(GOLD_PATH))

        print("[KET_QUA] " + str(thong_ke))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
