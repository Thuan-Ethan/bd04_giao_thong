"""
Functions/lam_sach_silver.py - chuan hoa bronze.chuyen_di -> silver.chuyen_di (+ _quarantine,
+ thong_ke_loai_bo). Ke thua logic da duoc nhom xac nhan dung truoc do (git log
spark_jobs/transform/bronze_to_silver.py, commit b3c6090, da bi xoa khi lam moi project) --
chi sua lai cho khop bronze hien tai: cot khoa la `ma_chuyen` (khong con `trip_id`), bo
quy tac loc theo `dup_seq` vi `nap_thang.py` hien tai da `dropDuplicates(["ma_chuyen"])`
ngay luc nap, khong con cot dup_seq trong bronze.

Ban ghi vung_don/vung_tra ngoai 1-263 di vao _quarantine (khong xoa). Cac luat khac vi pham
thi loai han (chi dem vao thong_ke, khong luu o dau). nam/thang/ngay la Delta generated
column tinh tu thoi_diem_don (khong nam trong DataFrame khi ghi, Delta tu tinh) -> partition
pruning van hoat dong khi truy van loc theo thoi_diem_don.

Idempotent theo thang: silver TINH LAI TOAN BO 1 thang moi lan chay (khong MERGE nhu bronze),
ghi de dung partition (nam, thang) bang replaceWhere.

    from lam_sach_silver import lam_sach, ghi_silver
    df_sach, df_quarantine, thong_ke = lam_sach(df_bronze_1_thang, nam=2022, thang=1)

    # hoac chay doc lap (doc bronze, lam sach, ghi xuong HDFS):
    spark-submit lam_sach_silver.py --nam 2022 --thang 1
"""
import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/bronze/chuyen_di"
SILVER_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/silver/chuyen_di"
QUARANTINE_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/_quarantine/chuyen_di"
THONG_KE_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/silver/thong_ke_loai_bo"

VUNG_MIN, VUNG_MAX = 1, 263  # 264/265 = "Unknown"/ngoai NYC -> quarantine, khong phai loi

QUY_TAC_LOAI = [
    ("thoi_diem_tra_khong_sau_don", "thoi_diem_tra <= thoi_diem_don"),
    ("thoi_gian_phut_ngoai_1_300", "thoi_gian_phut < 1 OR thoi_gian_phut > 300"),
    ("quang_duong_ngoai_0_100", "quang_duong_mile <= 0 OR quang_duong_mile > 100"),
    ("toc_do_tb_qua_80", "van_toc_tb_mph > 80"),
    ("so_khach_ngoai_1_8", "so_khach = 0 OR so_khach > 8"),
    ("tong_tien_am", "tong_tien < 0"),
]
NHAN_HINH_THUC_TT = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided trip"}


def _nhan_hinh_thuc_tt(cot):
    anh_xa = F.create_map(*[x for k, v in NHAN_HINH_THUC_TT.items() for x in (F.lit(k), F.lit(v))])
    return F.coalesce(anh_xa[cot], F.concat(F.lit("Unknown("), cot.cast("string"), F.lit(")")))


def _chon_cot(df_bronze):
    return df_bronze.select(
        F.col("ma_chuyen"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("thoi_diem_don"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("thoi_diem_tra"),
        F.col("PULocationID").cast("int").alias("vung_don"),
        F.col("DOLocationID").cast("int").alias("vung_tra"),
        F.col("passenger_count").cast("int").alias("so_khach"),
        F.col("trip_distance").alias("quang_duong_mile"),
        (
            (F.col("tpep_dropoff_datetime").cast("timestamp").cast("long")
             - F.col("tpep_pickup_datetime").cast("timestamp").cast("long")) / 60.0
        ).alias("thoi_gian_phut"),
        F.col("total_amount").alias("tong_tien"),
        _nhan_hinh_thuc_tt(F.col("payment_type")).alias("hinh_thuc_tt"),
    ).withColumn(
        "van_toc_tb_mph",
        F.when(F.col("thoi_gian_phut") > 0,
               F.col("quang_duong_mile") / (F.col("thoi_gian_phut") / 60.0)),
    )


def lam_sach(df_bronze_1_thang, nam, thang):
    """
    df_bronze_1_thang: DataFrame bronze.chuyen_di DA LOC rieng 1 thang (nam, thang).
    Tra ve (df_sach, df_quarantine, thong_ke):
        df_sach       - dong hop le, dung sinh silver.chuyen_di.
        df_quarantine - dong vung_don/vung_tra ngoai 1-263 (khong phai loi, chi cach ly).
        thong_ke      - dict {ten_quy_tac: so_dong_bi_loai} + tong_dong/so_dong_sach/
                        so_dong_quarantine/so_dong_loai.
    """
    tong_dong = df_bronze_1_thang.count()
    df = _chon_cot(df_bronze_1_thang).cache()

    thong_ke = {}
    bi_loai = F.lit(False)
    for ten, dieu_kien in QUY_TAC_LOAI:
        # coalesce(..., False): gia tri NULL (vd so_khach thieu) KHONG duoc tinh la "loai",
        # tranh bay logic 3 gia tri cua SQL (NULL OR False = NULL, khong phai False, khien
        # .filter(~bi_loai) am tham loai luon ca dong khong khop quy tac nao)
        dieu_kien_an_toan = F.coalesce(F.expr(dieu_kien), F.lit(False))
        n = df.filter(dieu_kien_an_toan).count()
        thong_ke[ten] = n
        bi_loai = bi_loai | dieu_kien_an_toan

    # nam/thang la generated column tinh tu thoi_diem_don -> mot so dong TLC co thoi_diem_don
    # lech voi thang khai bao trong ten file (co khi lech ca nam) phai loai truoc khi ghi,
    # khong chi de "lam sach" ma de dam bao ghi duoc (Delta tu choi neu replaceWhere sai).
    lech_thang_khai_bao = F.coalesce(
        (F.year("thoi_diem_don") != nam) | (F.month("thoi_diem_don") != thang),
        F.lit(True),  # thoi_diem_don NULL -> khong xac dinh duoc nam/thang -> loai
    )
    n_lech = df.filter(lech_thang_khai_bao).count()
    thong_ke["ngay_don_lech_thang_khai_bao"] = n_lech
    bi_loai = bi_loai | lech_thang_khai_bao

    con_lai = df.filter(~bi_loai)
    vung_loi = F.coalesce(
        (F.col("vung_don") < VUNG_MIN) | (F.col("vung_don") > VUNG_MAX)
        | (F.col("vung_tra") < VUNG_MIN) | (F.col("vung_tra") > VUNG_MAX),
        F.lit(False),
    )
    df_quarantine = con_lai.filter(vung_loi)
    df_sach = con_lai.filter(~vung_loi)

    so_quarantine = df_quarantine.count()
    so_sach = df_sach.count()
    thong_ke.update({
        "tong_dong": tong_dong,
        "so_dong_sach": so_sach,
        "so_dong_quarantine": so_quarantine,
        "so_dong_loai": tong_dong - so_sach - so_quarantine,
    })
    df.unpersist()
    return df_sach, df_quarantine, thong_ke


def _duong_dan_ton_tai(spark, duong_dan):
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(duong_dan)
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(jvm_path)


def _tao_bang_lan_dau(spark, duong_dan, co_cot_ngay):
    from delta.tables import DeltaTable
    builder = (
        DeltaTable.createOrReplace(spark)
        .location(duong_dan)
        .addColumn("ma_chuyen", "STRING")
        .addColumn("thoi_diem_don", "TIMESTAMP")
        .addColumn("thoi_diem_tra", "TIMESTAMP")
        .addColumn("vung_don", "INT")
        .addColumn("vung_tra", "INT")
        .addColumn("so_khach", "INT")
        .addColumn("quang_duong_mile", "DOUBLE")
        .addColumn("thoi_gian_phut", "DOUBLE")
        .addColumn("van_toc_tb_mph", "DOUBLE")
        .addColumn("tong_tien", "DOUBLE")
        .addColumn("hinh_thuc_tt", "STRING")
        .addColumn("nam", "INT", generatedAlwaysAs="YEAR(thoi_diem_don)")
        .addColumn("thang", "INT", generatedAlwaysAs="MONTH(thoi_diem_don)")
    )
    if co_cot_ngay:
        builder = builder.addColumn("ngay", "DATE", generatedAlwaysAs="CAST(thoi_diem_don AS DATE)")
    builder.partitionedBy("nam", "thang").execute()


def ghi_silver(spark, df_sach, df_quarantine, thong_ke, nam, thang):
    """Ghi df_sach/df_quarantine/thong_ke xuong HDFS (ghi de dung partition nam/thang)."""
    if not _duong_dan_ton_tai(spark, SILVER_PATH + "/_delta_log"):
        _tao_bang_lan_dau(spark, SILVER_PATH, co_cot_ngay=True)
    if not _duong_dan_ton_tai(spark, QUARANTINE_PATH + "/_delta_log"):
        _tao_bang_lan_dau(spark, QUARANTINE_PATH, co_cot_ngay=False)

    (df_sach.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"nam = {nam} AND thang = {thang}")
     .save(SILVER_PATH))
    (df_quarantine.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"nam = {nam} AND thang = {thang}")
     .save(QUARANTINE_PATH))

    dong_thong_ke = spark.createDataFrame([{**{"nam": nam, "thang": thang}, **thong_ke}])
    if not _duong_dan_ton_tai(spark, THONG_KE_PATH + "/_delta_log"):
        dong_thong_ke.write.format("delta").partitionBy("nam", "thang").save(THONG_KE_PATH)
    else:
        (dong_thong_ke.write.format("delta").mode("overwrite")
         .option("replaceWhere", f"nam = {nam} AND thang = {thang}")
         .save(THONG_KE_PATH))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nam", type=int, required=True)
    ap.add_argument("--thang", type=int, required=True, choices=range(1, 13), metavar="[1-12]")
    a = ap.parse_args()

    spark = (
        SparkSession.builder.appName(f"lam_sach_silver_{a.nam}_{a.thang:02d}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    try:
        bronze_1_thang = (
            spark.read.format("delta").load(BRONZE_PATH)
            .where((F.col("nam") == a.nam) & (F.col("thang") == a.thang))
        )
        if not bronze_1_thang.take(1):
            print(f"[LOI] bronze khong co du lieu cho {a.nam}-{a.thang:02d}. Chay nap_thang truoc.")
            return 1

        df_sach, df_quarantine, thong_ke = lam_sach(bronze_1_thang, a.nam, a.thang)
        ghi_silver(spark, df_sach, df_quarantine, thong_ke, a.nam, a.thang)

        print("[KET_QUA] " + str(thong_ke))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
