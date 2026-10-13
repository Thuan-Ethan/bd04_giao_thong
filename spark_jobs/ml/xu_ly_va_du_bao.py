"""
Functions/xu_ly_va_du_bao.py - pipeline xu ly 1 khung gio du lieu moi + du bao nhu cau cho
khung gio tiep theo (Model A). Tuong ung du_bao.py + DAG gt_du_bao trong de cuong muc 4.5/4.9
(chay moi 30 phut, du bao horizon 1h -- 2h se them sau theo xac nhan nguoi dung).

Ghep lai CAC HAM DA CO, khong viet lai logic lam sach/dac trung lan 2:
    1. Nap khung gio moi vao bronze       -- MERGE theo ma_chuyen (giong nap_thang.py)
    2. Lam sach vao silver                -- lam_sach_silver.lam_sach(), ghi HEP theo dung
                                              khung gio (khac ghi_silver() goc ghi theo thang)
    3. Tong hop vao gold.nhu_cau_theo_vung_gio -- ghi HEP theo dung khung gio
    4. Them 1 "luoi ao" cho khung_gio_muc_tieu (so_chuyen=NULL) vao gold, dua CA KHOI vao
       tao_dac_trung.tao_dac_trung() -- dam bao cong thuc lag/truot/khong gian/thoi tiet TINH
       Y HET luc huan luyen (tranh training/serving skew), khong viet lai cong thuc lan 2.
    5. Loc dung 263 dong cua khung_gio_muc_tieu lam dau vao, load PipelineModel da train,
       transform, ghi gold.du_bao_nhu_cau (schema theo de cuong muc 4.4).

LUU Y (ghi trong docs, xem docs/04-trang-thai-va-ke-hoach.md):
    - Model dung hien tai (mo_hinh_a_rf) chi la smoke-test tren 2 thang roi rac, CHUA PHAI
      model that -- chi de dung/kiem thu pipeline nay chay dung dau-cuoi. Thay duong dan model
      sau khi huan luyen lai tren 24 thang, khong can doi code.
    - "khung gio moi" hien gia lap bang cach doc truc tiep 1 lat cat gio tu file TLC thang da co
      tren HDFS (chua co Kafka that o Giai doan nay) -- xem trich_xuat_tho_theo_gio().
    - Thoi tiet tai khung_gio_muc_tieu (tuong lai) lay tu CSV lich su da co san (vi dang
      backtest tren du lieu TLC that cua qua khu) -- san xuat that se can API du bao thoi tiet
      thay vi lich su.
    - can_duoi/can_tren (khoang tin cay) CHUA tinh -- RandomForestRegressor cua MLlib khong tra
      ve truc tiep; de NULL, can quyet dinh cach uoc luong sau (vd quantile tu cac cay con).

    spark-submit xu_ly_va_du_bao.py --khung-gio "2023-02-20 14:00:00" --horizon 1 \
        --duong-dan-model hdfs:///lakehouse/bd04_giao_thong/models/mo_hinh_a_rf/20261010_184315
"""
import argparse
import datetime
import sys

from delta.tables import DeltaTable
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

from lam_sach_silver import lam_sach
from tao_dac_trung import tao_dac_trung

RAW_TLC = "hdfs://hadoop-master:9000/data/raw/tlc/yellow"
BRONZE_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/bronze/chuyen_di"
SILVER_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/silver/chuyen_di"
QUARANTINE_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/_quarantine/chuyen_di"
GOLD_NHU_CAU_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/gold/nhu_cau_theo_vung_gio"
DU_BAO_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/gold/du_bao_nhu_cau"
TAXI_ZONE_PATH = "hdfs://hadoop-master:9000/data/raw/tlc/lookup/taxi_zone_lookup.csv"
VUNG_MIN, VUNG_MAX = 1, 263


def _duong_dan_ton_tai(spark, duong_dan):
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(duong_dan)
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(jvm_path)


# ---------- Buoc 1: nap khung gio moi vao bronze (gia lap "du lieu moi nhan") ----------

def trich_xuat_tho_theo_gio(spark, khung_gio):
    """Gia lap nguon du lieu streaming: doc parquet TLC cua dung thang chua khung_gio, loc
    cac chuyen co tpep_pickup_datetime trong [khung_gio, khung_gio + 1h). San xuat that se
    thay buoc nay bang doc tu Kafka (Giai doan 2)."""
    duong_dan = f"{RAW_TLC}/yellow_tripdata_{khung_gio.year}-{khung_gio.month:02d}.parquet"
    ket_thuc = khung_gio + datetime.timedelta(hours=1)
    return (
        spark.read.parquet(duong_dan)
        .where(
            (F.col("tpep_pickup_datetime") >= F.lit(khung_gio))
            & (F.col("tpep_pickup_datetime") < F.lit(ket_thuc))
        )
    )


def nap_vao_bronze(spark, df_tho, khung_gio):
    """MERGE theo ma_chuyen, giong logic nap_thang.py -- idempotent, chay lai khong nhan doi."""
    cot_goc = df_tho.columns
    df = (
        df_tho.withColumn("ma_chuyen", F.sha2(F.concat_ws(
            "|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cot_goc]), 256))
        .withColumn("nam", F.lit(khung_gio.year).cast("int"))
        .withColumn("thang", F.lit(khung_gio.month).cast("int"))
        .withColumn("ten_file_nguon", F.lit(f"streaming_{khung_gio.isoformat()}"))
        .withColumn("thoi_diem_nap", F.current_timestamp())
        .dropDuplicates(["ma_chuyen"])
    )
    if not _duong_dan_ton_tai(spark, BRONZE_PATH + "/_delta_log"):
        df.write.format("delta").partitionBy("nam", "thang").save(BRONZE_PATH)
        return df.count()
    (
        DeltaTable.forPath(spark, BRONZE_PATH).alias("t")
        .merge(df.alias("s"),
               f"t.nam = {khung_gio.year} AND t.thang = {khung_gio.month} AND t.ma_chuyen = s.ma_chuyen")
        .whenNotMatchedInsertAll()
        .execute()
    )
    return df.count()


# ---------- Buoc 2: lam sach vao silver, ghi HEP theo dung khung gio ----------

def lam_sach_va_ghi_mot_gio(spark, khung_gio):
    """Doc lai bronze dung khung gio nay, lam sach (dung lai lam_sach() khong doi), ghi silver
    + _quarantine bang replaceWhere theo DUNG KHUNG GIO (khac ghi_silver() goc trong
    lam_sach_silver.py ghi theo CA THANG -- neu dung ham goc o day se xoa mat cac gio khac
    trong cung thang da xu ly truoc do)."""
    ket_thuc = khung_gio + datetime.timedelta(hours=1)
    bronze_gio = (
        spark.read.format("delta").load(BRONZE_PATH)
        .where((F.col("nam") == khung_gio.year) & (F.col("thang") == khung_gio.month))
        .where(
            (F.col("tpep_pickup_datetime") >= F.lit(khung_gio))
            & (F.col("tpep_pickup_datetime") < F.lit(ket_thuc))
        )
    )
    df_sach, df_quarantine, thong_ke = lam_sach(bronze_gio, khung_gio.year, khung_gio.month)

    dieu_kien = f"thoi_diem_don >= timestamp'{khung_gio}' AND thoi_diem_don < timestamp'{ket_thuc}'"
    for duong_dan, df in [(SILVER_PATH, df_sach), (QUARANTINE_PATH, df_quarantine)]:
        if not _duong_dan_ton_tai(spark, duong_dan + "/_delta_log"):
            df.write.format("delta").partitionBy("nam", "thang").save(duong_dan)
        else:
            (df.write.format("delta").mode("overwrite")
             .option("replaceWhere", dieu_kien)
             .save(duong_dan))
    return thong_ke


# ---------- Buoc 3: tong hop vao gold, ghi HEP theo dung khung gio ----------

def tong_hop_mot_gio(spark, khung_gio):
    """Tuong duong tao_nhu_cau_vung_gio.py nhung chi cho 1 khung_gio (khong cross join ca
    thang) -- ghi bang replaceWhere "khung_gio = ..." , khong dung het dung luong cua ca gold
    moi lan co du lieu moi."""
    vung = (
        spark.read.option("header", True).option("quote", '"').csv(TAXI_ZONE_PATH)
        .withColumn("LocationID", F.col("LocationID").cast("int"))
        .filter((F.col("LocationID") >= VUNG_MIN) & (F.col("LocationID") <= VUNG_MAX))
        .select(F.col("LocationID").alias("vung"))
    )
    ket_thuc = khung_gio + datetime.timedelta(hours=1)
    silver_gio = (
        spark.read.format("delta").load(SILVER_PATH)
        .where((F.col("thoi_diem_don") >= F.lit(khung_gio)) & (F.col("thoi_diem_don") < F.lit(ket_thuc)))
    )
    thuc_te = (
        silver_gio.groupBy(F.col("vung_don").alias("vung"))
        .agg(
            F.count("*").alias("so_chuyen"),
            F.sum("tong_tien").alias("tong_doanh_thu"),
            F.avg("thoi_gian_phut").alias("thoi_gian_chuyen_tb"),
            F.avg("quang_duong_mile").alias("quang_duong_tb"),
        )
    )
    ket_qua = (
        vung.withColumn("khung_gio", F.lit(khung_gio).cast("timestamp"))
        .join(thuc_te, on="vung", how="left")
        .withColumn("so_chuyen", F.coalesce(F.col("so_chuyen"), F.lit(0)).cast("int"))
        .withColumn("tong_doanh_thu", F.coalesce(F.col("tong_doanh_thu"), F.lit(0.0)))
        .withColumn("nguon_gia_tri", F.lit("thuc_te"))
        .select("vung", "khung_gio", "so_chuyen", "tong_doanh_thu",
                "thoi_gian_chuyen_tb", "quang_duong_tb", "nguon_gia_tri")
    )

    dieu_kien = f"khung_gio = timestamp'{khung_gio}'"
    if not _duong_dan_ton_tai(spark, GOLD_NHU_CAU_PATH + "/_delta_log"):
        ket_qua.write.format("delta").save(GOLD_NHU_CAU_PATH)
    else:
        (ket_qua.write.format("delta").mode("overwrite")
         .option("replaceWhere", dieu_kien).option("mergeSchema", "true")
         .save(GOLD_NHU_CAU_PATH))
    return ket_qua.count()


# ---------- Buoc 4+5: luoi ao cho khung_gio_muc_tieu, tinh dac trung, du bao ----------

def _luoi_ao(spark, khung_gio_muc_tieu):
    """263 dong "rong" cho khung_gio_muc_tieu (tuong lai, chua co du lieu that) -- dua vao
    cung 1 Window voi gold that de tao_dac_trung() tu tinh dung lag/truot (lag() khong can
    biet gia tri CURRENT row, chi can cac row TRUOC no trong cung Window)."""
    schema = StructType([
        StructField("vung", IntegerType()),
        StructField("khung_gio", TimestampType()),
        StructField("so_chuyen", IntegerType()),
        StructField("tong_doanh_thu", DoubleType()),
        StructField("thoi_gian_chuyen_tb", DoubleType()),
        StructField("quang_duong_tb", DoubleType()),
        StructField("nguon_gia_tri", StringType()),
    ])
    return spark.createDataFrame(
        [(v, khung_gio_muc_tieu, None, None, None, None, "du_bao") for v in range(VUNG_MIN, VUNG_MAX + 1)],
        schema=schema,
    )


def du_bao_mot_khung_gio(spark, khung_gio_hien_tai, horizon_gio, duong_dan_model):
    """Tra ve DataFrame ket qua du bao (vung, khung_gio_muc_tieu, so_chuyen_du_bao, horizon_gio)
    cho khung_gio_hien_tai + horizon_gio."""
    khung_gio_muc_tieu = khung_gio_hien_tai + datetime.timedelta(hours=horizon_gio)

    # Loai bo khung_gio_muc_tieu khoi gold that TRUOC khi ghep luoi ao -- neu khong, truong
    # hop test/backtest tren thang da nap du (khung_gio_muc_tieu da co du lieu that san) se bi
    # NHAN DOI dong (1 that + 1 ao) cho cung (vung, khung_gio), lam Window tinh lag/truot sai
    # (phat hien qua chay that: cung 1 vung ra 2 gia tri du doan khac nhau).
    gold_that = (
        spark.read.format("delta").load(GOLD_NHU_CAU_PATH)
        .where(F.col("khung_gio") != F.lit(khung_gio_muc_tieu))
    )
    luoi_ao = _luoi_ao(spark, khung_gio_muc_tieu)
    ca_khoi = gold_that.unionByName(luoi_ao)

    dac_trung = tao_dac_trung(spark, ca_khoi)
    dau_vao_du_bao = dac_trung.where(F.col("khung_gio") == F.lit(khung_gio_muc_tieu))

    mo_hinh = PipelineModel.load(duong_dan_model)
    ket_qua_tho = mo_hinh.transform(dau_vao_du_bao)

    cot_du_doan = "du_doan_rf" if "du_doan_rf" in ket_qua_tho.columns else "du_doan_gbt"
    return (
        ket_qua_tho
        .select(
            "vung",
            F.lit(khung_gio_hien_tai).cast("timestamp").alias("thoi_diem_du_bao"),
            F.lit(khung_gio_muc_tieu).cast("timestamp").alias("khung_gio_muc_tieu"),
            F.lit(horizon_gio).cast("int").alias("horizon_gio"),
            F.col(cot_du_doan).cast("double").alias("so_chuyen_du_bao"),
            F.lit(None).cast("double").alias("can_duoi"),
            F.lit(None).cast("double").alias("can_tren"),
            F.lit(duong_dan_model.split("/")[-1]).alias("phien_ban_mo_hinh"),
        )
    )


def ghi_du_bao(spark, df_du_bao, khung_gio_hien_tai, horizon_gio):
    dieu_kien = (
        f"thoi_diem_du_bao = timestamp'{khung_gio_hien_tai}' AND horizon_gio = {horizon_gio}"
    )
    if not _duong_dan_ton_tai(spark, DU_BAO_PATH + "/_delta_log"):
        df_du_bao.write.format("delta").save(DU_BAO_PATH)
    else:
        (df_du_bao.write.format("delta").mode("overwrite")
         .option("replaceWhere", dieu_kien)
         .save(DU_BAO_PATH))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--khung-gio", required=True, help="YYYY-MM-DD HH:MM:SS, gio tron")
    ap.add_argument("--horizon", type=int, default=1, choices=[1, 2])
    ap.add_argument("--duong-dan-model", required=True)
    ap.add_argument("--bo-qua-nap-bronze", action="store_true",
                     help="Dung khi du lieu da nap san vao bronze qua streaming "
                          "(nhan_chuyen_streaming.py) -- bo qua buoc doc lai tu TLC raw.")
    a = ap.parse_args()
    khung_gio = datetime.datetime.strptime(a.khung_gio, "%Y-%m-%d %H:%M:%S")

    spark = (
        SparkSession.builder.appName(f"xu_ly_va_du_bao_{khung_gio.isoformat()}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    try:
        if a.bo_qua_nap_bronze:
            so_dong_tho = None
        else:
            df_tho = trich_xuat_tho_theo_gio(spark, khung_gio)
            so_dong_tho = nap_vao_bronze(spark, df_tho, khung_gio)

        thong_ke_lam_sach = lam_sach_va_ghi_mot_gio(spark, khung_gio)
        so_o_gold = tong_hop_mot_gio(spark, khung_gio)

        df_du_bao = du_bao_mot_khung_gio(spark, khung_gio, a.horizon, a.duong_dan_model)
        mau_du_bao = [r.asDict() for r in df_du_bao.orderBy(F.desc("so_chuyen_du_bao")).limit(5).collect()]
        ghi_du_bao(spark, df_du_bao, khung_gio, a.horizon)

        ket_qua = {
            "khung_gio": str(khung_gio),
            "horizon_gio": a.horizon,
            "so_dong_tho_nhan": so_dong_tho,
            "thong_ke_lam_sach": thong_ke_lam_sach,
            "so_o_gold_cap_nhat": so_o_gold,
            "top5_vung_du_bao_cao_nhat": mau_du_bao,
        }
        print("[KET_QUA] " + str(ket_qua))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
