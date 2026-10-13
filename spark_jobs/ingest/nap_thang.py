"""
Functions/nap_thang.py - Nap 1 thang Parquet TLC vao bronze.chuyen_di (de cuong muc 4.5,
ingest/nap_thang.py). Day la buoc dau tien cua chuoi pipeline, duoc DAG gt_nap_thang_moi va
gt_backfill goi lai cho tung thang.

Quy uoc da chot tu phien lam viec truoc (xem docs/00-nhat-ky-quyet-dinh.md TH-09/TH-10, da
ap dung dung trong nap_vao_bronze() cua xu_ly_va_du_bao.py cho tung gio -- file nay la ban
tuong duong cho ca 1 thang):
    - Giu nguyen schema goc tung thang (KHONG ep ve 1 schema chung) -- dung mergeSchema=true
      khi ghi, Delta tu hop nhat schema qua cac lan nap khac nam.
    - Them cot: ma_chuyen (SHA-256 toan bo cot goc, dung lam khoa MERGE idempotent),
      nam/thang (LAY THEO THANG CUA FILE NGUON -- khac voi silver.chuyen_di: nam/thang o silver
      la generated column tinh tu thoi_diem_don, con o day la gia tri khai bao tu tham so dong
      lenh, giu nguyen cho du vai dong co thoi_diem_don loi/khac thang trong file goc),
      ten_file_nguon, thoi_diem_nap.
    - Ban ghi trung hoan toan trong CUNG 1 file duoc gop con 1 TRUOC MERGE (dropDuplicates theo
      ma_chuyen) -- neu khong, MERGE voi khoa nguon trung se chen ca hai, mat tinh idempotent.
    - MERGE theo (nam, thang, ma_chuyen) -- chi quet dung partition thang dang nap, khong
      MERGE toan bang.

Idempotent: chay lai lenh cho cung 1 thang se MERGE 0 dong moi (dedup lai tu dau).

    spark-submit --packages io.delta:delta-spark_2.12:3.1.0 \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        nap_thang.py --nam 2024 --thang 1
"""
import argparse
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_TLC = "hdfs://hadoop-master:9000/data/raw/tlc/yellow"
BRONZE_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/bronze/chuyen_di"


def _duong_dan_ton_tai(spark, duong_dan):
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(duong_dan)
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(jvm_path)


def doc_parquet_thang(spark, nam, thang):
    duong_dan = f"{RAW_TLC}/yellow_tripdata_{nam}-{thang:02d}.parquet"
    return spark.read.parquet(duong_dan)


def nap_thang(spark, df_tho, nam, thang):
    """Tra ve (df_da_ghep_cot, so_dong_nguon, so_dong_sau_dedup) -- chua ghi, de ham goi kiem
    tra truoc khi ghi that (vd DAG 1 kiem_tra_schema() can doc cot ma khong ghi)."""
    cot_goc = df_tho.columns
    so_dong_nguon = df_tho.count()
    df = (
        df_tho.withColumn("ma_chuyen", F.sha2(F.concat_ws(
            "|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cot_goc]), 256))
        .withColumn("nam", F.lit(nam).cast("int"))
        .withColumn("thang", F.lit(thang).cast("int"))
        .withColumn("ten_file_nguon", F.lit(f"yellow_tripdata_{nam}-{thang:02d}.parquet"))
        .withColumn("thoi_diem_nap", F.current_timestamp())
        .dropDuplicates(["ma_chuyen"])
        # QUAN TRONG (phat hien qua chay that 2024-01): MERGE insert-only cua Delta don toan bo
        # ban ghi vao DUNG 1 task ghi neu khong ep lai so phan vung -- voi ~3 trieu dong/thang,
        # 1 task ghi tuan tu tren 1 loi CPU cuc ky cham (~0,3MB/s quan sat duoc). Ep repartition
        # de Delta chia viec ghi ra nhieu task song song, dung het so container cum dang co.
        .repartition(8)
    )
    so_dong_sau_dedup = df.count()
    return df, so_dong_nguon, so_dong_sau_dedup


def ghi_bronze(spark, df, nam, thang, merge_schema=False):
    """MERGE theo (nam, thang, ma_chuyen) -- chi quet dung partition dang nap. Tra ve so dong
    thuc su duoc chen moi (dem partition truoc/sau, giong quy uoc da dung cho silver/gold).

    merge_schema=True: dung khi DAG gt_nap_thang_moi phat hien schema thang moi khac schema
    chuan (kiem_tra_schema.py) -- bat autoMerge de Delta tu hop nhat schema (them/bo cot) ngay
    trong lenh MERGE, thay vi nem AnalysisException."""
    if merge_schema:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    if not _duong_dan_ton_tai(spark, BRONZE_PATH + "/_delta_log"):
        ghi = df.write.format("delta").partitionBy("nam", "thang")
        if merge_schema:
            ghi = ghi.option("mergeSchema", "true")
        ghi.save(BRONZE_PATH)
        return df.count()

    truoc = (
        spark.read.format("delta").load(BRONZE_PATH)
        .where((F.col("nam") == nam) & (F.col("thang") == thang))
        .count()
    )
    (
        DeltaTable.forPath(spark, BRONZE_PATH).alias("t")
        .merge(df.alias("s"),
               f"t.nam = {nam} AND t.thang = {thang} AND t.ma_chuyen = s.ma_chuyen")
        .whenNotMatchedInsertAll()
        .execute()
    )
    sau = (
        spark.read.format("delta").load(BRONZE_PATH)
        .where((F.col("nam") == nam) & (F.col("thang") == thang))
        .count()
    )
    return sau - truoc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nam", type=int, required=True)
    ap.add_argument("--thang", type=int, required=True)
    ap.add_argument("--merge-schema", action="store_true",
                     help="Bat autoMerge schema khi MERGE vao bronze (DAG 1 dung khi schema thang moi khac chuan)")
    a = ap.parse_args()

    spark = (
        SparkSession.builder.appName(f"nap_thang_{a.nam}_{a.thang:02d}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # QUAN TRONG (phat hien qua chay that): AQE (coalescePartitions, mac dinh BAT) tu dong
        # gop lai cac partition sau shuffle, VO HIEU HOA .repartition(8) o nap_thang() -- khien
        # MERGE insert-only van don ve 1 task ghi duy nhat, cuc cham (~0,3MB/s quan sat duoc tren
        # ~3 trieu dong/thang). Tat rieng coalescePartitions cho job nay de ep dung du 8 task
        # ghi song song nhu da repartition.
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .getOrCreate()
    )
    try:
        df_tho = doc_parquet_thang(spark, a.nam, a.thang)
        df, so_dong_nguon, so_dong_sau_dedup = nap_thang(spark, df_tho, a.nam, a.thang)
        so_dong_chen_moi = ghi_bronze(spark, df, a.nam, a.thang, merge_schema=a.merge_schema)

        ket_qua = {
            "nam": a.nam, "thang": a.thang,
            "so_dong_nguon": so_dong_nguon,
            "so_dong_sau_dedup": so_dong_sau_dedup,
            "so_dong_trung_trong_file": so_dong_nguon - so_dong_sau_dedup,
            "so_dong_chen_moi_vao_bronze": so_dong_chen_moi,
        }
        print("[KET_QUA]", ket_qua)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
