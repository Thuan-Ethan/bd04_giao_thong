"""
Functions/nhan_chuyen_streaming.py - Spark Structured Streaming: doc Kafka topic chuyen_dang_chay
(JSON chuyen di tho, do phat_lai_chuyen_di.py gui), MERGE vao bronze.chuyen_di theo micro-batch.
CHI lam viec nay -- khong lam sach/tong hop/du bao ngay trong job streaming (thiet ke da thong
nhat voi nguoi dung: tach Spark Streaming [chi nap] khoi Airflow-trigger batch [lam sach + gold +
feature + du bao, goi lai xu_ly_va_du_bao.py]).

Dong thoi cap nhat 1 bang kiem soat nho `gold.trang_thai_streaming` (1 dong duy nhat) ghi gio du
lieu moi nhat da nap -- de Airflow Sensor doc nhe (1 dong), khong phai quet toan bo bronze moi
lan kiem tra "da du 1 gio mo phong chua".

LUU Y: schema JSON dinh nghia san khop voi schema TLC 2023-02->2024-12 (VendorID/PU/DO int,
passenger_count/RatecodeID bigint, Airport_fee) -- dung thang khac nam schema khac (vd 2022, hoac
2025 co them cbd_congestion_fee) se can mo rong SCHEMA_TLC truoc.

    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,io.delta:delta-spark_2.12:3.1.0 \\
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \\
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \\
        nhan_chuyen_streaming.py
"""
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

KAFKA_BOOTSTRAP = "hadoop-master:9092"
TOPIC = "chuyen_dang_chay"
BRONZE_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/bronze/chuyen_di"
TRANG_THAI_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/gold/trang_thai_streaming"
CHECKPOINT_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/checkpoints/nhan_chuyen_streaming"

SCHEMA_TLC = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", StringType()),
    StructField("tpep_dropoff_datetime", StringType()),
    StructField("passenger_count", LongType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", LongType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", LongType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("Airport_fee", DoubleType()),
])


def _duong_dan_ton_tai(spark, duong_dan):
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(duong_dan)
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(jvm_path)


def xu_ly_mot_micro_batch(df_batch, id_batch):
    if df_batch.rdd.isEmpty():
        return
    spark = df_batch.sparkSession

    df = (
        df_batch
        .withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
    )
    cot_hash = df.columns
    df = (
        df.withColumn("ma_chuyen", F.sha2(F.concat_ws(
            "|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cot_hash]), 256))
        .withColumn("nam", F.year("tpep_pickup_datetime").cast("int"))
        .withColumn("thang", F.month("tpep_pickup_datetime").cast("int"))
        .withColumn("ten_file_nguon", F.lit("streaming_chuyen_dang_chay"))
        .withColumn("thoi_diem_nap", F.current_timestamp())
        .where(F.col("nam").isNotNull() & F.col("thang").isNotNull())
        .dropDuplicates(["ma_chuyen"])
    )

    if not _duong_dan_ton_tai(spark, BRONZE_PATH + "/_delta_log"):
        df.write.format("delta").partitionBy("nam", "thang").save(BRONZE_PATH)
    else:
        cac_thang = [r.asDict() for r in df.select("nam", "thang").distinct().collect()]
        dieu_kien_thang = " OR ".join(
            f"(t.nam = {r['nam']} AND t.thang = {r['thang']})" for r in cac_thang
        )
        (
            DeltaTable.forPath(spark, BRONZE_PATH).alias("t")
            .merge(df.alias("s"), f"({dieu_kien_thang}) AND t.ma_chuyen = s.ma_chuyen")
            .whenNotMatchedInsertAll()
            .execute()
        )

    gio_moi_nhat = df.select(F.max(F.date_trunc("hour", "tpep_pickup_datetime"))).first()[0]
    if gio_moi_nhat is not None:
        dong_trang_thai = spark.createDataFrame(
            [("trang_thai", gio_moi_nhat)], ["id", "gio_du_lieu_moi_nhat"]
        )
        (dong_trang_thai.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").save(TRANG_THAI_PATH))

    print(f"[BATCH {id_batch}] {df.count()} dong, gio moi nhat={gio_moi_nhat}")


def main():
    spark = (
        SparkSession.builder.appName("nhan_chuyen_streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    du_lieu = (
        kafka_df.selectExpr("CAST(value AS STRING) AS json_tho")
        .select(F.from_json("json_tho", SCHEMA_TLC).alias("d"))
        .select("d.*")
    )

    truy_van = (
        du_lieu.writeStream
        .foreachBatch(xu_ly_mot_micro_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )
    truy_van.awaitTermination()


if __name__ == "__main__":
    sys.exit(main())
