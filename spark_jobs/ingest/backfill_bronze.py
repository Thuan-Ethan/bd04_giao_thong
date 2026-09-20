# spark_jobs/ingest/backfill_bronze.py
"""
Backfill NHIỀU tháng trong 1 lần spark-submit duy nhất, dùng chung SparkSession
để tránh chi phí khởi động lặp lại (JVM, YARN AM, resolve packages) mỗi tháng.

Usage:
spark-submit --master yarn \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  spark_jobs/ingest/backfill_bronze.py --start-nam 2022 --end-nam 2025
"""

import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

BRONZE_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/bronze/chuyen_di"
RAW_ZONE = "hdfs://hadoop-master:9000/data/raw/tlc/yellow"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bd04_backfill_bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def read_month(spark, nam, thang):
    path = f"{RAW_ZONE}/yellow_tripdata_{nam}-{thang:02d}.parquet"
    df = spark.read.parquet(path)

    def safe_col(name):
        return F.coalesce(F.col(name).cast("string"), F.lit("")) if name in df.columns else F.lit("")

    df = df.withColumn(
        "trip_id",
        F.sha2(F.concat_ws("||", safe_col("VendorID"), safe_col("tpep_pickup_datetime"),
                            safe_col("tpep_dropoff_datetime"), safe_col("PULocationID"),
                            safe_col("DOLocationID")), 256),
    )
    df = df.withColumn("nam", F.lit(nam)).withColumn("thang", F.lit(thang))
    df = df.withColumn("source_file", F.input_file_name())
    df = df.withColumn("ingested_at", F.current_timestamp())
    return df


def merge_into_bronze(spark, df_month, nam, thang):
    if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        bronze_table = DeltaTable.forPath(spark, BRONZE_PATH)
        (bronze_table.alias("target")
            .merge(df_month.alias("source"),
                   "target.trip_id = source.trip_id AND target.nam = source.nam AND target.thang = source.thang")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        (df_month.write.format("delta").partitionBy("nam", "thang")
            .mode("overwrite").option("mergeSchema", "true").save(BRONZE_PATH))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-nam", type=int, required=True)
    parser.add_argument("--end-nam", type=int, required=True)
    args = parser.parse_args()

    spark = build_spark_session()
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    try:
        for nam in range(args.start_nam, args.end_nam + 1):
            for thang in range(1, 13):
                print(f"=== Ingest {nam}-{thang:02d} ===")
                try:
                    df_month = read_month(spark, nam, thang)
                    # Kiểm tra rỗng RẺ hơn count() đầy đủ - chỉ cần biết có >=1 dòng
                    if df_month.take(1) == []:
                        print(f"[WARN] {nam}-{thang:02d} rỗng, bỏ qua.")
                        continue
                    merge_into_bronze(spark, df_month, nam, thang)
                    print(f"[OK] {nam}-{thang:02d} merged.")
                except Exception as e:
                    print(f"[ERROR] {nam}-{thang:02d}: {e}")
                    # Không dừng cả job - ghi lỗi rồi qua tháng tiếp theo
                    continue
    finally:
        spark.stop()


if __name__ == "__main__":
    main()