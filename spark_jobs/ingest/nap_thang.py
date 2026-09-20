# spark_jobs/ingest/nap_thang.py
"""
Ingest ONE MONTH of TLC Yellow Taxi data into the bronze Delta table.
Idempotent: re-running with the same --nam/--thang does not duplicate rows.

Design decisions (see learnings-and-gotchas.md):
- Column names are kept AS-IS from the source Parquet (no forced renaming),
  since TLC's schema legitimately differs across years (airport_fee, 
  cbd_congestion_fee appear/disappear). mergeSchema=true lets Delta absorb
  these differences naturally instead of us hardcoding a fixed schema.

Usage:
spark-submit --master yarn \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  spark_jobs/ingest/nap_thang.py --nam 2024 --thang 1
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
        .appName("bd04_ingest_month")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def read_month(spark: SparkSession, nam: int, thang: int):
    """Read one month's raw Parquet, keeping original TLC column names as-is."""
    month_str = f"{thang:02d}"
    path = f"{RAW_ZONE}/yellow_tripdata_{nam}-{month_str}.parquet"

    df = spark.read.parquet(path)

    # Surrogate key: TLC files have no natural unique ID per trip.
    # SHA-256 hash of identifying fields -> deterministic, safe to re-run.
    # Guard each column with coalesce() since some may be absent depending
    # on the source year's schema.
    def safe_col(name: str):
        return F.coalesce(F.col(name).cast("string"), F.lit("")) if name in df.columns else F.lit("")

    df = df.withColumn(
        "trip_id",
        F.sha2(
            F.concat_ws(
                "||",
                safe_col("VendorID"),
                safe_col("tpep_pickup_datetime"),
                safe_col("tpep_dropoff_datetime"),
                safe_col("PULocationID"),
                safe_col("DOLocationID"),
            ),
            256,
        ),
    )

    # Partition columns (kept as nam/thang - matches the already-established
    # bronze table partition scheme, not part of the code-identifier rename)
    df = df.withColumn("nam", F.lit(nam)).withColumn("thang", F.lit(thang))

    # Audit metadata
    df = df.withColumn("source_file", F.input_file_name())
    df = df.withColumn("ingested_at", F.current_timestamp())

    return df


def merge_into_bronze(spark: SparkSession, df_month, nam: int, thang: int):
    """Idempotent MERGE into the bronze Delta table, with schema evolution enabled."""
    if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        bronze_table = DeltaTable.forPath(spark, BRONZE_PATH)
        (
            bronze_table.alias("target")
            .merge(
                df_month.alias("source"),
                "target.trip_id = source.trip_id AND target.nam = source.nam AND target.thang = source.thang",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[OK] Merged {nam}-{thang:02d} into bronze.chuyen_di")
    else:
        # First run ever: table doesn't exist yet, create it
        (
            df_month.write.format("delta")
            .partitionBy("nam", "thang")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(BRONZE_PATH)
        )
        print(f"[OK] Created bronze.chuyen_di, loaded first month {nam}-{thang:02d}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nam", type=int, required=True)
    parser.add_argument("--thang", type=int, required=True)
    args = parser.parse_args()

    spark = build_spark_session()
    # mergeSchema at session level so it applies to both write() and merge()
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    try:
        df_month = read_month(spark, args.nam, args.thang)
        row_count = df_month.count()
        print(f"[INFO] Read {row_count:,} rows for {args.nam}-{args.thang:02d}")

        if row_count == 0:
            print(f"[WARN] {args.nam}-{args.thang:02d} has no data, skipping.")
            sys.exit(0)

        merge_into_bronze(spark, df_month, args.nam, args.thang)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()