"""
Functions/huan_luyen_eta.py - Mo hinh B: du bao thoi gian chuyen (ETA) giua 2 vung, tu
silver.chuyen_di (de cuong muc 4.6).

GBTRegressor. Dau vao: vung_don, vung_tra, gio, thu, quang_duong_mile, nhiet_do, luong_mua,
toc_do_gio (ghep tu ghep_thoi_tiet_spark.py, da viet va kiem thu truoc do). Nhan: thoi_gian_phut.

Baseline: thoi_gian_trung_vi lich su cua cap (vung_don, vung_tra, cung gio-trong-ngay,
thu-trong-tuan) tu gold.ma_tran_od (tinh san tu toan bo lich su -- xem tao_ma_tran_od.py).

Metric: MAE (phut), MAPE. Nguong dat (de cuong muc 4.6): MAE <= 4 phut tren tap test.

Chia du lieu THEO THOI GIAN (khong random split), giong huan_luyen.py (Mo hinh A).

    spark-submit huan_luyen_eta.py \
        --ngay-bat-dau-validation 2025-07-01 --ngay-bat-dau-test 2025-10-01
"""
import argparse
import datetime
import sys

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ghep_thoi_tiet_spark import ghep_thoi_tiet

SILVER_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/silver/chuyen_di"
MA_TRAN_OD_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/gold/ma_tran_od"
MODELS_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/models"

COT_DAC_TRUNG = [
    "vung_don", "vung_tra", "gio", "thu", "quang_duong_mile",
    "nhiet_do", "luong_mua", "toc_do_gio",
]


def _chuan_bi_du_lieu(spark, df_silver):
    df = (
        ghep_thoi_tiet(spark, df_silver, cot_thoi_gian="thoi_diem_don")
        .withColumn("gio", F.hour("thoi_diem_don"))
        .withColumn("thu", F.dayofweek("thoi_diem_don"))
    )
    ma_tran_od = (
        spark.read.format("delta").load(MA_TRAN_OD_PATH)
        .select(
            "vung_don", "vung_tra",
            F.col("khung_gio_trong_ngay").alias("gio"),
            F.col("thu_trong_tuan").alias("thu"),
            F.col("thoi_gian_trung_vi").alias("baseline_trung_vi_od"),
        )
    )
    return df.join(ma_tran_od, on=["vung_don", "vung_tra", "gio", "thu"], how="left")


def _danh_gia(df_du_doan, cot_du_doan, cot_thuc_te="thoi_gian_phut"):
    mae = RegressionEvaluator(labelCol=cot_thuc_te, predictionCol=cot_du_doan, metricName="mae").evaluate(df_du_doan)
    mape = (
        df_du_doan
        .withColumn("_sai_so_tuyet_doi_pt", F.abs(F.col(cot_thuc_te) - F.col(cot_du_doan)) / F.col(cot_thuc_te))
        .agg(F.avg("_sai_so_tuyet_doi_pt")).first()[0]
    )
    return {"mae_phut": mae, "mape": mape}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ngay-bat-dau-validation", required=True, help="YYYY-MM-DD")
    ap.add_argument("--ngay-bat-dau-test", required=True, help="YYYY-MM-DD")
    a = ap.parse_args()

    spark = (
        SparkSession.builder.appName("huan_luyen_mo_hinh_b_eta")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    try:
        df_silver = spark.read.format("delta").load(SILVER_PATH)
        df = _chuan_bi_du_lieu(spark, df_silver)
        df = df.where(F.col("baseline_trung_vi_od").isNotNull()).cache()

        train = df.where(F.col("thoi_diem_don") < a.ngay_bat_dau_validation)
        validation = df.where(
            (F.col("thoi_diem_don") >= a.ngay_bat_dau_validation) & (F.col("thoi_diem_don") < a.ngay_bat_dau_test)
        )
        test = df.where(F.col("thoi_diem_don") >= a.ngay_bat_dau_test)
        so_dong = {"train": train.count(), "validation": validation.count(), "test": test.count()}
        if so_dong["train"] == 0 or so_dong["test"] == 0:
            print(f"[LOI] Tap train hoac test rong: {so_dong}")
            return 1

        rap_vector = VectorAssembler(inputCols=COT_DAC_TRUNG, outputCol="dac_trung", handleInvalid="skip")
        gbt = GBTRegressor(featuresCol="dac_trung", labelCol="thoi_gian_phut", predictionCol="du_doan_eta",
                            maxDepth=6, maxIter=50, seed=42)
        pipeline = Pipeline(stages=[rap_vector, gbt])
        mo_hinh = pipeline.fit(train)

        ket_qua = {"so_dong": so_dong}
        for ten_tap, tap in [("validation", validation), ("test", test)]:
            du_doan = mo_hinh.transform(tap)
            ket_qua[ten_tap] = {
                "gbt": _danh_gia(du_doan, "du_doan_eta"),
                "baseline_trung_vi_od": _danh_gia(du_doan, "baseline_trung_vi_od"),
            }

        print("[KET_QUA] " + str(ket_qua))

        phien_ban = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        mo_hinh.write().overwrite().save(f"{MODELS_PATH}/mo_hinh_b_eta/{phien_ban}")
        print(f"[OK] Da luu model, phien_ban={phien_ban}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
