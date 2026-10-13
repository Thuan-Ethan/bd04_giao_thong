"""
Functions/huan_luyen.py - Mo hinh A: du bao so_chuyen theo vung, tu features.dac_trung_nhu_cau
(de cuong muc 4.6).

Thuat toan chinh: RandomForestRegressor (MLlib). So sanh voi GBTRegressor va 2 baseline:
    baseline_tuan_truoc = lag_168h (gia tri cung gio tuan truoc)
    baseline_tb_4_tuan   = trung binh so_chuyen cung (vung, gio, thu) trong 4 tuan gan nhat
                           (trung binh cua lag_168h, lag_336h, lag_504h, lag_672h)

Chia du lieu THEO THOI GIAN (khong random split -- de cuong canh bao ro rang se bi loi):
    train      : [ngay_bat_dau_du_lieu, ngay_bat_dau_validation)
    validation : [ngay_bat_dau_validation, ngay_bat_dau_test)
    test       : [ngay_bat_dau_test, ngay_ket_thuc_du_lieu]

Metric: MAE, RMSE TOAN CUC; QUAN TRONG HON theo de cuong: MAE/RMSE rieng theo 3 nhom vung
(nhu cau cao/trung binh/thap) -- chia tertile theo trung binh so_chuyen MOI VUNG tren tap
TRAIN (khong dung test de tranh leak nhom).

Nguong dat (de cuong): MAE tot hon baseline_tuan_truoc >=25% o nhom nhu cau cao, >=10% o
nhom nhu cau thap.

Bien muc tieu co the thu log1p(so_chuyen) (--dung-log1p) -- de cuong yeu cau thu ca 2 va
bao cao, khong bat buoc chon 1.

    spark-submit huan_luyen.py \
        --ngay-bat-dau-validation 2025-07-01 --ngay-bat-dau-test 2025-10-01 \
        [--dung-log1p]
"""
import argparse
import sys

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, SQLTransformer, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

FEATURES_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/features/dac_trung_nhu_cau"
MODELS_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/models"
REPORTS_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/reports"

COT_SO = [
    "lag_1h", "lag_2h", "lag_24h", "lag_168h",
    "trung_binh_truot_3h", "do_lech_chuan_truot_3h",
    "trung_binh_truot_24h", "do_lech_chuan_truot_24h",
    "trung_binh_truot_168h", "do_lech_chuan_truot_168h",
    "nhu_cau_vung_ke_gio_truoc",
    "gio", "thu", "thang",
    "sin_gio", "cos_gio", "sin_thu", "cos_thu",
    "nhiet_do", "luong_mua", "toc_do_gio",
]
COT_BOOL = ["la_cuoi_tuan", "la_ngay_le", "la_gio_cao_diem"]
COT_PHAN_LOAI = ["loai_vung"]


def _them_baseline_4_tuan(df):
    cua_so = Window.partitionBy("vung").orderBy("khung_gio")
    df = df.withColumn("lag_336h", F.lag("so_chuyen", 336).over(cua_so))
    df = df.withColumn("lag_504h", F.lag("so_chuyen", 504).over(cua_so))
    df = df.withColumn("lag_672h", F.lag("so_chuyen", 672).over(cua_so))
    return df.withColumn(
        "baseline_tb_4_tuan",
        (F.col("lag_168h") + F.col("lag_336h") + F.col("lag_504h") + F.col("lag_672h")) / 4.0,
    ).withColumn("baseline_tuan_truoc", F.col("lag_168h").cast("double"))


def _chia_nhom_vung(df_train):
    """Chia 263 vung thanh 3 nhom (thap/trung_binh/cao) theo tertile trung binh so_chuyen
    MOI VUNG tren tap TRAIN. Tra ve DataFrame (vung, nhom_vung)."""
    tb_theo_vung = df_train.groupBy("vung").agg(F.avg("so_chuyen").alias("tb_so_chuyen"))
    q1, q2 = tb_theo_vung.approxQuantile("tb_so_chuyen", [1 / 3, 2 / 3], 0.01)
    return tb_theo_vung.withColumn(
        "nhom_vung",
        F.when(F.col("tb_so_chuyen") <= q1, F.lit("thap"))
        .when(F.col("tb_so_chuyen") <= q2, F.lit("trung_binh"))
        .otherwise(F.lit("cao")),
    ).select("vung", "nhom_vung")


def _danh_gia(df_du_doan, cot_du_doan, cot_thuc_te="so_chuyen"):
    mae = RegressionEvaluator(labelCol=cot_thuc_te, predictionCol=cot_du_doan, metricName="mae").evaluate(df_du_doan)
    rmse = RegressionEvaluator(labelCol=cot_thuc_te, predictionCol=cot_du_doan, metricName="rmse").evaluate(df_du_doan)
    return {"mae": mae, "rmse": rmse}


def _tinh_va_ghi_residual(df_du_doan, cot_du_doan, phien_ban):
    """Bieu do residual theo gio-trong-ngay va theo vung cho Mo hinh A -- BAT BUOC phan tich
    theo de cuong muc 4.6, chi ra thoi diem/khu vuc mo hinh yeu nhat. Tinh tren tap TEST, ghi
    2 bang nho (gom theo gio / theo vung) ra HDFS de ve bieu do sau (khong can giu toan bo
    du lieu chi tiet, chi can trung binh/do lech chuan/so luong moi nhom)."""
    du_lieu = df_du_doan.withColumn("residual", F.col("so_chuyen") - F.col(cot_du_doan))

    theo_gio = (
        du_lieu.groupBy("gio")
        .agg(
            F.avg("residual").alias("residual_trung_binh"),
            F.stddev("residual").alias("residual_do_lech_chuan"),
            F.count("residual").alias("so_dong"),
        )
        .orderBy("gio")
        .withColumn("phien_ban_mo_hinh", F.lit(phien_ban))
    )
    theo_vung = (
        du_lieu.groupBy("vung")
        .agg(
            F.avg("residual").alias("residual_trung_binh"),
            F.stddev("residual").alias("residual_do_lech_chuan"),
            F.count("residual").alias("so_dong"),
        )
        .orderBy("vung")
        .withColumn("phien_ban_mo_hinh", F.lit(phien_ban))
    )
    theo_gio.write.format("delta").mode("overwrite").save(f"{REPORTS_PATH}/residual_theo_gio")
    theo_vung.write.format("delta").mode("overwrite").save(f"{REPORTS_PATH}/residual_theo_vung")


def _ghi_bao_cao_lich_su(spark, ket_qua):
    """Ghi ket_qua + phien_ban vao reports/huan_luyen_lich_su (Delta, append) -- cho phep
    DAG gt_huan_luyen_lai doc lai ket qua qua `deltalake` (khong qua Spark) de so sanh/promote,
    THAY vi parse stdout cua tien trinh spark-submit local. Can thiet vi gt_huan_luyen_lai.py
    se doi sang --deploy-mode cluster (tranh RAM master) -- voi cluster mode, stdout/print()
    chay tren container YARN tren worker, KHONG xuat hien trong log local nua (da xac nhan
    tu truoc o gt_backfill/gt_du_bao_streaming)."""
    import json

    rdd = spark.sparkContext.parallelize([json.dumps(ket_qua, default=str)])
    df_bao_cao = (
        spark.read.json(rdd)
        .withColumn("thoi_diem_ghi", F.current_timestamp())
    )
    df_bao_cao.write.format("delta").mode("append").save(f"{REPORTS_PATH}/huan_luyen_lich_su")


def _danh_gia_theo_nhom(df_du_doan, cot_du_doan, nhom_vung):
    df = df_du_doan.join(nhom_vung, on="vung", how="left")
    ket_qua = {}
    for nhom in ["thap", "trung_binh", "cao"]:
        con = df.where(F.col("nhom_vung") == nhom)
        if con.take(1):
            ket_qua[nhom] = _danh_gia(con, cot_du_doan)
    return ket_qua


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ngay-bat-dau-validation", required=True, help="YYYY-MM-DD")
    ap.add_argument("--ngay-bat-dau-test", required=True, help="YYYY-MM-DD")
    ap.add_argument("--dung-log1p", action="store_true")
    a = ap.parse_args()

    spark = (
        SparkSession.builder.appName("huan_luyen_mo_hinh_a")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    try:
        df = spark.read.format("delta").load(FEATURES_PATH)
        df = _them_baseline_4_tuan(df)
        # Can du lieu cho ca du bao va 2 baseline -> loc dong co du lich su (672h = 28 ngay)
        df = df.where(F.col("lag_672h").isNotNull()).cache()

        train = df.where(F.col("khung_gio") < a.ngay_bat_dau_validation)
        validation = df.where(
            (F.col("khung_gio") >= a.ngay_bat_dau_validation) & (F.col("khung_gio") < a.ngay_bat_dau_test)
        )
        test = df.where(F.col("khung_gio") >= a.ngay_bat_dau_test)
        so_dong = {"train": train.count(), "validation": validation.count(), "test": test.count()}
        if so_dong["train"] == 0 or so_dong["test"] == 0:
            print(f"[LOI] Tap train hoac test rong: {so_dong}")
            return 1

        nhom_vung = _chia_nhom_vung(train).cache()

        cot_nhan = "log1p_so_chuyen" if a.dung_log1p else "so_chuyen"
        if a.dung_log1p:
            train = train.withColumn(cot_nhan, F.log1p("so_chuyen"))
            validation = validation.withColumn(cot_nhan, F.log1p("so_chuyen"))
            test = test.withColumn(cot_nhan, F.log1p("so_chuyen"))

        # Ep bool -> double NGAY TRONG pipeline (khong phai withColumn ben ngoai nhu ban cu) --
        # de model luu lai TU CHUA du buoc tien xu ly, khong can nguoi goi transform() phai
        # tu lam lai dung buoc nay (tranh lech huan luyen/suy luan -- phat hien khi viet
        # xu_ly_va_du_bao.py: IllegalArgumentException "_la_cuoi_tuan_so does not exist").
        cau_lenh_ep_bool = ", ".join(f"CAST({c} AS DOUBLE) AS _{c}_so" for c in COT_BOOL)
        ep_bool = SQLTransformer(statement=f"SELECT *, {cau_lenh_ep_bool} FROM __THIS__")

        chi_so = [StringIndexer(inputCol=c, outputCol=f"_{c}_idx", handleInvalid="keep") for c in COT_PHAN_LOAI]
        one_hot = [OneHotEncoder(inputCol=f"_{c}_idx", outputCol=f"_{c}_oh") for c in COT_PHAN_LOAI]
        cot_dac_trung_day_du = COT_SO + [f"_{c}_oh" for c in COT_PHAN_LOAI] + [
            f"_{c}_so" for c in COT_BOOL
        ]

        rap_vector = VectorAssembler(inputCols=cot_dac_trung_day_du, outputCol="dac_trung", handleInvalid="skip")

        rf = RandomForestRegressor(featuresCol="dac_trung", labelCol=cot_nhan, predictionCol="du_doan_rf",
                                    maxDepth=8, numTrees=100, seed=42)
        gbt = GBTRegressor(featuresCol="dac_trung", labelCol=cot_nhan, predictionCol="du_doan_gbt",
                            maxDepth=6, maxIter=50, seed=42)

        pipeline_rf = Pipeline(stages=[ep_bool] + chi_so + one_hot + [rap_vector, rf])
        pipeline_gbt = Pipeline(stages=[ep_bool] + chi_so + one_hot + [rap_vector, gbt])

        mo_hinh_rf = pipeline_rf.fit(train)
        mo_hinh_gbt = pipeline_gbt.fit(train)

        phien_ban = __import__("datetime").datetime.now().strftime("%Y%m%d_%H%M%S")

        ket_qua = {"so_dong": so_dong, "dung_log1p": a.dung_log1p, "phien_ban": phien_ban}
        for ten_tap, tap in [("validation", validation), ("test", test)]:
            # Transform doc lap tu `tap` goc cho tung pipeline -- KHONG chain output cua
            # pipeline nay vao pipeline kia (gay trung ten cot trung gian nhu _loai_vung_idx).
            du_doan_rf = mo_hinh_rf.transform(tap)
            du_doan_gbt = mo_hinh_gbt.transform(tap)

            if a.dung_log1p:
                du_doan_rf = du_doan_rf.withColumn("du_doan_rf", F.expm1("du_doan_rf"))
                du_doan_gbt = du_doan_gbt.withColumn("du_doan_gbt", F.expm1("du_doan_gbt"))

            ket_qua[ten_tap] = {
                "rf_toan_cuc": _danh_gia(du_doan_rf, "du_doan_rf"),
                "gbt_toan_cuc": _danh_gia(du_doan_gbt, "du_doan_gbt"),
                "baseline_tuan_truoc_toan_cuc": _danh_gia(tap, "baseline_tuan_truoc"),
                "baseline_tb_4_tuan_toan_cuc": _danh_gia(tap, "baseline_tb_4_tuan"),
                "rf_theo_nhom": _danh_gia_theo_nhom(du_doan_rf, "du_doan_rf", nhom_vung),
                "baseline_tuan_truoc_theo_nhom": _danh_gia_theo_nhom(tap, "baseline_tuan_truoc", nhom_vung),
            }

            if ten_tap == "test":
                # Residual chi tinh tren tap TEST, dung du_doan cua RF (mo hinh chinh theo de
                # cuong) -- dung gia tri so_chuyen GOC (khong phai log1p) de don vi de hieu.
                _tinh_va_ghi_residual(du_doan_rf, "du_doan_rf", phien_ban)

        print("[KET_QUA] " + str(ket_qua))
        _ghi_bao_cao_lich_su(spark, ket_qua)

        mo_hinh_rf.write().overwrite().save(f"{MODELS_PATH}/mo_hinh_a_rf/{phien_ban}")
        mo_hinh_gbt.write().overwrite().save(f"{MODELS_PATH}/mo_hinh_a_gbt/{phien_ban}")
        print(f"[OK] Da luu model, phien_ban={phien_ban}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
