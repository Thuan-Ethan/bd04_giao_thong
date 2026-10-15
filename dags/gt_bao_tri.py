"""
dags/gt_bao_tri.py - DAG 5 Giai doan 2 theo de cuong muc 4.9:
    optimize_zorder (OPTIMIZE + ZORDER BY cho bronze/silver -- 2 bang lon nhat)
        -> vacuum (VACUUM RETAIN 168 HOURS cho toan bo bang chinh)
        -> ghi_thong_ke (dung luong tung tang, luu vao reports/bao_tri_thong_ke)

Lich: "0 5 * * 0" (5h sang Chu nhat, dung dac ta de cuong).

Canh bao chi phi (ghi nhan THAT tu phien lam viec 2026-10-12 khi chay thi_nghiem_hieu_nang.py):
OPTIMIZE ZORDER tren silver.chuyen_di day du 24 thang (~10,5GB) ton ~50+ phut va gay ap luc
RAM/swap dang ke tren may master dung chung -- DAG nay NEN chay ngoai gio cao diem (dung voi
lich 5h sang Chu nhat da chon trong de cuong) va KHONG nen chay dong thoi voi cac job Spark nang
khac (huan luyen mo hinh, backfill). timeout rieng cho buoc optimize duoc dat rong (3 gio) de
phan anh dung chi phi thuc te nay.
"""
import datetime
import subprocess

from airflow import DAG
from airflow.decorators import task

LENH_MOI_TRUONG = (
    "export HADOOP_HOME=/opt/hadoop; export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop; "
    "export YARN_CONF_DIR=/opt/hadoop/etc/hadoop; export SPARK_HOME=/opt/spark/spark-3.5.9-bin-hadoop3; "
    "export PATH=$SPARK_HOME/bin:$HADOOP_HOME/bin:$PATH; "
)
CO_DELTA = (
    "--packages io.delta:delta-spark_2.12:3.1.0 "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
)
THU_MUC_FUNCTIONS = "/home/master/bd04_giao_thong/Functions"


def _chay_spark_submit(buoc, timeout_giay):
    lenh = (
        f"{LENH_MOI_TRUONG} cd {THU_MUC_FUNCTIONS} && "
        f"spark-submit --master yarn --deploy-mode cluster {CO_DELTA} "
        f"bao_tri_lakehouse.py --buoc {buoc}"
    )
    ket_qua = subprocess.run(
        ["bash", "-c", lenh], capture_output=True, text=True, timeout=timeout_giay,
    )
    duoi_log = (ket_qua.stdout or "")[-3000:] + "\n" + (ket_qua.stderr or "")[-3000:]
    if ket_qua.returncode != 0:
        raise RuntimeError(f"bao_tri buoc={buoc} that bai (exit={ket_qua.returncode}):\n{duoi_log}")
    return {"buoc": buoc, "trang_thai": "xong"}


with DAG(
    dag_id="gt_bao_tri",
    description="OPTIMIZE + ZORDER + VACUUM + thong ke dung luong lakehouse (de cuong muc 4.9, DAG 5)",
    schedule_interval="0 5 * * 0",
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bd04_giao_thong", "giai_doan_2", "bao_tri"],
) as dag:

    @task(execution_timeout=datetime.timedelta(hours=3))
    def optimize_zorder():
        return _chay_spark_submit("optimize", timeout_giay=3600 * 3)

    @task(execution_timeout=datetime.timedelta(hours=1))
    def vacuum(_ket_qua_optimize):
        return _chay_spark_submit("vacuum", timeout_giay=3600)

    @task
    def ghi_thong_ke(_ket_qua_vacuum):
        return _chay_spark_submit("thong_ke", timeout_giay=600)

    ghi_thong_ke(vacuum(optimize_zorder()))
