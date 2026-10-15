"""
dags/gt_nap_thang_moi.py - DAG 1 Giai doan 2 theo de cuong muc 4.9:
    FileSensor cho file Parquet thang moi tren HDFS
        -> kiem_tra_schema (so voi schema chuan)
        -> [BranchPythonOperator]
             khop      -> nap_bronze
             khac      -> ghi_nhat_ky_thay_doi -> nap_bronze_mergeSchema
        -> bronze_to_silver -> tao_nhu_cau -> tao_ma_tran_od -> tao_dac_trung

Lich: "0 6 5 * *" (6h sang ngay 5 hang thang, dung dac ta de cuong). Thang duoc nap = THANG
TRUOC thang DAG chay (gia dinh TLC cong bo du lieu tre ~1 thang, giong thuc te). Co the ep
nam/thang cu the qua dag_run.conf de demo/kiem thu tren du lieu da co san, vi du:
    airflow dags trigger gt_nap_thang_moi --conf '{"nam": 2025, "thang": 1}'
(thang 2025-01 THAT su co them cot cbd_congestion_fee so voi cac thang 2023-02->2024-12 truoc
do -- dung de demo nhanh re mergeSchema bang du lieu thuc te, khong can gia lap).

Idempotent (checklist nghiem thu de cuong muc 4.11): chay lai DAG cho CUNG 1 thang se MERGE 0
dong moi, vi nap_thang.ghi_bronze() MERGE theo khoa (nam, thang, ma_chuyen) -- da xac nhan dung
qua gt_backfill chay lai thang cu.

QUAN TRONG - vi sao kiem_tra_schema.py chay --deploy-mode client (khac quy uoc cluster-mode cua
cac buoc pipeline nang khac trong DAG nay): day la buoc DUY NHAT can doc lai ket qua ([KET_QUA])
tu stdout cua tien trinh spark-submit local de BranchPythonOperator quyet dinh nhanh re -- voi
--deploy-mode cluster, driver chay tren container YARN tren worker, in ra KHONG xuat hien trong
stdout cua tien trinh local (da xac nhan qua chay that o gt_backfill/gt_du_bao_streaming, phai
doc qua yarn logs hoac du lieu Delta). kiem_tra_schema.py chi doc SCHEMA (khong quet du lieu),
chay vai giay, khong co nguy co chiem RAM master nhu cac job xu ly du lieu nang -- client mode
an toan cho truong hop nay.
"""
import datetime
import json
import logging
import subprocess

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

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
TIMEOUT_MOI_SCRIPT_GIAY = 5400

PY_FILES_THEO_SCRIPT = {
    "tao_dac_trung.py": f"--py-files {THU_MUC_FUNCTIONS}/ghep_thoi_tiet_spark.py ",
}


def _duong_dan_parquet_thang(nam, thang):
    return f"/data/raw/tlc/yellow/yellow_tripdata_{nam}-{thang:02d}.parquet"


def _chay_spark_submit(ten_script, them_tham_so="", deploy_mode="cluster"):
    py_files = PY_FILES_THEO_SCRIPT.get(ten_script, "")
    lenh = (
        f"{LENH_MOI_TRUONG} cd {THU_MUC_FUNCTIONS} && "
        f"spark-submit --master yarn --deploy-mode {deploy_mode} {py_files}{CO_DELTA} {ten_script} {them_tham_so}"
    )
    ket_qua = subprocess.run(
        ["bash", "-c", lenh],
        capture_output=True, text=True, timeout=TIMEOUT_MOI_SCRIPT_GIAY,
    )
    duoi_log = (ket_qua.stdout or "")[-3000:] + "\n" + (ket_qua.stderr or "")[-3000:]
    if ket_qua.returncode != 0:
        raise RuntimeError(f"{ten_script} that bai (exit={ket_qua.returncode}):\n{duoi_log}")
    return ket_qua.stdout or ""


def _thang_can_nap(**ngu_canh):
    conf = ngu_canh["dag_run"].conf or {}
    if "nam" in conf and "thang" in conf:
        return int(conf["nam"]), int(conf["thang"])
    ngay_chay = ngu_canh["logical_date"]
    thang_truoc = ngay_chay.replace(day=1) - datetime.timedelta(days=1)
    return thang_truoc.year, thang_truoc.month


with DAG(
    dag_id="gt_nap_thang_moi",
    description="Nap thang moi hang thang + kiem tra/nhanh re schema evolution (de cuong muc 4.9, DAG 1)",
    schedule_interval="0 6 5 * *",
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bd04_giao_thong", "giai_doan_2", "nap_thang_moi"],
) as dag:

    def _cho_file_thang_moi(**ngu_canh):
        nam, thang = _thang_can_nap(**ngu_canh)
        ngu_canh["ti"].xcom_push(key="nam", value=nam)
        ngu_canh["ti"].xcom_push(key="thang", value=thang)
        duong_dan = _duong_dan_parquet_thang(nam, thang)
        lenh = f"{LENH_MOI_TRUONG} hdfs dfs -test -e {duong_dan}"
        kq = subprocess.run(["bash", "-c", lenh], timeout=30)
        return kq.returncode == 0

    cho_file_thang_moi = PythonSensor(
        task_id="cho_file_thang_moi",
        python_callable=_cho_file_thang_moi,
        poke_interval=300,
        timeout=3600 * 6,
        mode="reschedule",  # khong giu worker slot trong luc cho nhieu gio (de cuong: file co the chua co)
    )

    def _kiem_tra_schema(**ngu_canh):
        ti = ngu_canh["ti"]
        nam = ti.xcom_pull(task_ids="cho_file_thang_moi", key="nam")
        thang = ti.xcom_pull(task_ids="cho_file_thang_moi", key="thang")
        out = _chay_spark_submit(
            "kiem_tra_schema.py", f"--nam {nam} --thang {thang}", deploy_mode="client"
        )
        dong_ket_qua = next(l for l in out.splitlines() if l.startswith("[KET_QUA]"))
        ket_qua = json.loads(dong_ket_qua[len("[KET_QUA] "):])
        ti.xcom_push(key="ket_qua_schema", value=ket_qua)
        logging.info("Ket qua kiem tra schema %s-%02d: %s", nam, thang, ket_qua)

    kiem_tra_schema = PythonOperator(
        task_id="kiem_tra_schema",
        python_callable=_kiem_tra_schema,
    )

    def _nhanh_re_schema(**ngu_canh):
        ti = ngu_canh["ti"]
        ket_qua = ti.xcom_pull(task_ids="kiem_tra_schema", key="ket_qua_schema")
        return "nap_bronze" if ket_qua["khop"] else "ghi_nhat_ky_thay_doi"

    nhanh_re_schema = BranchPythonOperator(
        task_id="nhanh_re_schema",
        python_callable=_nhanh_re_schema,
    )

    @task
    def nap_bronze(**ngu_canh):
        ti = ngu_canh["ti"]
        nam = ti.xcom_pull(task_ids="cho_file_thang_moi", key="nam")
        thang = ti.xcom_pull(task_ids="cho_file_thang_moi", key="thang")
        _chay_spark_submit("nap_thang.py", f"--nam {nam} --thang {thang}")
        return {"nam": nam, "thang": thang}

    @task
    def ghi_nhat_ky_thay_doi(**ngu_canh):
        ti = ngu_canh["ti"]
        ket_qua = ti.xcom_pull(task_ids="kiem_tra_schema", key="ket_qua_schema")
        nam = ti.xcom_pull(task_ids="cho_file_thang_moi", key="nam")
        thang = ti.xcom_pull(task_ids="cho_file_thang_moi", key="thang")
        logging.warning(
            "[THAY DOI SCHEMA] thang=%s-%02d cot_thieu=%s cot_them=%s -- se nap bang mergeSchema",
            nam, thang, ket_qua["cot_thieu_trong_thang_moi"], ket_qua["cot_them_trong_thang_moi"],
        )
        return {"nam": nam, "thang": thang, "thay_doi": ket_qua}

    @task
    def nap_bronze_mergeSchema(**ngu_canh):
        ti = ngu_canh["ti"]
        nam = ti.xcom_pull(task_ids="cho_file_thang_moi", key="nam")
        thang = ti.xcom_pull(task_ids="cho_file_thang_moi", key="thang")
        _chay_spark_submit("nap_thang.py", f"--nam {nam} --thang {thang} --merge-schema")
        return {"nam": nam, "thang": thang}

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def bronze_to_silver(**ngu_canh):
        ti = ngu_canh["ti"]
        nam = ti.xcom_pull(task_ids="cho_file_thang_moi", key="nam")
        thang = ti.xcom_pull(task_ids="cho_file_thang_moi", key="thang")
        _chay_spark_submit("lam_sach_silver.py", f"--nam {nam} --thang {thang}")
        return {"nam": nam, "thang": thang}

    @task
    def tao_nhu_cau(thong_tin: dict):
        _chay_spark_submit(
            "tao_nhu_cau_vung_gio.py", f"--nam {thong_tin['nam']} --thang {thong_tin['thang']}"
        )
        return thong_tin

    @task
    def tao_ma_tran_od(thong_tin: dict):
        # Tinh lai TOAN BO bang (mode overwrite, khong co --nam/--thang) -- giong quy uoc da
        # dung o gt_backfill, vi day la buoc tong hop toan cuc, khong theo tung thang.
        _chay_spark_submit("tao_ma_tran_od.py")
        return thong_tin

    @task
    def va_knn_nhu_cau(thong_tin: dict):
        _chay_spark_submit("va_knn_nhu_cau.py")
        return thong_tin

    @task
    def tao_dac_trung(thong_tin: dict):
        _chay_spark_submit("tao_dac_trung.py")
        return thong_tin

    nhanh_bronze = nap_bronze()
    nhanh_log_thay_doi = ghi_nhat_ky_thay_doi()
    nhanh_bronze_merge = nap_bronze_mergeSchema()

    cho_file_thang_moi >> kiem_tra_schema >> nhanh_re_schema
    nhanh_re_schema >> nhanh_bronze
    nhanh_re_schema >> nhanh_log_thay_doi >> nhanh_bronze_merge

    thong_tin_sau_nap = bronze_to_silver()
    [nhanh_bronze, nhanh_bronze_merge] >> thong_tin_sau_nap
    tao_dac_trung(tao_ma_tran_od(va_knn_nhu_cau(tao_nhu_cau(thong_tin_sau_nap))))
