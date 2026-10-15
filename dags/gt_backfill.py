"""
dags/gt_backfill.py - DAG 2 Giai doan 2 theo de cuong muc 4.9: nap lai (backfill) nhieu thang
lien tuc, trigger THU CONG (khong lich co dinh), dung Dynamic Task Mapping (.expand()) de
sinh song song 1 task cho moi thang, gioi han max_active_tis_per_dag=2 de tranh qua tai cum
(chi 3 node, con phai danh tai nguyen cho webapp/streaming khac).

QUAN TRONG - deploy-mode cluster (phat hien qua chay that, khong phai toi uu truoc): lan chay
backfill dau tien dung `--deploy-mode client` (mac dinh) khien MOI driver JVM (2g RAM/driver)
chay dong tren MAY MASTER bat ke du lieu xu ly o dau -- voi 2 thang chay song song + streaming
demo (nhan_chuyen_streaming + gt_du_bao_streaming moi 20s) cung dang chay tren master, RAM master
can kiet (7,0/7,4GB dung, swap day 100%), khien Airflow danh dau hang loat task la "zombie" (qua
480s khong gui duoc heartbeat, nguong 300s) va FAIL dong loat du script khong co loi gi. Trong khi
do worker2/worker3 gan nhu ranh (~4GB RAM du moi may). Sua: dung `--deploy-mode cluster` -- YARN
tu dat driver vao node con trong (thuong la worker2/worker3), giam tai that cho master. Danh doi:
ket qua in ra ([KET_QUA]) nam trong log cua YARN container tren worker, khong con hien truc tiep
trong duoi_log cua _chay_spark_submit() nhu truoc -- xac nhan ket qua qua du lieu Delta that (da
la cach uu tien dung xuyen suot du an) hoac `yarn logs -applicationId <id>` khi can debug loi.

QUAN TRONG - vi sao chia 2 giai doan (khong the chay het ca 6 buoc pipeline song song theo
thang): kiem tra truc tiep code cac buoc hien co cho thay chi 3 buoc dau la THEO THANG that su
(dung --nam/--thang + replaceWhere/MERGE scope dung partition, an toan chay song song):
    nap_thang.py -> lam_sach_silver.py -> tao_nhu_cau_vung_gio.py
3 buoc sau la TINH LAI TOAN BO BANG moi lan chay (mode("overwrite") toan bang, KHONG co
tham so --nam/--thang, KHONG dung replaceWhere) -- neu goi song song theo thang se GHI DE
CHONG CHEO nhau, mat du lieu:
    tao_ma_tran_od.py -> va_knn_nhu_cau.py -> tao_dac_trung.py
=> Thiet ke: Giai doan A (xu_ly_mot_thang, .expand() theo danh sach thang, song song toi da 2)
chi chay 3 buoc dau; Giai doan B (tong_hop_sau_khi_nap, 1 task duy nhat, Airflow tu dong cho
TOAN BO cac task mapped o Giai doan A xong moi chay) chay 3 buoc sau, dung 1 lan cho ca khoang
backfill.

Trigger (vi du backfill 24 thang 2024-01 -> 2025-12):
    airflow dags trigger gt_backfill --conf \
        '{"nam_bat_dau": 2024, "thang_bat_dau": 1, "nam_ket_thuc": 2025, "thang_ket_thuc": 12}'
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
TIMEOUT_MOI_SCRIPT_GIAY = 5400  # 90 phut/script, du du cho 1 thang du lieu lon nhat

# Script nao import sibling module cuc bo thi phai dinh kem qua --py-files de YARN gui theo
# driver container tren worker (deploy-mode cluster khong tu thay thu muc Functions/ nhu client
# mode chay tai cho) -- hien chi tao_dac_trung.py import ghep_thoi_tiet_spark.py.
PY_FILES_THEO_SCRIPT = {
    "tao_dac_trung.py": f"--py-files {THU_MUC_FUNCTIONS}/ghep_thoi_tiet_spark.py ",
}


def _chay_spark_submit(ten_script, them_tham_so=""):
    py_files = PY_FILES_THEO_SCRIPT.get(ten_script, "")
    lenh = (
        f"{LENH_MOI_TRUONG} cd {THU_MUC_FUNCTIONS} && "
        f"spark-submit --master yarn --deploy-mode cluster {py_files}{CO_DELTA} {ten_script} {them_tham_so}"
    )
    ket_qua = subprocess.run(
        ["bash", "-c", lenh],
        capture_output=True, text=True, timeout=TIMEOUT_MOI_SCRIPT_GIAY,
    )
    duoi_log = (ket_qua.stdout or "")[-3000:] + "\n" + (ket_qua.stderr or "")[-3000:]
    if ket_qua.returncode != 0:
        raise RuntimeError(f"{ten_script} that bai (exit={ket_qua.returncode}):\n{duoi_log}")
    return duoi_log


def _danh_sach_thang(nam_bd, thang_bd, nam_kt, thang_kt):
    ds = []
    y, m = nam_bd, thang_bd
    while (y, m) <= (nam_kt, thang_kt):
        ds.append({"nam": y, "thang": m})
        m += 1
        if m > 12:
            m = 1
            y += 1
    return ds


with DAG(
    dag_id="gt_backfill",
    description="Backfill nhieu thang lien tuc qua dynamic task mapping (de cuong muc 4.9, DAG 2)",
    schedule_interval=None,  # thu cong, dung Variable/dag_run.conf khi trigger
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bd04_giao_thong", "giai_doan_2", "backfill"],
) as dag:

    @task
    def lay_danh_sach_thang(**ngu_canh):
        conf = ngu_canh["dag_run"].conf or {}
        bat_buoc = ["nam_bat_dau", "thang_bat_dau", "nam_ket_thuc", "thang_ket_thuc"]
        thieu = [k for k in bat_buoc if k not in conf]
        if thieu:
            raise ValueError(
                f"Thieu tham so khi trigger: {thieu}. Vi du: "
                '{"nam_bat_dau":2024,"thang_bat_dau":1,"nam_ket_thuc":2025,"thang_ket_thuc":12}'
            )
        return _danh_sach_thang(
            conf["nam_bat_dau"], conf["thang_bat_dau"], conf["nam_ket_thuc"], conf["thang_ket_thuc"]
        )

    # retries=2 (phat hien qua chay that): 1 lan that bai do loi mang thoang qua giua cac node
    # (SocketTimeoutException toi DataNode, 65s, tu phuc hoi ngay sau do -- HDFS report xac nhan
    # cum khong co block loi) lam ca 24 thang backfill bi huy vi 1 thang duy nhat. Cho phep tu
    # dong thu lai thay vi mat toan bo cong suc cua cac thang khac da/dang xu ly thanh cong.
    @task(max_active_tis_per_dag=2, retries=2, retry_delay=datetime.timedelta(minutes=2))
    def xu_ly_mot_thang(thang_info: dict):
        nam, thang = thang_info["nam"], thang_info["thang"]
        tham_so = f"--nam {nam} --thang {thang}"
        _chay_spark_submit("nap_thang.py", tham_so)
        _chay_spark_submit("lam_sach_silver.py", tham_so)
        _chay_spark_submit("tao_nhu_cau_vung_gio.py", tham_so)
        return {"nam": nam, "thang": thang, "trang_thai": "xong"}

    @task
    def tong_hop_sau_khi_nap(ket_qua_tung_thang: list):
        # QUAN TRONG (phat hien qua chay that): ket_qua_tung_thang la LazyXComAccess (Airflow
        # dynamic task mapping), KHONG serialize JSON truc tiep duoc -- phai ep ve list/dict
        # thuan truoc khi dua vao gia tri tra ve (Airflow luu return value vao XCom).
        chi_tiet = [dict(r) for r in ket_qua_tung_thang]
        _chay_spark_submit("tao_ma_tran_od.py")
        _chay_spark_submit("va_knn_nhu_cau.py")
        _chay_spark_submit("tao_dac_trung.py")
        return {
            "so_thang_da_nap": len(chi_tiet),
            "chi_tiet": chi_tiet,
        }

    danh_sach = lay_danh_sach_thang()
    ket_qua_tung_thang = xu_ly_mot_thang.expand(thang_info=danh_sach)
    tong_hop_sau_khi_nap(ket_qua_tung_thang)
