"""
dags/gt_huan_luyen_lai.py - DAG 2 Giai doan 2: tai huan luyen Mo hinh A dinh ky, so sanh MAE voi
model dang dung, promote (cap nhat Airflow Variable duong_dan_model_a_dang_dung) neu tot hon --
tuong duong gt_huan_luyen_lai cua de cuong (muc 4.9), nhung chu ky tinh theo THANG MO PHONG
(khong phai thang doi thuc -- quyet dinh da xac nhan voi nguoi dung).

Chu ky: he so nen mac dinh 1 gio mo phong = 20 giay that -> 1 THANG mo phong (~720 gio) ~
720 x 20s = 14.400s = 4 GIO THAT. schedule_interval = timedelta(hours=4) (doi theo
HE_SO_NEN_GIAY neu doi toc do nen o phat_lai_chuyen_di.py).

Moc chia train/validation/test TINH DONG theo du lieu hien co trong features.dac_trung_nhu_cau
(doc nhe qua deltalake, khong can Spark) -- vi du lieu mo phong tich luy dan, khong co san du
24 thang nhu gia dinh goc cua de cuong. So ngay validation/test (N_NGAY_VALIDATION,
N_NGAY_TEST) la tham so tam thoi cho giai doan dau khi du lieu con it, CAN XEM LAI khi da
nap du 24 thang that qua Giai doan 1.

QUAN TRONG (dieu tra 2026-10-12, GIU `--deploy-mode client` -- KHONG doi sang cluster cho DAG
nay, khac voi cac DAG/job nang khac da ap dung quyet dinh #5): thu doi sang
`--deploy-mode cluster` de tranh RAM master (giong cac job khac) nhung THAT BAI 100% (2/2 lan
thu, ca khi tang rieng `spark.yarn.am.memory`/`spark.driver.memory`) voi loi giong nhau:
`AM Container ... exitCode: 13 (User application exited with status 1)`.

**Nguyen nhan goc da xac dinh qua log container cuc bo** (khong qua duoc `yarn logs` vi log
aggregation chua cau hinh -- phai doc truc tiep tu
`/opt/hadoop/logs/userlogs/<app_id>/<container_id>/stdout` tren node thuc su chay container):
`ModuleNotFoundError: No module named 'numpy'` khi `huan_luyen.py` import `pyspark.ml`. Kiem tra
sau them cho thay day la **loi cau hinh ha tang cum, khong phai loi code**:
`launch_container.sh` cua YARN dat `export HOME="/home/"` (THIEU ten user, dung ca 3 lan thu tren
ca worker2 va worker3) -- khien Python tim "user site-packages" (nguon cai `numpy` qua
`pip3 install --user`) tai `/home/.local` -- thu muc nay KHONG TON TAI tren bat ky node nao (moi
node co `/home/<user>/.local` rieng: `/home/master`, `/home/worker2`, `/home/worker3` -- KHONG
dung ten "master" xuyen suot ca 3 may nhu tuong, moi may co user OS rieng). Khong co quyen
sudo/root de sua cau hinh YARN (`yarn.nodemanager.*`) hoac tao `/home/.local` chung (thu truc
tiep: `mkdir /home/.local` -> `Permission denied`, `/home/` thuoc root).

**Quyet dinh**: GIU `client` cho DAG nay (khac moi job/DAG nang khac da chuyen sang `cluster`)
vi: (1) da xac nhan chay THANH CONG o client mode voi du lieu/tham so THAT; (2) DAG nay chay
theo lich rieng (moi 4 gio mo phong), KHONG dong thoi voi cac job nang khac trong van hanh binh
thuong -- khac voi su co RAM cu (gt_backfill) xay ra do NHIEU job client-mode chay DONG THOI;
(3) sua dung loi ha tang nay can quyen root (sua yarn-site.xml hoac tao thu muc he thong), ngoai
pham vi co the tu lam trong phien nay -- ghi lai day du de nguoi co quyen admin xem lai sau.
`Functions/huan_luyen.py` van giu ham `_ghi_bao_cao_lich_su()` (ghi ket qua vao
`reports/huan_luyen_lich_su` qua Delta) -- cai thien nay GIU LAI du khong con bat buoc o client
mode, vi doc qua `deltalake` on dinh hon parse log file bat ke deploy-mode.
"""
import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

FEATURES_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/features/dac_trung_nhu_cau"
MODELS_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/models"
REPORTS_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/reports"
BIEN_MODEL_DANG_DUNG = "duong_dan_model_a_dang_dung"
BIEN_MAE_DANG_DUNG = "mae_model_a_dang_dung"
MODEL_MAC_DINH = "hdfs:///lakehouse/bd04_giao_thong/models/mo_hinh_a_rf/20261010_195627"

N_NGAY_VALIDATION = 3  # TAM THOI -- xem lai khi du du lieu 24 thang that (de cuong: 3 THANG)
N_NGAY_TEST = 3        # TAM THOI -- xem lai khi du du lieu 24 thang that (de cuong: 3 THANG)

LENH_MOI_TRUONG = (
    "export HADOOP_HOME=/opt/hadoop; export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop; "
    "export YARN_CONF_DIR=/opt/hadoop/etc/hadoop; export SPARK_HOME=/opt/spark/spark-3.5.9-bin-hadoop3; "
    "export PATH=$SPARK_HOME/bin:$HADOOP_HOME/bin:$PATH; "
)


MUI_GIO_HE_THONG = "Asia/Ho_Chi_Minh"  # mui gio may master -- xem ghi chu trong gt_du_bao_streaming.py


def _tinh_moc_chia(**ngu_canh):
    from deltalake import DeltaTable
    from zoneinfo import ZoneInfo
    dt = DeltaTable(FEATURES_PATH)
    df = dt.to_pandas(columns=["khung_gio"])
    # Loi that da phat hien: deltalake tra ve UTC-aware, phai .astimezone() ve mui gio he
    # thong TRUOC khi bo tzinfo (khong duoc .replace() truc tiep tren UTC) -- xem ghi chu day
    # du trong gt_du_bao_streaming.py._gio_du_lieu_moi_nhat().
    ngay_cuoi = df["khung_gio"].max().to_pydatetime().astimezone(ZoneInfo(MUI_GIO_HE_THONG)).replace(tzinfo=None)

    ngay_bat_dau_test = ngay_cuoi - datetime.timedelta(days=N_NGAY_TEST)
    ngay_bat_dau_validation = ngay_bat_dau_test - datetime.timedelta(days=N_NGAY_VALIDATION)

    ti = ngu_canh["ti"]
    ti.xcom_push(key="ngay_bat_dau_validation", value=ngay_bat_dau_validation.strftime("%Y-%m-%d"))
    ti.xcom_push(key="ngay_bat_dau_test", value=ngay_bat_dau_test.strftime("%Y-%m-%d"))


def _so_sanh_va_promote(**_ngu_canh):
    from deltalake import DeltaTable

    dt = DeltaTable(f"{REPORTS_PATH}/huan_luyen_lich_su")
    df = dt.to_pandas(columns=["phien_ban", "test", "thoi_diem_ghi"])
    dong_moi_nhat = df.sort_values("thoi_diem_ghi").iloc[-1]

    phien_ban_moi = dong_moi_nhat["phien_ban"]
    mae_moi = dong_moi_nhat["test"]["rf_toan_cuc"]["mae"]

    mae_cu = Variable.get(BIEN_MAE_DANG_DUNG, default_var=None)
    mae_cu = float(mae_cu) if mae_cu is not None else float("inf")

    if mae_moi < mae_cu:
        duong_dan_moi = f"{MODELS_PATH}/mo_hinh_a_rf/{phien_ban_moi}"
        Variable.set(BIEN_MODEL_DANG_DUNG, duong_dan_moi)
        Variable.set(BIEN_MAE_DANG_DUNG, str(mae_moi))
        print(f"[PROMOTE] Model moi {phien_ban_moi} (MAE={mae_moi:.4f}) tot hon model cu "
              f"(MAE={mae_cu:.4f}) -> da cap nhat {BIEN_MODEL_DANG_DUNG}")
    else:
        print(f"[GIU NGUYEN] Model moi {phien_ban_moi} (MAE={mae_moi:.4f}) KHONG tot hon "
              f"model dang dung (MAE={mae_cu:.4f}) -> khong promote")


with DAG(
    dag_id="gt_huan_luyen_lai",
    description="Tai huan luyen Mo hinh A dinh ky theo thang MO PHONG (~4 gio that o he so nen 20s=1h)",
    schedule_interval=datetime.timedelta(hours=4),
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    tags=["bd04_giao_thong", "giai_doan_2"],
) as dag:

    tinh_moc_chia = PythonOperator(
        task_id="tinh_moc_chia_train_val_test",
        python_callable=_tinh_moc_chia,
    )

    huan_luyen_lai = BashOperator(
        task_id="huan_luyen_lai",
        bash_command=(
            LENH_MOI_TRUONG
            + "cd /home/master/bd04_giao_thong/Functions && "
            "spark-submit --master yarn --deploy-mode client "
            "--packages io.delta:delta-spark_2.12:3.1.0 "
            "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
            "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
            "huan_luyen.py "
            "--ngay-bat-dau-validation \"{{ ti.xcom_pull(task_ids='tinh_moc_chia_train_val_test', key='ngay_bat_dau_validation') }}\" "
            "--ngay-bat-dau-test \"{{ ti.xcom_pull(task_ids='tinh_moc_chia_train_val_test', key='ngay_bat_dau_test') }}\" "
        ),
        execution_timeout=datetime.timedelta(hours=2),
    )

    so_sanh_va_promote = PythonOperator(
        task_id="so_sanh_va_promote",
        python_callable=_so_sanh_va_promote,
    )

    tinh_moc_chia >> huan_luyen_lai >> so_sanh_va_promote
