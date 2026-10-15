"""
dags/gt_du_bao_streaming.py - DAG 1 Giai doan 2: cho du 1 GIO MO PHONG du lieu moi tu streaming
(doc gold.trang_thai_streaming bang deltalake -- nhe, khong can spark-submit moi lan kiem tra),
roi goi lai Functions/xu_ly_va_du_bao.py (--bo-qua-nap-bronze, vi du lieu da nap qua streaming
roi) de lam sach + gold + feature + du bao cho khung gio vua hoan tat.

Trigger theo DU LIEU (PythonSensor), KHONG theo lich co dinh 30 phut that cua de cuong goc --
thiet ke da thong nhat voi nguoi dung, vi muc dich demo/mo phong can trigger dung nhip du lieu
streaming (mac dinh 20s that = 1 gio mo phong).

Luu khung gio da xu ly gan nhat trong Airflow Variable `khung_gio_du_bao_gan_nhat` de biet
khung gio TIEP THEO can du bao khi DAG chay lai.
"""
import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

TRANG_THAI_STREAMING_PATH = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong/gold/trang_thai_streaming"
BIEN_KHUNG_GIO_GAN_NHAT = "khung_gio_du_bao_gan_nhat"
BIEN_MODEL_DANG_DUNG = "duong_dan_model_a_dang_dung"
MODEL_MAC_DINH = "hdfs:///lakehouse/bd04_giao_thong/models/mo_hinh_a_rf/20261010_195627"

LENH_MOI_TRUONG = (
    "export HADOOP_HOME=/opt/hadoop; export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop; "
    "export YARN_CONF_DIR=/opt/hadoop/etc/hadoop; export SPARK_HOME=/opt/spark/spark-3.5.9-bin-hadoop3; "
    "export PATH=$SPARK_HOME/bin:$HADOOP_HOME/bin:$PATH; "
)


MUI_GIO_HE_THONG = "Asia/Ho_Chi_Minh"  # mui gio may master -- xem ghi chu ham duoi


def _gio_du_lieu_moi_nhat():
    """Doc gold.trang_thai_streaming truc tiep qua deltalake (khong qua Spark/YARN) -- an toan
    goi thuong xuyen trong Sensor ma khong chiem tai nguyen cum.

    LUU Y MUI GIO (loi that da phat hien va sua): deltalake/pyarrow doc timestamp tra ve dang
    UTC-aware (dung, vi Parquet luu UTC-instant that). Nhung moi gia tri thoi gian khac trong
    toan bo pipeline (bronze/silver/gold/features) deu la "naive" theo MUI GIO HE THONG cua may
    master (Asia/Ho_Chi_Minh, +07:00) -- vi Spark session mac dinh dung tz he thong khi doc/ghi
    naive datetime (spark.sql.session.timeZone khong duoc set rieng, lay theo JVM/OS). Phai
    CHUYEN VE dung mui gio nay truoc khi bo tzinfo bang .astimezone(), KHONG duoc .replace()
    truc tiep tren gia tri UTC -- neu khong se lech 7 tieng (da gay loi that: DAG xu ly nham
    gio cu thang 2 thay vi gio moi thang 3 vua nap qua streaming)."""
    from deltalake import DeltaTable
    from zoneinfo import ZoneInfo
    try:
        dt = DeltaTable(TRANG_THAI_STREAMING_PATH)
    except Exception:
        return None
    df = dt.to_pandas()
    if df.empty:
        return None
    gio_utc = df.iloc[0]["gio_du_lieu_moi_nhat"]
    return gio_utc.to_pydatetime().astimezone(ZoneInfo(MUI_GIO_HE_THONG)).replace(tzinfo=None)


def _gio_tiep_theo_can_xu_ly():
    """Gio CAN xu ly tiep theo = gio da xu ly gan nhat + 1h (xu ly TUAN TU, khong nhay coc).
    Neu chua tung xu ly gio nao, lay gio moi nhat hien co lam diem bat dau (khong lui ve qua
    xa lich su). QUAN TRONG: neu chi lay "gio moi nhat" moi lan (thay vi +1h tuan tu), khi
    pipeline xu ly cham hon toc do nap du lieu se BI NHAY COC bo sot cac gio o giua -- da phat
    hien qua chay that (8 gio bi bo sot khi thu nghiem 10 gio mo phong lien tuc)."""
    gio_da_xu_ly = Variable.get(BIEN_KHUNG_GIO_GAN_NHAT, default_var=None)
    if gio_da_xu_ly is None:
        return _gio_du_lieu_moi_nhat()
    return datetime.datetime.fromisoformat(gio_da_xu_ly) + datetime.timedelta(hours=1)


def _kiem_tra_du_1_gio(**_ngu_canh):
    """Sensor callable: True khi gio TIEP THEO can xu ly da co du lieu that (khong nhay coc)."""
    gio_moi_nhat = _gio_du_lieu_moi_nhat()
    if gio_moi_nhat is None:
        return False
    gio_tiep_theo = _gio_tiep_theo_can_xu_ly()
    return gio_moi_nhat >= gio_tiep_theo


def _lay_khung_gio_can_du_bao(**_ngu_canh):
    """Tra ve dung khung gio TIEP THEO can xu ly (tuan tu), khong phai gio moi nhat."""
    return _gio_tiep_theo_can_xu_ly().strftime("%Y-%m-%d %H:%M:%S")


def _cap_nhat_bien_sau_khi_xong(**ngu_canh):
    khung_gio = ngu_canh["ti"].xcom_pull(task_ids="lay_khung_gio")
    Variable.set(BIEN_KHUNG_GIO_GAN_NHAT, khung_gio)


# DAG tu chay LAP LAI theo nhip du lieu mo phong (khop he_so_nen mac dinh cua
# phat_lai_chuyen_di.py: 1 gio mo phong = 20s that). Moi lan chay, Sensor chi cho ngan (vai
# lan poke) -- neu chua co gio moi thi soft_fail (skip sach, khong bao loi), lan chay ke tiep
# (20s sau) se thu lai. Day la cach "trigger theo du lieu" ma van de Airflow scheduler tu lap
# lai, khong can TriggerDagRunOperator tu goi lai chinh no.
with DAG(
    dag_id="gt_du_bao_streaming",
    description="Du bao nhu cau 1 gio toi, tu kiem tra lap lai theo nhip du lieu streaming",
    schedule_interval=datetime.timedelta(seconds=20),
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # khong de nhieu lan chay chong lan (tranh xu ly trung khung gio)
    tags=["bd04_giao_thong", "giai_doan_2"],
) as dag:

    cho_du_1_gio = PythonSensor(
        task_id="cho_du_1_gio_mo_phong",
        python_callable=_kiem_tra_du_1_gio,
        poke_interval=5,
        timeout=15,
        mode="reschedule",  # nha slot worker trong luc cho, khong giu tien trinh
        soft_fail=True,     # chua co gio moi -> skip sach (khong phai that bai that)
    )

    lay_khung_gio = PythonOperator(
        task_id="lay_khung_gio",
        python_callable=_lay_khung_gio_can_du_bao,
    )

    # QUAN TRONG (phat hien qua chay that khi backfill gt_backfill.py): --deploy-mode client
    # (mac dinh) khien MOI driver JVM (2GB RAM) chay dong tren MAY MASTER, bat ke du lieu xu ly
    # o dau. Voi DAG nay tu kich hoat moi 20s, cac driver chay dong lien tuc tung gay can kiet
    # RAM master (swap day 100%, Airflow danh "zombie" hang loat task). Sua: --deploy-mode
    # cluster de YARN tu dat driver vao node con trong (worker2/3). xu_ly_va_du_bao.py import
    # sibling module cuc bo (lam_sach_silver.py, tao_dac_trung.py, va giant tiep
    # ghep_thoi_tiet_spark.py qua tao_dac_trung) -- phai dinh kem ca 3 qua --py-files de driver
    # tren node khac doc duoc, khac voi client mode chay tai cho tu doc duoc thu muc Functions/.
    du_bao = BashOperator(
        task_id="xu_ly_va_du_bao",
        bash_command=(
            LENH_MOI_TRUONG
            + "cd /home/master/bd04_giao_thong/Functions && "
            "spark-submit --master yarn --deploy-mode cluster "
            "--py-files lam_sach_silver.py,tao_dac_trung.py,ghep_thoi_tiet_spark.py "
            "--packages io.delta:delta-spark_2.12:3.1.0 "
            "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
            "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
            "xu_ly_va_du_bao.py "
            "--khung-gio \"{{ ti.xcom_pull(task_ids='lay_khung_gio') }}\" "
            "--horizon 1 --bo-qua-nap-bronze "
            f"--duong-dan-model \"{{{{ var.value.get('{BIEN_MODEL_DANG_DUNG}', '{MODEL_MAC_DINH}') }}}}\""
        ),
        retries=1,
        retry_delay=datetime.timedelta(seconds=10),
    )

    cap_nhat_bien = PythonOperator(
        task_id="cap_nhat_khung_gio_da_xu_ly",
        python_callable=_cap_nhat_bien_sau_khi_xong,
    )

    cho_du_1_gio >> lay_khung_gio >> du_bao >> cap_nhat_bien
