"""
Functions/phat_lai_chuyen_di.py - Kafka producer mo phong: phat lai chuyen di THAT cua 1 thang
(theo dung thu tu thoi gian thuc) vao topic `chuyen_dang_chay`, nen thoi gian theo de cuong
(mac dinh 1 gio mo phong = 20 giay that, --he-so-nen tuy chinh giay/gio).

Dung Spark CHI de doc + sap xep + loc parquet (tan dung doc phan tan), nhung PHAT tuan tu tu
driver bang toLocalIterator() (khong materialize ca thang vao bo nho driver) de kiem soat
chinh xac nhip do gui theo dung thu tu thoi gian -- day la ly do KHONG dung 1 job Spark song
song thong thuong cho buoc nay.

    spark-submit phat_lai_chuyen_di.py --nam 2023 --thang 2 --he-so-nen 20 \\
        [--gio-bat-dau "2023-02-20 00:00:00"] [--gio-ket-thuc "2023-02-20 02:00:00"]
"""
import argparse
import datetime
import json
import sys
import time

from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_TLC = "hdfs://hadoop-master:9000/data/raw/tlc/yellow"
KAFKA_BOOTSTRAP = "hadoop-master:9092"
TOPIC = "chuyen_dang_chay"


def _ep_json(gia_tri):
    if isinstance(gia_tri, (datetime.datetime, datetime.date)):
        return gia_tri.isoformat()
    return gia_tri


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nam", type=int, required=True)
    ap.add_argument("--thang", type=int, required=True)
    ap.add_argument("--he-so-nen", type=float, default=20.0,
                     help="so giay THAT tuong ung 1 GIO mo phong (mac dinh 20, dung de cuong)")
    ap.add_argument("--gio-bat-dau", default=None, help="YYYY-MM-DD HH:MM:SS, bo qua = tu dau thang")
    ap.add_argument("--gio-ket-thuc", default=None, help="YYYY-MM-DD HH:MM:SS, bo qua = het thang")
    a = ap.parse_args()

    spark = SparkSession.builder.appName(f"phat_lai_{a.nam}_{a.thang:02d}").getOrCreate()
    duong_dan = f"{RAW_TLC}/yellow_tripdata_{a.nam}-{a.thang:02d}.parquet"
    df = spark.read.parquet(duong_dan).where(F.col("tpep_pickup_datetime").isNotNull())

    if a.gio_bat_dau:
        df = df.where(F.col("tpep_pickup_datetime") >= F.lit(a.gio_bat_dau))
    if a.gio_ket_thuc:
        df = df.where(F.col("tpep_pickup_datetime") < F.lit(a.gio_ket_thuc))

    # 1 partition, sap theo dung thu tu thoi gian -- bat buoc de toLocalIterator() tra ve
    # dung thu tu tuan tu can cho viec phat lai dung nhip do.
    df = df.orderBy("tpep_pickup_datetime").coalesce(1)

    gio_dau_tien = df.select(F.min("tpep_pickup_datetime")).first()[0]
    if gio_dau_tien is None:
        print("[LOI] Khong co du lieu trong khoang da chon")
        spark.stop()
        return 1

    he_so = a.he_so_nen / 3600.0  # ty le giay-that / giay-mo-phong

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    moc_bat_dau_that = time.time()
    so_da_gui = 0
    do_lech_mo_phong_cuoi = 0.0

    for hang in df.toLocalIterator():
        d = hang.asDict()
        do_lech_mo_phong_cuoi = (d["tpep_pickup_datetime"] - gio_dau_tien).total_seconds()
        gio_nen_gui_luc = moc_bat_dau_that + do_lech_mo_phong_cuoi * he_so
        cho = gio_nen_gui_luc - time.time()
        if cho > 0:
            time.sleep(cho)

        thong_diep = {k: _ep_json(v) for k, v in d.items()}
        producer.produce(TOPIC, value=json.dumps(thong_diep, default=str).encode("utf-8"))
        producer.poll(0)
        so_da_gui += 1
        if so_da_gui % 2000 == 0:
            producer.flush()
            print(f"[INFO] Da gui {so_da_gui:,} chuyen, do lech mo phong={do_lech_mo_phong_cuoi/3600:.2f} gio")

    producer.flush()
    thoi_gian_thuc_chay = time.time() - moc_bat_dau_that
    print("[KET_QUA] " + str({
        "so_chuyen_da_gui": so_da_gui,
        "tong_gio_mo_phong": round(do_lech_mo_phong_cuoi / 3600, 2),
        "thoi_gian_thuc_chay_giay": round(thoi_gian_thuc_chay, 1),
        "he_so_nen": a.he_so_nen,
    }))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
