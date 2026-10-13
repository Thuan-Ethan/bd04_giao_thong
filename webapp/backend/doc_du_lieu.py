"""
doc_du_lieu.py - Lop doc/ghi du lieu cho webapp, dung chung cho main.py.

Nguyen tac (theo docs/06-thiet-ke-ung-dung-web.md muc 3.1): doc Delta table truc tiep qua thu
vien `deltalake` (delta-rs), KHONG qua Spark/YARN -- tranh khoi dong 1 Spark job chi de tra JSON
cho 1 request web (cung cach lam da dung cho Airflow Sensor o gt_du_bao_streaming.py).

LUU Y MUI GIO (quy uoc bat buoc toan du an, xem docs/05): deltalake doc timestamp tra ve UTC-aware
pandas Timestamp. Moi noi trong file nay deu chuyen ve naive gio he thong (Asia/Ho_Chi_Minh) truoc
khi so sanh/tra ve frontend, qua ham `_ve_gio_he_thong_series`/`_ve_gio_he_thong_1`.
"""
import datetime
import json
import os
import subprocess
import uuid
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

MUI_GIO = "Asia/Ho_Chi_Minh"
LAKEHOUSE = "hdfs://hadoop-master:9000/lakehouse/bd04_giao_thong"
PATH_NHU_CAU = f"{LAKEHOUSE}/gold/nhu_cau_theo_vung_gio"
PATH_DU_BAO = f"{LAKEHOUSE}/gold/du_bao_nhu_cau"
PATH_OD = f"{LAKEHOUSE}/gold/ma_tran_od"
PATH_DIEU_PHOI = f"{LAKEHOUSE}/silver/ke_hoach_dieu_phoi"
GEOJSON_PATH = Path("/home/master/bd04_giao_thong/taxi_zones.geojson")

ENV_HADOOP = {
    **os.environ,
    "HADOOP_HOME": "/opt/hadoop",
    "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
    "PATH": "/opt/hadoop/bin:" + os.environ.get("PATH", ""),
}
ENV_AIRFLOW = {
    **os.environ,
    "AIRFLOW_HOME": "/home/master/airflow",
    "PATH": "/home/master/airflow_venv/bin:" + os.environ.get("PATH", ""),
}

# ---------- cache GeoJSON + tra ten vung (nap 1 lan luc khoi dong) ----------

_geojson_cache = None
_ten_vung_cache = None


def doc_geojson():
    global _geojson_cache, _ten_vung_cache
    if _geojson_cache is None:
        with open(GEOJSON_PATH, encoding="utf-8") as f:
            _geojson_cache = json.load(f)
        _ten_vung_cache = {
            int(feat["properties"]["LocationID"]): feat["properties"]
            for feat in _geojson_cache["features"]
        }
    return _geojson_cache


def tra_ten_vung(vung_id):
    doc_geojson()
    info = _ten_vung_cache.get(int(vung_id))
    if info is None:
        return {"zone": f"Vung {vung_id}", "borough": None}
    return {"zone": info.get("zone"), "borough": info.get("borough")}


# ---------- tien ich mui gio ----------


def _ve_gio_he_thong_series(s):
    """Cot pandas Timestamp UTC-aware -> naive gio he thong (Asia/Ho_Chi_Minh), vector hoa."""
    return s.dt.tz_convert(ZoneInfo(MUI_GIO)).dt.tz_localize(None)


def _ve_utc_series(s):
    """Nguoc lai: naive gio he thong -> UTC-aware, dung khi can ghi lai gia tri timestamp mo ta
    dung instant that (deltalake/pyarrow luon can tz-aware instant chinh xac khi ghi TIMESTAMP)."""
    return s.dt.tz_localize(ZoneInfo(MUI_GIO)).dt.tz_convert("UTC")


# ---------- doc Delta table ----------


def doc_nhu_cau(cot=None):
    dt = DeltaTable(PATH_NHU_CAU)
    df = dt.to_pandas(columns=cot)
    if "khung_gio" in df.columns:
        df["khung_gio"] = _ve_gio_he_thong_series(df["khung_gio"])
    return df


def doc_du_bao(cot=None):
    dt = DeltaTable(PATH_DU_BAO)
    df = dt.to_pandas(columns=cot)
    if "thoi_diem_du_bao" in df.columns:
        df["thoi_diem_du_bao"] = _ve_gio_he_thong_series(df["thoi_diem_du_bao"])
    if "khung_gio_muc_tieu" in df.columns:
        df["khung_gio_muc_tieu"] = _ve_gio_he_thong_series(df["khung_gio_muc_tieu"])
    return df


def doc_od():
    dt = DeltaTable(PATH_OD)
    return dt.to_pandas()


def _dieu_phoi_ton_tai():
    try:
        DeltaTable(PATH_DIEU_PHOI)
        return True
    except Exception:
        return False


def doc_dieu_phoi():
    if not _dieu_phoi_ton_tai():
        return pd.DataFrame(
            columns=["id_ke_hoach", "vung", "khung_gio_ap_dung", "so_xe_dieu_den",
                     "ghi_chu", "thoi_diem_tao"]
        )
    dt = DeltaTable(PATH_DIEU_PHOI)
    df = dt.to_pandas()
    df["khung_gio_ap_dung"] = _ve_gio_he_thong_series(df["khung_gio_ap_dung"])
    df["thoi_diem_tao"] = _ve_gio_he_thong_series(df["thoi_diem_tao"])
    return df


def ghi_dieu_phoi(vung, khung_gio_ap_dung, so_xe_dieu_den, ghi_chu):
    now_vn = datetime.datetime.now(ZoneInfo(MUI_GIO))
    df = pd.DataFrame(
        [{
            "id_ke_hoach": str(uuid.uuid4()),
            "vung": int(vung),
            "khung_gio_ap_dung": pd.Timestamp(khung_gio_ap_dung).tz_localize(ZoneInfo(MUI_GIO)).tz_convert("UTC"),
            "so_xe_dieu_den": int(so_xe_dieu_den),
            "ghi_chu": ghi_chu or "",
            "thoi_diem_tao": pd.Timestamp(now_vn).tz_convert("UTC"),
        }]
    )
    table = pa.Table.from_pandas(df, preserve_index=False)
    mode = "append" if _dieu_phoi_ton_tai() else "overwrite"
    write_deltalake(PATH_DIEU_PHOI, table, mode=mode)
    return df.iloc[0]["id_ke_hoach"]


# ---------- he thong: dung luong + lich su DAG ----------


def dung_luong_tang():
    ket_qua = {}
    tang_duong_dan = {
        "bronze": "/lakehouse/bd04_giao_thong/bronze",
        "silver": "/lakehouse/bd04_giao_thong/silver",
        "gold": "/lakehouse/bd04_giao_thong/gold",
        "features": "/lakehouse/bd04_giao_thong/features",
        "models": "/lakehouse/bd04_giao_thong/models",
    }
    try:
        proc = subprocess.run(
            ["hdfs", "dfs", "-du", "-s"] + list(tang_duong_dan.values()),
            env=ENV_HADOOP, capture_output=True, text=True, timeout=30,
        )
        lines = [l for l in proc.stdout.splitlines() if l.strip()]
        ten_tang = list(tang_duong_dan.keys())
        for i, line in enumerate(lines):
            parts = line.split()
            if len(parts) >= 1 and i < len(ten_tang):
                ket_qua[ten_tang[i]] = {"so_byte": int(parts[0])}
    except Exception as e:
        ket_qua["loi"] = str(e)
    return ket_qua


def lich_su_dag(dag_id, so_luong=10):
    try:
        proc = subprocess.run(
            ["/home/master/airflow_venv/bin/airflow", "dags", "list-runs",
             "-d", dag_id, "-o", "json"],
            env=ENV_AIRFLOW, capture_output=True, text=True, timeout=30,
        )
        data = json.loads(proc.stdout)
        return data[:so_luong] if isinstance(data, list) else []
    except Exception as e:
        return [{"loi": str(e)}]
