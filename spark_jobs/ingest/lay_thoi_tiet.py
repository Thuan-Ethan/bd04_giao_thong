"""
Functions/lay_thoi_tiet.py - lay nhiet do + luong mua theo gio tai New York tu
Open-Meteo Historical Weather API (archive-api.open-meteo.com, ERA5 reanalysis).

Input: 1 hoac nhieu thoi diem, gio dia phuong New York, dinh dang 24h
    ("YYYY-MM-DD HH:MM" hoac datetime.datetime; phut/giay bi bo qua, ep ve gio tron).
Output: list [{"thoi_diem": <dau vao goc>, "nhiet_do": float|None, "luong_mua": float|None}, ...]
    cung thu tu voi dau vao.

Quy uoc giu dung voi /data/raw/open_meteo/README.txt (da tai du lieu lon truoc do):
- Toa do: 40.7812, -73.9665 (Central Park, NYC) -- khop voi du lieu da tai san tren HDFS.
- Luon goi API voi timezone=UTC roi tu quy doi sang gio New York bang zoneinfo.
  KHONG dung timezone=America/New_York cua API: no ap 1 do lech UTC co dinh cho ca
  chuoi thoi gian -> sai gio vao mua dong (da phat hien truoc do).
- Gio mo ho khi lui gio thang 11 (1 gio dia phuong xuat hien 2 lan, EDT roi EST):
  lay ca 2 instant UTC, gop lai AVG nhiet_do / SUM luong_mua.
- Gio khong ton tai khi tien gio thang 3 (vd 02:30 bi nhay qua): tra ve None,
  khong doan/goi API sai.

Vi du:
    python3 lay_thoi_tiet.py '["2022-01-15 14:00", "2023-07-04 08:00"]'
"""
import datetime
import json
import sys
from zoneinfo import ZoneInfo

import requests

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
VI_TRI = {"latitude": 40.7812, "longitude": -73.9665}  # Central Park, NYC
MUI_GIO_NY = ZoneInfo("America/New_York")
MUI_GIO_UTC = datetime.timezone.utc


def _ep_thoi_diem_ny(gia_tri):
    """Chuyen str/datetime dau vao -> datetime co tzinfo=NY, gio tron (bo phut/giay)."""
    if isinstance(gia_tri, str):
        dt = datetime.datetime.fromisoformat(gia_tri)
    elif isinstance(gia_tri, datetime.datetime):
        dt = gia_tri
    else:
        raise TypeError(f"thoi_diem phai la str hoac datetime, nhan {type(gia_tri)!r}")
    dt = dt.replace(tzinfo=MUI_GIO_NY) if dt.tzinfo is None else dt.astimezone(MUI_GIO_NY)
    return dt.replace(minute=0, second=0, microsecond=0)


def _cac_instant_utc(dt_ny):
    """Tra ve list instant UTC ung voi 1 gio dia phuong NY (xem quy uoc DST o dau file).
    [] neu gio khong ton tai; 1 phan tu neu binh thuong; 2 phan tu neu mo ho (lui gio thang 11)."""
    utc_fold0 = dt_ny.replace(fold=0).astimezone(MUI_GIO_UTC)
    utc_fold1 = dt_ny.replace(fold=1).astimezone(MUI_GIO_UTC)
    lai_ny = utc_fold0.astimezone(MUI_GIO_NY)
    if (lai_ny.hour, lai_ny.day, lai_ny.month) != (dt_ny.hour, dt_ny.day, dt_ny.month):
        return []
    return [utc_fold0] if utc_fold0 == utc_fold1 else [utc_fold0, utc_fold1]


def lay_thoi_tiet(danh_sach_thoi_diem, timeout=30):
    don_le = not isinstance(danh_sach_thoi_diem, (list, tuple))
    danh_sach_goc = [danh_sach_thoi_diem] if don_le else list(danh_sach_thoi_diem)
    if not danh_sach_goc:
        return []

    danh_sach_ny = [_ep_thoi_diem_ny(x) for x in danh_sach_goc]
    danh_sach_instant = [_cac_instant_utc(dt) for dt in danh_sach_ny]

    tat_ca_instant = [u for cac_u in danh_sach_instant for u in cac_u]
    if not tat_ca_instant:
        return [{"thoi_diem": g, "nhiet_do": None, "luong_mua": None} for g in danh_sach_goc]

    tham_so = {
        **VI_TRI,
        "start_date": min(tat_ca_instant).date().isoformat(),
        "end_date": max(tat_ca_instant).date().isoformat(),
        "hourly": "temperature_2m,precipitation",
        "timezone": "UTC",
    }
    phan_hoi = requests.get(ARCHIVE_URL, params=tham_so, timeout=timeout)
    phan_hoi.raise_for_status()
    du_lieu = phan_hoi.json()["hourly"]
    tra_cuu = dict(zip(du_lieu["time"], zip(du_lieu["temperature_2m"], du_lieu["precipitation"])))

    ket_qua = []
    for goc, cac_instant in zip(danh_sach_goc, danh_sach_instant):
        if not cac_instant:
            ket_qua.append({"thoi_diem": goc, "nhiet_do": None, "luong_mua": None})
            continue
        gia_tri = [tra_cuu.get(u.strftime("%Y-%m-%dT%H:%M")) for u in cac_instant]
        gia_tri = [g for g in gia_tri if g is not None]
        if not gia_tri:
            ket_qua.append({"thoi_diem": goc, "nhiet_do": None, "luong_mua": None})
            continue
        nhiet_do = sum(g[0] for g in gia_tri) / len(gia_tri)  # AVG, ke ca gio mo ho (2 ban ghi)
        luong_mua = sum(g[1] for g in gia_tri)                # SUM, ke ca gio mo ho (2 ban ghi)
        ket_qua.append({"thoi_diem": goc, "nhiet_do": round(nhiet_do, 2), "luong_mua": round(luong_mua, 2)})
    return ket_qua


if __name__ == "__main__":
    dau_vao = json.loads(sys.argv[1]) if len(sys.argv) > 1 else ["2022-01-15 14:00"]
    print(json.dumps(lay_thoi_tiet(dau_vao), ensure_ascii=False, indent=2))
