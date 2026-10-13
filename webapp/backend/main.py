"""
main.py - Backend FastAPI cho webapp bd04_giao_thong, dac ta theo de cuong muc 4.8 + thiet ke
docs/06-thiet-ke-ung-dung-web.md. 5 man hinh: Ban do nhiet nhu cau, Chi tiet vung, Ma tran OD,
Dieu phoi (ghi nguoc), Hieu nang he thong.

Chay: /home/master/webapp_venv/bin/uvicorn main:app --host 0.0.0.0 --port 8090
"""
import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import doc_du_lieu as ddl

MUI_GIO = "Asia/Ho_Chi_Minh"
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

app = FastAPI(title="bd04_giao_thong - API")


def _ep_thoi_gian(chuoi: str) -> datetime.datetime:
    try:
        return datetime.datetime.fromisoformat(chuoi)
    except ValueError:
        raise HTTPException(400, f"Khong doc duoc thoi gian: {chuoi}")


# =====================================================================================
# Man hinh 1 - Ban do nhiet nhu cau
# =====================================================================================


@app.get("/api/vung-geojson")
def vung_geojson():
    return ddl.doc_geojson()


@app.get("/api/khung-gio-khoang")
def khung_gio_khoang():
    nhu_cau = ddl.doc_nhu_cau(cot=["khung_gio"])
    du_bao = ddl.doc_du_bao(cot=["khung_gio_muc_tieu"])
    return {
        "thuc_te": {
            "min": nhu_cau["khung_gio"].min().isoformat(),
            "max": nhu_cau["khung_gio"].max().isoformat(),
        },
        "du_bao": {
            "min": du_bao["khung_gio_muc_tieu"].min().isoformat() if len(du_bao) else None,
            "max": du_bao["khung_gio_muc_tieu"].max().isoformat() if len(du_bao) else None,
        },
    }


@app.get("/api/nhu-cau")
def nhu_cau(khung_gio: str = Query(...), che_do: str = Query("thuc_te")):
    khung_gio_dt = _ep_thoi_gian(khung_gio)

    if che_do == "thuc_te":
        df = ddl.doc_nhu_cau(cot=["vung", "khung_gio", "so_chuyen"])
        hang = df[df["khung_gio"] == khung_gio_dt]
        ket_qua = [
            {"vung": int(r.vung), "gia_tri": int(r.so_chuyen)} for r in hang.itertuples()
        ]
    elif che_do == "du_bao":
        df = ddl.doc_du_bao(cot=["vung", "khung_gio_muc_tieu", "so_chuyen_du_bao", "horizon_gio",
                                  "thoi_diem_du_bao"])
        hang = df[df["khung_gio_muc_tieu"] == khung_gio_dt]
        # lay ban du bao gan nhat (thoi_diem_du_bao lon nhat) cho moi vung, uu tien horizon=1
        hang = hang.sort_values(["vung", "horizon_gio", "thoi_diem_du_bao"],
                                 ascending=[True, True, False])
        hang = hang.drop_duplicates(subset=["vung"], keep="first")
        ket_qua = [
            {"vung": int(r.vung), "gia_tri": round(float(r.so_chuyen_du_bao), 1)}
            for r in hang.itertuples()
        ]
    elif che_do == "sai_so":
        thuc_te = ddl.doc_nhu_cau(cot=["vung", "khung_gio", "so_chuyen"])
        thuc_te = thuc_te[thuc_te["khung_gio"] == khung_gio_dt][["vung", "so_chuyen"]]
        du_bao = ddl.doc_du_bao(cot=["vung", "khung_gio_muc_tieu", "so_chuyen_du_bao",
                                      "horizon_gio"])
        du_bao = du_bao[(du_bao["khung_gio_muc_tieu"] == khung_gio_dt)
                         & (du_bao["horizon_gio"] == 1)][["vung", "so_chuyen_du_bao"]]
        gop = thuc_te.merge(du_bao, on="vung", how="inner")
        gop["sai_so"] = gop["so_chuyen"] - gop["so_chuyen_du_bao"]
        ket_qua = [
            {"vung": int(r.vung), "gia_tri": round(float(r.sai_so), 1)}
            for r in gop.itertuples()
        ]
    else:
        raise HTTPException(400, "che_do phai la thuc_te | du_bao | sai_so")

    return {"khung_gio": khung_gio, "che_do": che_do, "du_lieu": ket_qua}


# =====================================================================================
# Man hinh 2 - Chi tiet vung
# =====================================================================================


@app.get("/api/vung/{vung_id}")
def chi_tiet_vung(vung_id: int):
    if not (1 <= vung_id <= 263):
        raise HTTPException(404, "vung phai trong khoang 1-263")

    ten = ddl.tra_ten_vung(vung_id)

    nhu_cau_df = ddl.doc_nhu_cau(cot=["vung", "khung_gio", "so_chuyen"])
    cua_vung = nhu_cau_df[nhu_cau_df["vung"] == vung_id]
    gio_moi_nhat = nhu_cau_df["khung_gio"].max()
    bay_ngay_truoc = gio_moi_nhat - datetime.timedelta(days=7)
    lich_su = cua_vung[cua_vung["khung_gio"] >= bay_ngay_truoc].sort_values("khung_gio")
    lich_su_7_ngay = [
        {"khung_gio": r.khung_gio.isoformat(), "so_chuyen": int(r.so_chuyen)}
        for r in lich_su.itertuples()
    ]

    du_bao_df = ddl.doc_du_bao(cot=["vung", "thoi_diem_du_bao", "khung_gio_muc_tieu",
                                     "horizon_gio", "so_chuyen_du_bao"])
    cua_vung_db = du_bao_df[du_bao_df["vung"] == vung_id]
    du_bao_2h_toi = []
    if len(cua_vung_db):
        thoi_diem_gan_nhat = cua_vung_db["thoi_diem_du_bao"].max()
        gan_nhat = cua_vung_db[cua_vung_db["thoi_diem_du_bao"] == thoi_diem_gan_nhat]
        gan_nhat = gan_nhat.sort_values("horizon_gio")
        du_bao_2h_toi = [
            {
                "khung_gio_muc_tieu": r.khung_gio_muc_tieu.isoformat(),
                "horizon_gio": int(r.horizon_gio),
                "so_chuyen_du_bao": round(float(r.so_chuyen_du_bao), 1),
            }
            for r in gan_nhat.itertuples()
        ]

    od_df = ddl.doc_od()
    tu_vung = od_df[od_df["vung_don"] == vung_id]
    top5 = (
        tu_vung.groupby("vung_tra")
        .agg(so_chuyen=("so_chuyen", "sum"), thoi_gian_trung_vi=("thoi_gian_trung_vi", "mean"))
        .reset_index()
        .sort_values("so_chuyen", ascending=False)
        .head(5)
    )
    top_5_diem_den = [
        {
            "vung_tra": int(r.vung_tra),
            "ten_vung": ddl.tra_ten_vung(r.vung_tra)["zone"],
            "so_chuyen": int(r.so_chuyen),
        }
        for r in top5.itertuples()
    ]

    thoi_gian_chuyen_tb_phut = (
        round(float(tu_vung["thoi_gian_trung_vi"].mean()), 1) if len(tu_vung) else None
    )

    return {
        "vung": vung_id,
        "ten_vung": ten["zone"],
        "borough": ten["borough"],
        "lich_su_7_ngay": lich_su_7_ngay,
        "du_bao_2h_toi": du_bao_2h_toi,
        "top_5_diem_den": top_5_diem_den,
        "thoi_gian_chuyen_tb_phut": thoi_gian_chuyen_tb_phut,
    }


# =====================================================================================
# Man hinh 3 - Ma tran OD
# =====================================================================================


@app.get("/api/od")
def ma_tran_od(gio: int = Query(..., ge=0, le=23), thu: int = Query(..., ge=1, le=7),
                day_du: bool = Query(False)):
    df = ddl.doc_od()
    loc = df[(df["khung_gio_trong_ngay"] == gio) & (df["thu_trong_tuan"] == thu)]

    if not day_du:
        top30 = (
            loc.groupby("vung_don")["so_chuyen"].sum().sort_values(ascending=False).head(30).index
        )
        loc = loc[loc["vung_don"].isin(top30) & loc["vung_tra"].isin(top30)]

    du_lieu = [
        {
            "vung_don": int(r.vung_don),
            "vung_tra": int(r.vung_tra),
            "so_chuyen": int(r.so_chuyen),
            "thoi_gian_trung_vi": float(r.thoi_gian_trung_vi) if pd.notna(r.thoi_gian_trung_vi) else None,
        }
        for r in loc.itertuples()
    ]
    return {"gio": gio, "thu": thu, "rut_gon": not day_du, "du_lieu": du_lieu}


@app.get("/api/od/{vung_don}/{vung_tra}")
def phan_phoi_od(vung_don: int, vung_tra: int):
    df = ddl.doc_od()
    loc = df[(df["vung_don"] == vung_don) & (df["vung_tra"] == vung_tra)]
    if not len(loc):
        raise HTTPException(404, "Khong co du lieu cho cap vung nay")
    chi_tiet = [
        {
            "khung_gio_trong_ngay": int(r.khung_gio_trong_ngay),
            "thu_trong_tuan": int(r.thu_trong_tuan),
            "so_chuyen": int(r.so_chuyen),
            "thoi_gian_trung_vi": float(r.thoi_gian_trung_vi) if pd.notna(r.thoi_gian_trung_vi) else None,
            "thoi_gian_p90": float(r.thoi_gian_p90) if pd.notna(r.thoi_gian_p90) else None,
        }
        for r in loc.itertuples()
    ]
    return {
        "vung_don": vung_don, "vung_tra": vung_tra,
        "ten_vung_don": ddl.tra_ten_vung(vung_don)["zone"],
        "ten_vung_tra": ddl.tra_ten_vung(vung_tra)["zone"],
        "chi_tiet": chi_tiet,
    }


# =====================================================================================
# Man hinh 4 - Dieu phoi (ghi nguoc)
# =====================================================================================


class YeuCauDieuPhoi(BaseModel):
    vung: int
    khung_gio_ap_dung: str
    so_xe_dieu_den: int
    ghi_chu: str | None = None


@app.post("/api/dieu-phoi")
def tao_dieu_phoi(yc: YeuCauDieuPhoi):
    if not (1 <= yc.vung <= 263):
        raise HTTPException(400, "vung phai trong khoang 1-263")
    if yc.so_xe_dieu_den < 0:
        raise HTTPException(400, "so_xe_dieu_den khong duoc am")
    khung_gio_dt = _ep_thoi_gian(yc.khung_gio_ap_dung)
    id_ke_hoach = ddl.ghi_dieu_phoi(yc.vung, khung_gio_dt, yc.so_xe_dieu_den, yc.ghi_chu)
    return {"id_ke_hoach": id_ke_hoach, "trang_thai": "da_ghi"}


@app.get("/api/dieu-phoi")
def doi_chieu_dieu_phoi(khung_gio: str = Query(...)):
    khung_gio_dt = _ep_thoi_gian(khung_gio)
    ke_hoach = ddl.doc_dieu_phoi()
    ke_hoach = ke_hoach[ke_hoach["khung_gio_ap_dung"] == khung_gio_dt]

    thuc_te = ddl.doc_nhu_cau(cot=["vung", "khung_gio", "so_chuyen"])
    thuc_te = thuc_te[thuc_te["khung_gio"] == khung_gio_dt][["vung", "so_chuyen"]]

    gop = ke_hoach.merge(thuc_te, on="vung", how="left")
    ket_qua = [
        {
            "id_ke_hoach": r.id_ke_hoach,
            "vung": int(r.vung),
            "ten_vung": ddl.tra_ten_vung(r.vung)["zone"],
            "so_xe_dieu_den": int(r.so_xe_dieu_den),
            "ghi_chu": r.ghi_chu,
            "so_chuyen_thuc_te": int(r.so_chuyen) if pd.notna(r.so_chuyen) else None,
        }
        for r in gop.itertuples()
    ]
    return {"khung_gio": khung_gio, "du_lieu": ket_qua}


# =====================================================================================
# Man hinh 5 - Hieu nang he thong
# =====================================================================================


@app.get("/api/he-thong/thong-ke")
def thong_ke_he_thong():
    return {
        "dung_luong_tang": ddl.dung_luong_tang(),
        "lich_su_dag": {
            "gt_du_bao_streaming": ddl.lich_su_dag("gt_du_bao_streaming", so_luong=10),
            "gt_huan_luyen_lai": ddl.lich_su_dag("gt_huan_luyen_lai", so_luong=10),
        },
        "thi_nghiem_hieu_nang": {
            "trang_thai": "chua_chay",
            "ghi_chu": "Hang muc 7 thi nghiem hieu nang (de cuong muc 4.5) chua duoc thuc hien.",
        },
    }


# =====================================================================================
# Phuc vu frontend tinh
# =====================================================================================


@app.get("/")
def trang_chu():
    return FileResponse(FRONTEND_DIR / "index.html")


app.mount("/", StaticFiles(directory=str(FRONTEND_DIR)), name="frontend")
