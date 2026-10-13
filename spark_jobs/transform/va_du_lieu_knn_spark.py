"""
Functions/va_du_lieu_knn_spark.py - va (impute) 1 cot du lieu bi khuyet/sai schema trong
DataFrame bang KNN: voi moi dong thieu, tim k=10 dong giong nhat (khoang cach Euclidean
tren cac cot dac trung da chuan hoa z-score) trong TOAN BO du lieu cu (df_lich_su - khong
gioi han trong df_can_va truyen vao), roi lay TRUNG BINH gia tri cot can va cua 10 dong do
dien vao vi tri thieu.

Khac ADR-001 cu (chi danh rieng cho gold.nhu_cau_theo_vung_gio, chay tren driver qua
sklearn.KNNImputer voi tap ung vien da loc hep): ham nay nhan truc tiep danh sach cot dac
trung + cot can va, chay PHAN TAN tren toan bo du lieu cu (khong gioi han kich thuoc, khong
can collect ve driver), dung cho bat ky DataFrame.

Cach lam (broadcast ben "dong thieu" nho, quet phan tan ben "lich su" lon):
    1. Dong "khuyet" = cot_can_va la null, HOAC bat ky cot trong cac_cot_bat_buoc null
       (sai schema). Thuong chi vai % -> collect ve driver, broadcast.
    2. Chuan hoa z-score cac_cot_dac_trung CUNG 1 bo (trung_binh, do_lech_chuan) cho ca
       2 ben (chi tinh tren df_lich_su) de khoang cach co nghia giua cac don vi khac nhau.
    3. Quet df_lich_su 1 lan (dong hop le: cot_can_va + cac_cot_dac_trung deu khong null),
       voi moi dong tinh khoang cach toi TUNG dong khuyet da broadcast -> flatMap ra
       (ma_dong_thieu, khoang_cach, gia_tri_lich_su).
    4. Window rank theo khoang_cach tang dan trong tung ma_dong_thieu, giu k=10 gan nhat,
       lay AVG gia_tri_lich_su -> gia tri va. Join lai vao df_can_va goc theo ma_dong.

Luu y hieu nang: do O(so_dong_khuyet x so_dong_lich_su) khoang cach can tinh (dung nhu yeu
cau "khong gioi han pham vi tim"), chi phu hop khi so_dong_khuyet o muc vai nghin tro xuong
(thuc te du lieu du bao thuong duoi 1%). Neu so_dong_khuyet qua lon, nen tien loc df_lich_su
theo 1-2 cot phan loai truoc (vd cung vung/gio) de giam quy mo truoc khi goi ham nay.

    from va_du_lieu_knn_spark import va_knn
    df_da_va = va_knn(
        spark, df_can_va, df_lich_su,
        cac_cot_dac_trung=["gio_trong_ngay", "thu_trong_tuan", "PULocationID"],
        cot_can_va="nhiet_do", ma_dong="ma_chuyen", k=10,
    )
"""
from functools import reduce
import math

from pyspark.sql import Window
from pyspark.sql import functions as F


def _dong_khuyet(df, cot_can_va, cac_cot_bat_buoc):
    dieu_kien = F.col(cot_can_va).isNull()
    for c in cac_cot_bat_buoc or []:
        dieu_kien = dieu_kien | F.col(c).isNull()
    return df.where(dieu_kien)


def _chuan_hoa(df, cac_cot, thong_ke):
    """Ap z-score (trung_binh, do_lech_chuan) co san trong thong_ke vao cac_cot cua df,
    tao cot moi _chuan_<ten_cot>. thong_ke phai duoc tinh 1 lan tu df_lich_su va dung lai
    cho ca 2 ben, khong tinh rieng tung ben."""
    for c in cac_cot:
        tb, lc = thong_ke[c]
        df = df.withColumn(f"_chuan_{c}", (F.col(c) - F.lit(tb)) / F.lit(lc))
    return df


def va_knn(spark, df_can_va, df_lich_su, cac_cot_dac_trung, cot_can_va, ma_dong,
           cac_cot_bat_buoc=None, k=10):
    """
    df_can_va        : DataFrame co the chua dong thieu/sai schema can va.
    df_lich_su        : DataFrame toan bo du lieu cu lam nguon lang gieng (khong can la
                        tap con/giong df_can_va; co the la ca bang Delta day du).
    cac_cot_dac_trung : list ten cot numeric dung tinh khoang cach Euclidean.
    cot_can_va        : ten cot can dien gia tri thieu.
    ma_dong           : ten cot khoa duy nhat cua df_can_va, dung join ket qua va lai.
    cac_cot_bat_buoc  : list cot khac (ngoai cot_can_va) coi la "sai schema" neu null.
    k                 : so lang gieng gan nhat (mac dinh 10).

    Tra ve df_can_va voi cot_can_va da duoc dien cho cac dong khuyet; dong khong khuyet
    giu nguyen gia tri cu.
    """
    khuyet = _dong_khuyet(df_can_va, cot_can_va, cac_cot_bat_buoc)
    cac_dong_khuyet_tho = khuyet.select(ma_dong, *cac_cot_dac_trung).collect()
    if not cac_dong_khuyet_tho:
        return df_can_va

    lich_su_hop_le = df_lich_su.where(
        F.col(cot_can_va).isNotNull()
        & reduce(lambda a, c: a & F.col(c).isNotNull(), cac_cot_dac_trung, F.lit(True))
    )
    thong_ke_agg = lich_su_hop_le.select(
        *[F.mean(c).alias(f"tb_{c}") for c in cac_cot_dac_trung],
        *[F.stddev(c).alias(f"lc_{c}") for c in cac_cot_dac_trung],
    ).first()
    thong_ke = {
        c: (thong_ke_agg[f"tb_{c}"], thong_ke_agg[f"lc_{c}"] or 1.0)
        for c in cac_cot_dac_trung
    }

    cot_chuan = [f"_chuan_{c}" for c in cac_cot_dac_trung]
    lich_su_chuan = _chuan_hoa(lich_su_hop_le, cac_cot_dac_trung, thong_ke)

    cac_dong_khuyet = [
        {
            "ma_dong": r[ma_dong],
            "vector": [
                (r[c] - thong_ke[c][0]) / thong_ke[c][1] for c in cac_cot_dac_trung
            ],
        }
        for r in cac_dong_khuyet_tho
    ]
    bc_dong_khuyet = spark.sparkContext.broadcast(cac_dong_khuyet)

    def _khoang_cach_toi_tat_ca_dong_khuyet(hang):
        vector_ls = [hang[c] for c in cot_chuan]
        gia_tri_ls = float(hang[cot_can_va])
        for dong in bc_dong_khuyet.value:
            kc = math.sqrt(sum((a - b) ** 2 for a, b in zip(vector_ls, dong["vector"])))
            yield (dong["ma_dong"], float(kc), gia_tri_ls)

    rdd_khoang_cach = (
        lich_su_chuan.select(cot_can_va, *cot_chuan).rdd
        .flatMap(_khoang_cach_toi_tat_ca_dong_khuyet)
    )
    df_khoang_cach = rdd_khoang_cach.toDF(["ma_dong_thieu", "khoang_cach", "gia_tri"])

    cua_so = Window.partitionBy("ma_dong_thieu").orderBy("khoang_cach")
    k_lang_gieng_gan_nhat = (
        df_khoang_cach
        .withColumn("thu_tu", F.row_number().over(cua_so))
        .where(F.col("thu_tu") <= k)
    )
    gia_tri_va = k_lang_gieng_gan_nhat.groupBy("ma_dong_thieu").agg(
        F.avg("gia_tri").alias("_gia_tri_va"),
        F.count("gia_tri").alias("_so_lang_gieng_dung"),
    )

    da_va = (
        df_can_va.join(
            gia_tri_va, df_can_va[ma_dong] == gia_tri_va["ma_dong_thieu"], "left"
        )
        .withColumn(cot_can_va, F.coalesce(F.col(cot_can_va), F.col("_gia_tri_va")))
        .drop("ma_dong_thieu", "_gia_tri_va", "_so_lang_gieng_dung")
    )
    return da_va
