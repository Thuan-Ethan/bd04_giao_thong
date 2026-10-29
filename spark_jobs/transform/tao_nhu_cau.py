"""
spark_jobs/transform/tao_nhu_cau.py

Sinh gold.nhu_cau_theo_vung_gio: nhu cầu theo (vùng đón x giờ), kèm LƯỚI ĐẦY ĐỦ.
Mọi cặp (vùng 1-263, giờ trong tháng) đều có một dòng, kể cả giờ không có chuyến nào
(so_chuyen = 0). Nếu thiếu các dòng này, mô hình chỉ thấy giờ có chuyến và sẽ học lệch.

Cách làm:
  1. Bảng lịch : sinh mọi giờ của tháng (sequence + explode)
  2. Bảng vùng : 263 vùng từ taxi_zone_lookup.csv (bỏ 264, 265 "Unknown")
  3. Lưới      : bảng lịch CROSS JOIN bảng vùng
  4. Nhu cầu   : silver gom theo (vung_don, giờ của thoi_diem_don)
  5. Gộp       : lưới LEFT JOIN nhu cầu, điền 0 cho giờ trống
  6. Ghi gold  : MERGE (upsert) theo (vung, khung_gio) nên chạy lại không nhân đôi

Đơn vị xử lý: 1 tháng (theo thời điểm đón). Silver phân vùng theo nam, thang nên chỉ đọc đúng
phân vùng đó (partition pruning).

Chạy:
  spark-submit spark_jobs/transform/tao_nhu_cau.py --nam 2023 --thang 1
"""
import argparse
from datetime import datetime

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable

HDFS = "hdfs://master:9000"
LAKE = f"{HDFS}/lakehouse/bd04_giao_thong"
SILVER_PATH = f"{LAKE}/silver/chuyen_di"
GOLD_PATH = f"{LAKE}/gold/nhu_cau_theo_vung_gio"
LOOKUP_PATH = f"{HDFS}/data/tlc_raw/zones/taxi_zone_lookup.csv"

VUNG_MAX = 263
KIEU_DECIMAL = "decimal(18,6)"


def parse_args():
    p = argparse.ArgumentParser(description="silver.chuyen_di -> gold.nhu_cau_theo_vung_gio")
    p.add_argument("--nam", type=int, required=True)
    p.add_argument("--thang", type=int, required=True, help="1..12")
    p.add_argument("--dien-0-cot-tb", action="store_true",
                   help="Điền 0 cho thoi_gian_chuyen_tb và quang_duong_tb ở giờ không có chuyến "
                        "(mặc định để NULL vì không có chuyến thì không có trung bình)")
    args = p.parse_args()
    if not 1 <= args.thang <= 12:
        p.error("--thang phải từ 1 đến 12")
    return args


def khoang_thang(nam, thang):
    """Trả về (đầu tháng, đầu tháng sau)."""
    bat_dau = datetime(nam, thang, 1)
    ke_tiep = datetime(nam + 1, 1, 1) if thang == 12 else datetime(nam, thang + 1, 1)
    return bat_dau, ke_tiep


def tao_bang_lich(spark, nam, thang):
    """Mọi giờ của tháng: 00:00 ngày 1 đến 23:00 ngày cuối (tính theo giờ treo tường)."""
    bd, kt = khoang_thang(nam, thang)
    return spark.sql(
        f"SELECT explode(sequence("
        f"timestamp'{bd:%Y-%m-%d %H:%M:%S}', "
        f"timestamp'{kt:%Y-%m-%d %H:%M:%S}' - interval 1 hour, "
        f"interval 1 hour)) AS khung_gio"
    )


def doc_bang_vung(spark):
    """263 vùng hợp lệ từ taxi_zone_lookup.csv."""
    z = spark.read.option("header", True).csv(LOOKUP_PATH)
    ten_cot = next((c for c in z.columns if c.strip().lower().replace("\ufeff", "") == "locationid"), None)
    if ten_cot is None:
        raise ValueError(f"taxi_zone_lookup.csv không có cột LocationID. Cột hiện có: {z.columns}")
    vung = (z.select(F.col(f"`{ten_cot}`").cast("int").alias("vung"))
              .filter(F.col("vung").between(1, VUNG_MAX))
              .distinct())
    n = vung.count()
    if n != VUNG_MAX:
        print(f"[CANH BAO] lookup có {n} vùng hợp lệ, đề cương nói {VUNG_MAX}")
    return vung


def tinh_nhu_cau(silver):
    """Gom silver theo (vùng đón, giờ đón). Nhu cầu của một vùng là số chuyến ĐÓN tại vùng đó."""
    # Dùng DECIMAL thay vì DOUBLE khi cộng: cộng số thực phụ thuộc thứ tự dòng (không xác định
    # giữa các lần chạy), còn cộng DECIMAL cho kết quả chính xác và giống hệt mỗi lần.
    d = lambda c: F.col(c).cast(KIEU_DECIMAL)
    return (silver.groupBy(
                F.col("vung_don").alias("vung"),
                F.date_trunc("hour", F.col("thoi_diem_don")).alias("khung_gio"))
            .agg(F.count(F.lit(1)).alias("so_chuyen"),
                 F.sum(d("tong_tien")).alias("tong_doanh_thu"),
                 F.avg(d("thoi_gian_phut")).alias("thoi_gian_chuyen_tb"),
                 F.avg(d("quang_duong_mile")).alias("quang_duong_tb")))


def ghep_luoi(luoi, nhu_cau, dien_0_tb):
    g = luoi.join(nhu_cau, ["vung", "khung_gio"], "left")
    khong = F.lit(0).cast(KIEU_DECIMAL)
    tb = (lambda c: F.coalesce(F.col(c), khong)) if dien_0_tb else (lambda c: F.col(c))
    # Làm tròn trên DECIMAL (HALF_UP, xác định) rồi mới đổi sang DOUBLE để lưu.
    return g.select(
        F.col("vung").cast("int").alias("vung"),
        F.col("khung_gio"),
        F.coalesce(F.col("so_chuyen"), F.lit(0)).cast("int").alias("so_chuyen"),
        F.round(F.coalesce(F.col("tong_doanh_thu"), khong), 2).cast("double").alias("tong_doanh_thu"),
        F.round(tb("thoi_gian_chuyen_tb"), 4).cast("double").alias("thoi_gian_chuyen_tb"),
        F.round(tb("quang_duong_tb"), 4).cast("double").alias("quang_duong_tb"),
    )


def tao_gold_neu_chua_co(spark):
    if DeltaTable.isDeltaTable(spark, GOLD_PATH):
        return
    (DeltaTable.createIfNotExists(spark).location(GOLD_PATH)
        .addColumn("vung", "INT")
        .addColumn("khung_gio", "TIMESTAMP")
        .addColumn("so_chuyen", "INT")
        .addColumn("tong_doanh_thu", "DOUBLE")
        .addColumn("thoi_gian_chuyen_tb", "DOUBLE")
        .addColumn("quang_duong_tb", "DOUBLE")
        .execute())
    print(f"[GOLD] đã tạo bảng mới tại {GOLD_PATH}")


def merge_vao_gold(spark, df):
    """Upsert theo (vung, khung_gio). Chỉ UPDATE khi giá trị thật sự đổi."""
    bang = DeltaTable.forPath(spark, GOLD_PATH)
    version_truoc = bang.history(1).select("version").first()[0]

    khac = ("NOT (t.so_chuyen <=> s.so_chuyen AND t.tong_doanh_thu <=> s.tong_doanh_thu "
            "AND t.thoi_gian_chuyen_tb <=> s.thoi_gian_chuyen_tb "
            "AND t.quang_duong_tb <=> s.quang_duong_tb)")
    (bang.alias("t")
         .merge(df.alias("s"), "t.vung = s.vung AND t.khung_gio = s.khung_gio")
         .whenMatchedUpdateAll(condition=khac)
         .whenNotMatchedInsertAll()
         .execute())

    ls = bang.history(1).select("version", "operationMetrics").first()
    if ls["version"] == version_truoc:
        return 0, 0
    m = ls["operationMetrics"]
    return int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0))


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName(f"tao_nhu_cau_{args.nam}_{args.thang:02d}")
        .config("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
        # Cùng múi giờ UTC với bronze/silver để giờ treo tường không bị lệch
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    silver = (spark.read.format("delta").load(SILVER_PATH)
              .filter((F.col("nam") == args.nam) & (F.col("thang") == args.thang)))
    so_silver = silver.count()
    print(f"[INPUT] silver phân vùng {args.nam}-{args.thang:02d}: {so_silver:,} dòng")
    if so_silver == 0:
        print("[CANH BAO] silver rỗng ở phân vùng này, dừng.")
        spark.stop()
        return

    lich = tao_bang_lich(spark, args.nam, args.thang)
    vung = doc_bang_vung(spark)
    luoi = lich.crossJoin(vung)

    df = (ghep_luoi(luoi, tinh_nhu_cau(silver), args.dien_0_cot_tb)
          .persist(StorageLevel.MEMORY_AND_DISK))

    so_gio = lich.count()
    kq = df.agg(F.count(F.lit(1)).alias("dong"),
                F.sum("so_chuyen").alias("tong_chuyen"),
                F.sum(F.when(F.col("so_chuyen") > 0, 1).otherwise(0)).alias("dong_co_chuyen")).first()
    dong, tong_chuyen, co_chuyen = int(kq["dong"]), int(kq["tong_chuyen"]), int(kq["dong_co_chuyen"])

    tao_gold_neu_chua_co(spark)
    so_chen, so_sua = merge_vao_gold(spark, df)
    df.unpersist()

    print("=" * 72)
    print(f"[LUOI] {so_gio} giờ x {VUNG_MAX} vùng = {so_gio * VUNG_MAX:,} dòng (thực tế: {dong:,})")
    print(f"[LUOI] dòng có chuyến: {co_chuyen:,} | dòng giờ trống điền 0: {dong - co_chuyen:,} "
          f"({(dong - co_chuyen) / dong:.1%})")
    print(f"[KIEM TRA] tổng so_chuyen gold = {tong_chuyen:,} | số dòng silver = {so_silver:,} | "
          f"{'KHOP' if tong_chuyen == so_silver else 'LECH!'}")
    print(f"[KET QUA] chèn: {so_chen:,} | cập nhật: {so_sua:,}")
    print("=" * 72)
    spark.stop()


if __name__ == "__main__":
    main()