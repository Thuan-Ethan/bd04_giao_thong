"""
spark_jobs/transform/tao_ma_tran_od.py

Sinh gold.ma_tran_od (Origin-Destination): với mỗi cặp (vùng đón, vùng trả), mỗi giờ trong ngày
(0-23) và mỗi thứ trong tuần (1-7), đếm số chuyến và tính trung vị / phân vị 90 của thời gian chuyến.

Dùng cho: baseline và đặc trưng của mô hình ETA (mục 4.6), màn hình Ma trận OD và Chi tiết vùng (4.8).

KHÔNG có ngày trong khóa, nên bảng đã gộp mọi tuần thành một hồ sơ. Trung vị không cộng dồn được
theo tháng, vì vậy mỗi lần chạy tính lại TOÀN BỘ phạm vi rồi ghi đè bảng.

CHỐNG RÒ RỈ DỮ LIỆU: ma_tran_od dùng làm baseline/đặc trưng cho mô hình ETA phải chỉ tính trên
các tháng TRAIN (mặc định 2023-01 .. 2024-06, tức 18 tháng). Nếu tính cả tháng test thì baseline
đã "nhìn thấy" đáp án. Bản đầy đủ 24 tháng (--day-du) chỉ để web hiển thị, ghi sang bảng khác.

Quy ước thu_trong_tuan: ISO, 1 = Thứ hai ... 7 = Chủ nhật.
Giờ tính theo giờ treo tường của New York (múi giờ phiên đặt UTC như bronze/silver).

Chạy:
  # Bản train-only (bảng ma_tran_od chính thức, dùng cho ML)
  spark-submit spark_jobs/transform/tao_ma_tran_od.py
  # Bản đầy đủ 24 tháng cho web (ghi vào gold/ma_tran_od_day_du)
  spark-submit spark_jobs/transform/tao_ma_tran_od.py --day-du
  # Tùy chỉnh phạm vi
  spark-submit spark_jobs/transform/tao_ma_tran_od.py --tu 2023-01 --den 2023-12 --ra hdfs://.../gold/ma_tran_od_thu
"""
import argparse

from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://master:9000"
LAKE = f"{HDFS}/lakehouse/bd04_giao_thong"
SILVER_PATH = f"{LAKE}/silver/chuyen_di"
GOLD_PATH = f"{LAKE}/gold/ma_tran_od"
GOLD_DAY_DU = f"{LAKE}/gold/ma_tran_od_day_du"

VUNG_MAX = 263
NGUONG_IT_CHUYEN = 30   # chỉ để thống kê: ô dưới ngưỡng này có trung vị kém tin cậy


def parse_ym(s):
    try:
        nam, thang = s.split("-")
        nam, thang = int(nam), int(thang)
        assert 1 <= thang <= 12
        return nam, thang
    except Exception:
        raise argparse.ArgumentTypeError(f"'{s}' không đúng dạng YYYY-MM")


def parse_args():
    p = argparse.ArgumentParser(description="silver.chuyen_di -> gold.ma_tran_od")
    p.add_argument("--tu", type=parse_ym, default=None,
                   help="Tháng bắt đầu YYYY-MM (mặc định 2023-01)")
    p.add_argument("--den", type=parse_ym, default=None,
                   help="Tháng kết thúc YYYY-MM, gồm cả tháng này (mặc định 2024-06; 2024-12 nếu --day-du)")
    p.add_argument("--day-du", action="store_true",
                   help="Tính cả 24 tháng và ghi vào gold/ma_tran_od_day_du (chỉ để web, KHÔNG dùng cho ML)")
    p.add_argument("--ra", default=None, help="Đường dẫn Delta đầu ra (mặc định theo --day-du)")
    args = p.parse_args()

    args.tu = args.tu or (2023, 1)
    args.den = args.den or ((2024, 12) if args.day_du else (2024, 6))
    args.ra = args.ra or (GOLD_DAY_DU if args.day_du else GOLD_PATH)
    if args.tu > args.den:
        p.error("--tu phải nhỏ hơn hoặc bằng --den")
    return args


def doc_silver(spark, tu, den):
    """Đọc silver trong phạm vi tháng. Điều kiện chỉ dùng cột phân vùng nên Delta bỏ qua phân vùng ngoài khoảng."""
    ym = F.col("nam") * 100 + F.col("thang")
    return (spark.read.format("delta").load(SILVER_PATH)
            .filter(ym.between(tu[0] * 100 + tu[1], den[0] * 100 + den[1])))


def tinh_ma_tran(silver):
    """
    Gom theo (vung_don, vung_tra, giờ trong ngày, thứ).
    Dùng percentile CHÍNH XÁC thay vì percentile_approx: kết quả giống hệt mỗi lần chạy
    (percentile_approx có thể lệch giữa các lần), và vẫn nhẹ vì thoi_gian_phut chỉ có
    tối đa khoảng 18.000 giá trị khác nhau (1-300 phút, bước 1 giây).
    """
    s = (silver
         .withColumn("khung_gio_trong_ngay", F.hour("thoi_diem_don"))
         # dayofweek: 1 = Chủ nhật ... 7 = Thứ bảy  ->  ISO: 1 = Thứ hai ... 7 = Chủ nhật
         .withColumn("thu_trong_tuan", ((F.dayofweek("thoi_diem_don") + 5) % 7) + 1))

    return (s.groupBy("vung_don", "vung_tra", "khung_gio_trong_ngay", "thu_trong_tuan")
             .agg(F.count(F.lit(1)).alias("so_chuyen"),
                  F.round(F.expr("percentile(thoi_gian_phut, 0.5)"), 4).alias("thoi_gian_trung_vi"),
                  F.round(F.expr("percentile(thoi_gian_phut, 0.9)"), 4).alias("thoi_gian_p90"))
             .select(F.col("vung_don").cast("int"),
                     F.col("vung_tra").cast("int"),
                     F.col("khung_gio_trong_ngay").cast("int"),
                     F.col("thu_trong_tuan").cast("int"),
                     F.col("so_chuyen").cast("long"),
                     "thoi_gian_trung_vi", "thoi_gian_p90"))


def ghi_gold(df, path, tu, den):
    """Ghi đè bảng; ghi phạm vi tháng vào lịch sử Delta (cột userMetadata của DESCRIBE HISTORY)."""
    ghi_chu = f"tu={tu[0]}-{tu[1]:02d};den={den[0]}-{den[1]:02d}"
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .option("userMetadata", ghi_chu)
       .save(path))
    return ghi_chu


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("tao_ma_tran_od")
        .config("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    tu, den = args.tu, args.den
    print(f"[PHAM VI] {tu[0]}-{tu[1]:02d} .. {den[0]}-{den[1]:02d} | ra: {args.ra}")
    if den > (2024, 6) and args.ra == GOLD_PATH:
        print("[CANH BAO] phạm vi vượt 2024-06 nhưng ghi vào gold/ma_tran_od (bảng dùng cho ML). "
              "Nếu dùng làm baseline/đặc trưng ETA, tháng validation/test sẽ rò rỉ vào ma trận!")

    silver = doc_silver(spark, tu, den)
    so_silver = silver.count()
    print(f"[INPUT] silver trong phạm vi: {so_silver:,} dòng")
    if so_silver == 0:
        print("[CANH BAO] không có dòng silver nào trong phạm vi, dừng.")
        spark.stop()
        return

    ghi_chu = ghi_gold(tinh_ma_tran(silver), args.ra, tu, den)

    # Đọc lại bảng vừa ghi để kiểm tra (tránh tính lại phép gom nặng)
    od = spark.read.format("delta").load(args.ra)
    kq = od.agg(
        F.count(F.lit(1)).alias("so_o"),
        F.sum("so_chuyen").alias("tong_chuyen"),
        F.sum(F.when(F.col("thoi_gian_trung_vi") > F.col("thoi_gian_p90"), 1).otherwise(0)).alias("sai_phan_vi"),
        F.sum(F.when(F.col("so_chuyen") < NGUONG_IT_CHUYEN, 1).otherwise(0)).alias("o_it_chuyen"),
        F.sum(F.when(~(F.col("vung_don").between(1, VUNG_MAX) & F.col("vung_tra").between(1, VUNG_MAX)), 1)
               .otherwise(0)).alias("sai_vung"),
        F.countDistinct("vung_don", "vung_tra", "khung_gio_trong_ngay", "thu_trong_tuan").alias("khoa_khac_nhau"),
    ).first()

    so_o, tong = int(kq["so_o"]), int(kq["tong_chuyen"])
    toi_da = VUNG_MAX * VUNG_MAX * 24 * 7
    print("=" * 72)
    print(f"[KET QUA] số ô (dòng) của ma trận : {so_o:,} / {toi_da:,} tối đa ({so_o / toi_da:.1%} có chuyến)")
    print(f"[KET QUA] ô có dưới {NGUONG_IT_CHUYEN} chuyến     : {int(kq['o_it_chuyen']):,} ({int(kq['o_it_chuyen']) / so_o:.1%})")
    print(f"[KIEM TRA] tổng so_chuyen OD = {tong:,} | số dòng silver = {so_silver:,} | "
          f"{'KHOP' if tong == so_silver else 'LECH!'}")
    print(f"[KIEM TRA] khóa duy nhất      : {'DAT' if int(kq['khoa_khac_nhau']) == so_o else 'TRUNG KHOA!'}")
    print(f"[KIEM TRA] trung vị > p90     : {int(kq['sai_phan_vi'])} dòng (phải bằng 0)")
    print(f"[KIEM TRA] vùng ngoài 1-263   : {int(kq['sai_vung'])} dòng (phải bằng 0)")
    print(f"[GHI NHAN] userMetadata        : {ghi_chu}")
    print("=" * 72)
    spark.stop()


if __name__ == "__main__":
    main()