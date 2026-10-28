"""
spark_jobs/transform/bronze_to_silver.py

Chuẩn hóa bronze.chuyen_di (schema gốc TLC, thay đổi theo năm) về silver.chuyen_di
(schema thống nhất) và làm sạch theo 7 quy tắc:
    1. thoi_diem_tra <= thoi_diem_don → loại
    2. thoi_gian_phut < 1 hoặc > 300 → loại
    3. quang_duong_mile <= 0 hoặc > 100 → loại
    4. van_toc_tb_mph > 80 → loại
    5. so_khach = 0 hoặc > 8 → loại
    6. vung_don hoặc vung_tra không thuộc 1–263 (loại bỏ vùng “Unknown” 264/265) → _quarantine
    7. tong_tien < 0 → loại (chuyến hoàn tiền) 


Đơn vị xử lý: 1 phân vùng (nam, thang) của bronze, tính theo THỜI ĐIỂM ĐÓN.
(Lưu ý: file tháng 1 có thể chứa vài chuyến thuộc tháng 2 hoặc năm 2022, 2008;
 chúng nằm ở phân vùng riêng của bronze. Phân vùng nào không được chạy thì không vào silver.)

Đầu ra:
  silver/chuyen_di          bản ghi sạch (Delta, phân vùng nam, thang)
  _quarantine/chuyen_di     bản ghi có vùng ngoài 1-263 (quy tắc 6)
  gold/thong_ke_lam_sach    thống kê số bản ghi bị loại ở từng quy tắc

Idempotent: MERGE chỉ chèn theo ma_chuyen; --lam-lai xóa phân vùng đó rồi tính lại.

Chạy:
  spark-submit spark_jobs/transform/bronze_to_silver.py --nam 2023 --thang 1
  spark-submit spark_jobs/transform/bronze_to_silver.py --nam 2023 --thang 1 --lam-lai
"""
import argparse

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable

HDFS = "hdfs://master:9000"
LAKE = f"{HDFS}/lakehouse/bd04_giao_thong"
BRONZE_PATH = f"{LAKE}/bronze/chuyen_di"
SILVER_PATH = f"{LAKE}/silver/chuyen_di"
QUARANTINE_PATH = f"{LAKE}/_quarantine/chuyen_di"
THONG_KE_PATH = f"{LAKE}/gold/thong_ke_lam_sach"

VUNG_MAX = 263          # 264, 265 là "Unknown" -> quarantine
MA_QUARANTINE = "R6_vung_ngoai_1_263"

# Tên cột gốc TLC (tìm không phân biệt hoa thường vì tên đổi chữ hoa giữa các năm)
COT_GOC = {
    "don": "tpep_pickup_datetime",
    "tra": "tpep_dropoff_datetime",
    "vung_don": "PULocationID",
    "vung_tra": "DOLocationID",
    "so_khach": "passenger_count",
    "quang_duong": "trip_distance",
    "tong_tien": "total_amount",
    "thanh_toan": "payment_type",
}


def parse_args():
    p = argparse.ArgumentParser(description="bronze.chuyen_di -> silver.chuyen_di")
    p.add_argument("--nam", type=int, required=True, help="Năm của thời điểm đón")
    p.add_argument("--thang", type=int, required=True, help="Tháng của thời điểm đón (1..12)")
    p.add_argument("--lam-lai", action="store_true",
                   help="Xóa phân vùng này ở silver, quarantine, thống kê rồi tính lại")
    args = p.parse_args()
    if not 1 <= args.thang <= 12:
        p.error("--thang phải từ 1 đến 12")
    return args


def tim_cot(df, ten):
    """Tìm tên cột thật trong df, không phân biệt hoa thường."""
    for c in df.columns:
        if c.lower() == ten.lower():
            return c
    raise ValueError(f"Bronze không có cột '{ten}'. Cột hiện có: {df.columns}")


def chuan_hoa(bronze):
    """Đổi tên, ép kiểu về schema silver, tính thoi_gian_phut và van_toc_tb_mph."""
    def c(khoa):
        return F.col(tim_cot(bronze, COT_GOC[khoa]))

    tt = c("thanh_toan").cast("int")
    hinh_thuc = (F.when(tt == 1, "the")
                  .when(tt == 2, "tien_mat")
                  .when(tt == 3, "mien_phi")
                  .when(tt == 4, "tranh_chap")
                  .when(tt == 6, "huy_chuyen")
                  .otherwise("khong_ro"))

    df = bronze.select(
        F.col("ma_chuyen"),
        c("don").cast("timestamp").alias("thoi_diem_don"),
        c("tra").cast("timestamp").alias("thoi_diem_tra"),
        c("vung_don").cast("int").alias("vung_don"),
        c("vung_tra").cast("int").alias("vung_tra"),
        c("so_khach").cast("int").alias("so_khach"),
        c("quang_duong").cast("double").alias("quang_duong_mile"),
        c("tong_tien").cast("double").alias("tong_tien"),
        hinh_thuc.alias("hinh_thuc_tt"),
    )
    # Thời gian chuyến (phút), tính từ 2 timestamp
    df = df.withColumn(
        "thoi_gian_phut",
        (F.col("thoi_diem_tra").cast("double") - F.col("thoi_diem_don").cast("double")) / 60.0,
    )
    # Vận tốc trung bình (dặm/giờ); NULL nếu thời gian <= 0 (đã bị quy tắc 1, 2 chặn)
    df = df.withColumn(
        "van_toc_tb_mph",
        F.when(F.col("thoi_gian_phut") > 0,
               F.col("quang_duong_mile") / (F.col("thoi_gian_phut") / 60.0)),
    )
    return df


def dieu_kien_quy_tac():

    # 7 quy tắc 

    t = lambda cond, mac_dinh: F.coalesce(cond, F.lit(mac_dinh))
    return [
        ("R1_tra_truoc_don", "thoi_diem_tra <= thoi_diem_don", "loai",
         t(F.col("thoi_diem_tra") <= F.col("thoi_diem_don"), True)),
        ("R2_thoi_gian_ngoai_1_300", "thoi_gian_phut < 1 hoac > 300", "loai",
         t((F.col("thoi_gian_phut") < 1) | (F.col("thoi_gian_phut") > 300), True)),
        ("R3_quang_duong_ngoai_0_100", "quang_duong_mile <= 0 hoac > 100", "loai",
         t((F.col("quang_duong_mile") <= 0) | (F.col("quang_duong_mile") > 100), True)),
        ("R4_van_toc_tren_80", "van_toc_tb_mph > 80", "loai",
         t(F.col("van_toc_tb_mph") > 80, False)),
        ("R5_so_khach_0_hoac_tren_8", "so_khach = 0 hoac > 8", "loai",
         t((F.col("so_khach") == 0) | (F.col("so_khach") > 8), False)),
        (MA_QUARANTINE, "vung_don hoac vung_tra khong thuoc 1-263", "quarantine",
         t(~(F.col("vung_don").between(1, VUNG_MAX) & F.col("vung_tra").between(1, VUNG_MAX)), True)),
        ("R7_tong_tien_am", "tong_tien < 0", "loai",
         t(F.col("tong_tien") < 0, False)),
    ]


def gan_ly_do(df, quy_tac):
    """Mỗi dòng gắn vào quy tắc đầu tiên bị vi phạm (theo thứ tự đề cương); NULL nghĩa là sạch."""
    ly_do = F.lit(None).cast("string")
    for ma, _, _, cond in reversed(quy_tac):
        ly_do = F.when(cond, F.lit(ma)).otherwise(ly_do)
    return df.withColumn("ly_do", ly_do)


def them_cot_phan_vung(df):
    return (df.withColumn("nam", F.year("thoi_diem_don"))
              .withColumn("thang", F.month("thoi_diem_don"))
              .withColumn("ngay", F.to_date("thoi_diem_don")))


def tao_silver_neu_chua_co(spark):
    """nam, thang, ngay là generated column từ thoi_diem_don; phân vùng theo nam, thang."""
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        return False
    (DeltaTable.createIfNotExists(spark).location(SILVER_PATH)
        .addColumn("ma_chuyen", "STRING")
        .addColumn("thoi_diem_don", "TIMESTAMP")
        .addColumn("thoi_diem_tra", "TIMESTAMP")
        .addColumn("vung_don", "INT")
        .addColumn("vung_tra", "INT")
        .addColumn("so_khach", "INT")
        .addColumn("quang_duong_mile", "DOUBLE")
        .addColumn("thoi_gian_phut", "DOUBLE")
        .addColumn("van_toc_tb_mph", "DOUBLE")
        .addColumn("tong_tien", "DOUBLE")
        .addColumn("hinh_thuc_tt", "STRING")
        .addColumn("nam", "INT", generatedAlwaysAs="YEAR(`thoi_diem_don`)")
        .addColumn("thang", "INT", generatedAlwaysAs="MONTH(`thoi_diem_don`)")
        .addColumn("ngay", "DATE", generatedAlwaysAs="CAST(`thoi_diem_don` AS DATE)")
        .partitionedBy("nam", "thang")
        .execute())
    print(f"[SILVER] đã tạo bảng mới tại {SILVER_PATH}")
    return True


def xoa_phan_vung(spark, path, nam, thang):
    if DeltaTable.isDeltaTable(spark, path):
        DeltaTable.forPath(spark, path).delete(f"nam = {nam} AND thang = {thang}")


def merge_vao_silver(spark, silver_df):
    """MERGE chỉ chèn theo ma_chuyen. Trả về số dòng chèn (0 nếu Delta không tạo phiên bản mới)."""
    bang = DeltaTable.forPath(spark, SILVER_PATH)
    version_truoc = bang.history(1).select("version").first()[0]
    (bang.alias("t")
         .merge(silver_df.alias("s"), "t.ma_chuyen = s.ma_chuyen")
         .whenNotMatchedInsertAll()
         .execute())
    ls = bang.history(1).select("version", "operationMetrics").first()
    if ls["version"] == version_truoc:
        return 0
    return int(ls["operationMetrics"].get("numTargetRowsInserted", 0))


def ghi_quarantine(spark, flagged, nam, thang):
    """Ghi các bản ghi vùng ngoài 1-263 (xóa phân vùng cũ trước để chạy lại không nhân đôi)."""
    q = (them_cot_phan_vung(flagged.filter(F.col("ly_do") == MA_QUARANTINE))
         .withColumnRenamed("ly_do", "ly_do_loai")
         .withColumn("loai_luc", F.current_timestamp()))
    xoa_phan_vung(spark, QUARANTINE_PATH, nam, thang)
    q.write.format("delta").mode("append").partitionBy("nam", "thang").save(QUARANTINE_PATH)


def tinh_thong_ke(flagged, quy_tac):
    """Một lượt quét: tổng dòng, số vi phạm độc lập từng quy tắc, số dòng bị loại theo thứ tự."""
    exprs = [F.count(F.lit(1)).alias("tong")]
    for ma, _, _, cond in quy_tac:
        exprs.append(F.sum(F.when(cond, 1).otherwise(0)).alias(ma))
    doc_lap = flagged.agg(*exprs).first().asDict()

    theo_thu_tu = {r["ly_do"]: r["count"]
                   for r in flagged.groupBy("ly_do").count().collect()}
    return doc_lap, theo_thu_tu


def ghi_thong_ke(spark, nam, thang, quy_tac, doc_lap, theo_thu_tu):
    tong = int(doc_lap["tong"])
    so_sach = int(theo_thu_tu.get(None, 0))
    so_quar = int(theo_thu_tu.get(MA_QUARANTINE, 0))
    so_loai = tong - so_sach - so_quar

    hang = [(nam, thang, "DAU_VAO", "tong dong bronze cua phan vung", "-", tong, 0)]
    for ma, mo_ta, hanh_dong, _ in quy_tac:
        hang.append((nam, thang, ma, mo_ta, hanh_dong,
                     int(doc_lap[ma] or 0), int(theo_thu_tu.get(ma, 0))))
    hang += [
        (nam, thang, "TONG_LOAI", "tong dong bi loai (khong tinh quarantine)", "loai", so_loai, so_loai),
        (nam, thang, "VAO_QUARANTINE", "dong vao _quarantine", "quarantine", so_quar, so_quar),
        (nam, thang, "VAO_SILVER", "dong sach dua vao silver", "giu", so_sach, so_sach),
    ]
    df = (spark.createDataFrame(
              hang, "nam INT, thang INT, ma_quy_tac STRING, mo_ta STRING, hanh_dong STRING, "
                    "so_vi_pham_doc_lap LONG, so_ban_ghi_bi_loai LONG")
          .withColumn("chay_luc", F.current_timestamp()))
    xoa_phan_vung(spark, THONG_KE_PATH, nam, thang)
    df.write.format("delta").mode("append").save(THONG_KE_PATH)
    return tong, so_sach, so_quar, so_loai


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName(f"bronze_to_silver_{args.nam}_{args.thang:02d}")
        # Cùng cấu hình với nap_thang.py để nam/thang của silver khớp phân vùng của bronze
        .config("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    print(f"[INPUT] bronze phân vùng nam={args.nam}, thang={args.thang}")
    bronze = (spark.read.format("delta").load(BRONZE_PATH)
              .filter((F.col("nam") == args.nam) & (F.col("thang") == args.thang)))

    quy_tac = dieu_kien_quy_tac()
    flagged = gan_ly_do(chuan_hoa(bronze), quy_tac).persist(StorageLevel.MEMORY_AND_DISK)

    doc_lap, theo_thu_tu = tinh_thong_ke(flagged, quy_tac)
    if int(doc_lap["tong"]) == 0:
        print("[CANH BAO] phân vùng bronze rỗng, không có gì để xử lý.")
        flagged.unpersist()
        spark.stop()
        return

    if args.lam_lai:
        print("[LAM LAI] xóa phân vùng cũ ở silver trước khi tính lại")
        xoa_phan_vung(spark, SILVER_PATH, args.nam, args.thang)

    tao_silver_neu_chua_co(spark)
    silver_df = them_cot_phan_vung(
        flagged.filter(F.col("ly_do").isNull()).drop("ly_do"))
    so_chen = merge_vao_silver(spark, silver_df)

    ghi_quarantine(spark, flagged, args.nam, args.thang)
    tong, so_sach, so_quar, so_loai = ghi_thong_ke(
        spark, args.nam, args.thang, quy_tac, doc_lap, theo_thu_tu)
    flagged.unpersist()

    print("=" * 72)
    print(f"[THONG KE] phân vùng {args.nam}-{args.thang:02d}: {tong:,} dòng đầu vào")
    for ma, mo_ta, hanh_dong, _ in quy_tac:
        print(f"  {ma:<30} bị loại theo thứ tự: {int(theo_thu_tu.get(ma, 0)):>10,}"
              f"   vi phạm độc lập: {int(doc_lap[ma] or 0):>10,}   [{hanh_dong}]")
    print(f"[KET QUA] vào silver      : {so_sach:,} (chèn lần này: {so_chen:,})")
    print(f"[KET QUA] vào quarantine  : {so_quar:,}")
    print(f"[KET QUA] bị loại         : {so_loai:,}")
    if so_sach + so_quar + so_loai != tong:
        print("[LOI] tổng không khớp đầu vào, kiểm tra lại quy tắc!")
    print("=" * 72)
    spark.stop()


if __name__ == "__main__":
    main()