#!/bin/bash
# Backfill cac thang con thieu trong bronze.chuyen_di - tu dong bo qua thang da co

check_and_ingest() {
  local nam=$1
  local thang=$2
  local m=$((10#$thang))
  local hdfs_path="/lakehouse/bd04_giao_thong/bronze/chuyen_di/nam=${nam}/thang=${m}"

  if hdfs dfs -test -d "$hdfs_path" 2>/dev/null; then
    echo "[SKIP] ${nam}-${thang} da co, bo qua"
    return 0
  fi

  echo "=== Ingest ${nam}-${thang} ==="
  spark-submit --master yarn \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    --conf spark.sql.shuffle.partitions=16 \
    --num-executors 2 --executor-memory 1g --executor-cores 1 \
    --conf spark.executor.memoryOverhead=512m \
    --conf spark.yarn.am.memory=768m \
    spark_jobs/ingest/nap_thang.py --nam $nam --thang $m \
    > ~/logs/backfill_2022/${nam}-${thang}.log 2>&1

  if [ $? -ne 0 ]; then
    echo "!!! LOI o ${nam}-${thang}, dung lai toan bo"
    exit 1
  fi
  echo "[OK] ${nam}-${thang} xong"
}

for nam in 2022 2023 2024 2025; do
  for thang in $(seq -w 1 12); do
    check_and_ingest $nam $thang
  done
done

echo "BACKFILL_ALL_DONE"