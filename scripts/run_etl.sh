#!/usr/bin/env bash
set -euo pipefail

CSV_PATH=${1:-/mnt/data/amazon_reviews\(1\).csv}
MONGO_URI=${MONGO_URI:-mongodb://root:example@localhost:27017}
DB_NAME=${DB_NAME:-amazon_reviews_hw}

spark-submit \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  src/amazon_reviews_spark_mongo.py \
  --input "$CSV_PATH" \
  --mongo-uri "$MONGO_URI" \
  --db "$DB_NAME"
