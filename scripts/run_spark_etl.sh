#!/usr/bin/env bash
set -euo pipefail

if [ ! -f "data/amazon_reviews.csv" ]; then
  echo "Put your dataset at data/amazon_reviews.csv first."
  exit 1
fi

docker compose up -d cassandra redis spark
./scripts/init_cassandra.sh

docker compose exec spark spark-submit \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  /app/src/spark/load_to_cassandra.py
