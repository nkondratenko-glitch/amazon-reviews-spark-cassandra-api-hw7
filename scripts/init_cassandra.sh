#!/usr/bin/env bash
set -euo pipefail

docker compose up -d cassandra redis

echo "Waiting for Cassandra..."
until docker exec hw7-cassandra cqlsh -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; do
  sleep 5
  echo "Still waiting..."
done

docker exec -i hw7-cassandra cqlsh < cassandra/schema.cql
echo "Cassandra schema initialized."
