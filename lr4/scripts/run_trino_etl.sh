#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Waiting for Trino to become ready..."
for _ in $(seq 1 60); do
  if docker exec lr4-trino trino --server http://localhost:8080 --execute "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

docker exec -i lr4-clickhouse clickhouse-client --user default --password clickhouse --multiquery < "$ROOT_DIR/sql/03_clickhouse_reset_analytics.sql"

docker exec -i lr4-trino trino --server http://localhost:8080 --file /opt/project/sql/trino/01_build_snowflake.sql
