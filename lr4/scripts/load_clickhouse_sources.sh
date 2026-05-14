#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CLICKHOUSE_FILES=(
  "MOCK_DATA.csv"
  "MOCK_DATA (1).csv"
  "MOCK_DATA (2).csv"
  "MOCK_DATA (3).csv"
  "MOCK_DATA (4).csv"
)

docker exec lr4-clickhouse clickhouse-client --user default --password clickhouse --query "TRUNCATE TABLE bdtrino_source.mock_data"

for file_name in "${CLICKHOUSE_FILES[@]}"; do
  echo "Loading into ClickHouse: $file_name"
  docker exec lr4-clickhouse bash -lc "cat '/data/${file_name}' | clickhouse-client --user default --password clickhouse --query=\"INSERT INTO bdtrino_source.mock_data FORMAT CSVWithNames\""
done

docker exec lr4-clickhouse clickhouse-client --user default --password clickhouse --query "SELECT count() AS clickhouse_rows FROM bdtrino_source.mock_data"
