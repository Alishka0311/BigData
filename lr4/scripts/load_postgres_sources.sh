#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

POSTGRES_FILES=(
  "MOCK_DATA (5).csv"
  "MOCK_DATA (6).csv"
  "MOCK_DATA (7).csv"
  "MOCK_DATA (8).csv"
  "MOCK_DATA (9).csv"
)

docker exec lr4-postgres psql -U postgres -d bdtrino_source -c "TRUNCATE TABLE mock_data;"

for file_name in "${POSTGRES_FILES[@]}"; do
  echo "Loading into PostgreSQL: $file_name"
  docker exec lr4-postgres psql -U postgres -d bdtrino_source -c "\copy mock_data FROM '/data/${file_name}' WITH (FORMAT csv, HEADER true, QUOTE '\"', ESCAPE '\"')"
done

docker exec lr4-postgres psql -U postgres -d bdtrino_source -c "SELECT count(*) AS postgres_rows FROM mock_data;"
