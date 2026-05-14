#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

docker compose up -d
"$ROOT_DIR/scripts/init_sources.sh"
"$ROOT_DIR/scripts/run_trino_etl.sh"
"$ROOT_DIR/scripts/run_trino_reports.sh"
