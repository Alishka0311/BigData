#!/usr/bin/env bash
set -euo pipefail

JAR_PATH="/opt/flink/usrlib/lr3-flink-job-1.0.0.jar"

docker compose exec \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:19092 \
  -e KAFKA_TOPIC=petshop.sales.raw \
  -e POSTGRES_JDBC_URL=jdbc:postgresql://postgres:5432/pet_shop \
  -e POSTGRES_USER=pet_user \
  -e POSTGRES_PASSWORD=pet_password \
  jobmanager \
  flink run -d "$JAR_PATH"
