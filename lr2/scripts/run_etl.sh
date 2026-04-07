#!/bin/bash
set -euo pipefail

docker compose exec spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.3 \
  /opt/project/lr2/jobs/etl_to_postgres.py
