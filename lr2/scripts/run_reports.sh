#!/bin/bash
set -euo pipefail

docker exec -i bd_spark_clickhouse clickhouse-client \
  --user default \
  --password clickhouse \
  --multiquery <<'SQL'
CREATE DATABASE IF NOT EXISTS bdspark_reports;

CREATE TABLE IF NOT EXISTS bdspark_reports.sales_product_report
(
    product_key Int64,
    product_name Nullable(String),
    product_category Nullable(String),
    total_quantity Int64,
    total_revenue Decimal(18, 2),
    avg_rating Nullable(Decimal(18, 2)),
    reviews_count Nullable(Int64),
    sales_count Int64,
    sales_rank Int64
)
ENGINE = MergeTree
ORDER BY (sales_rank, product_key);

CREATE TABLE IF NOT EXISTS bdspark_reports.sales_customer_report
(
    customer_key Int64,
    customer_first_name Nullable(String),
    customer_last_name Nullable(String),
    customer_country Nullable(String),
    total_revenue Decimal(18, 2),
    orders_count Int64,
    avg_check Decimal(18, 2),
    customer_rank Int64
)
ENGINE = MergeTree
ORDER BY (customer_rank, customer_key);

CREATE TABLE IF NOT EXISTS bdspark_reports.sales_time_report
(
    sale_year Int32,
    sale_month Int32,
    period Nullable(String),
    total_revenue Decimal(18, 2),
    orders_count Int64,
    avg_order_value Decimal(18, 2)
)
ENGINE = MergeTree
ORDER BY (sale_year, sale_month);

CREATE TABLE IF NOT EXISTS bdspark_reports.sales_store_report
(
    store_key Int64,
    store_name Nullable(String),
    store_city Nullable(String),
    store_state Nullable(String),
    store_country Nullable(String),
    total_revenue Decimal(18, 2),
    orders_count Int64,
    avg_check Decimal(18, 2),
    store_rank Int64
)
ENGINE = MergeTree
ORDER BY (store_rank, store_key);

CREATE TABLE IF NOT EXISTS bdspark_reports.sales_supplier_report
(
    supplier_key Int64,
    supplier_name Nullable(String),
    supplier_country Nullable(String),
    total_revenue Decimal(18, 2),
    avg_product_price Nullable(Decimal(18, 2)),
    total_quantity Int64,
    supplier_rank Int64
)
ENGINE = MergeTree
ORDER BY (supplier_rank, supplier_key);

CREATE TABLE IF NOT EXISTS bdspark_reports.product_quality_report
(
    product_key Int64,
    product_name Nullable(String),
    product_category Nullable(String),
    product_rating Nullable(Decimal(18, 2)),
    product_reviews Nullable(Int64),
    total_quantity Int64,
    total_revenue Decimal(18, 2),
    rating_rank_desc Int64,
    rating_rank_asc Int64,
    reviews_rank Int64
)
ENGINE = MergeTree
ORDER BY (rating_rank_desc, product_key);

TRUNCATE TABLE bdspark_reports.sales_product_report;
TRUNCATE TABLE bdspark_reports.sales_customer_report;
TRUNCATE TABLE bdspark_reports.sales_time_report;
TRUNCATE TABLE bdspark_reports.sales_store_report;
TRUNCATE TABLE bdspark_reports.sales_supplier_report;
TRUNCATE TABLE bdspark_reports.product_quality_report;
SQL

docker compose exec spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.3 \
  /opt/project/lr2/jobs/build_clickhouse_reports.py
