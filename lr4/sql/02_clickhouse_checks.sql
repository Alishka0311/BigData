SELECT count() AS clickhouse_source_rows
FROM bdtrino_source.mock_data;

SELECT count() AS fact_sales_rows
FROM bdtrino_analytics.fact_sales;

SELECT count() AS sales_product_report_rows
FROM bdtrino_analytics.sales_product_report;

SELECT count() AS sales_customer_report_rows
FROM bdtrino_analytics.sales_customer_report;

SELECT count() AS sales_time_report_rows
FROM bdtrino_analytics.sales_time_report;

SELECT count() AS sales_store_report_rows
FROM bdtrino_analytics.sales_store_report;

SELECT count() AS sales_supplier_report_rows
FROM bdtrino_analytics.sales_supplier_report;

SELECT count() AS product_quality_report_rows
FROM bdtrino_analytics.product_quality_report;

SELECT *
FROM bdtrino_analytics.sales_product_report
ORDER BY sales_rank
LIMIT 10;
