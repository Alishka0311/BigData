SELECT count() AS rows_count FROM bdspark_reports.sales_product_report;
SELECT count() AS rows_count FROM bdspark_reports.sales_customer_report;
SELECT count() AS rows_count FROM bdspark_reports.sales_time_report;
SELECT count() AS rows_count FROM bdspark_reports.sales_store_report;
SELECT count() AS rows_count FROM bdspark_reports.sales_supplier_report;
SELECT count() AS rows_count FROM bdspark_reports.product_quality_report;

SELECT *
FROM bdspark_reports.sales_product_report
ORDER BY sales_rank
LIMIT 10;

SELECT *
FROM bdspark_reports.sales_store_report
ORDER BY store_rank
LIMIT 5;
