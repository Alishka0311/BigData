select count(*) as mock_data_count from mock_data;
select count(*) as fact_sales_count from fact_sales;
select
    (select sum(sale_total_price) from mock_data) as raw_sum,
    (select sum(sale_total_price) from fact_sales) as fact_sum;
select
    (select count(*) from fact_sales where customer_key is null) as customer_nulls,
    (select count(*) from fact_sales where seller_key is null) as seller_nulls,
    (select count(*) from fact_sales where product_key is null) as product_nulls,
    (select count(*) from fact_sales where store_key is null) as store_nulls,
    (select count(*) from fact_sales where supplier_key is null) as supplier_nulls;
