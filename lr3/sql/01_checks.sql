select 'dim_customer' as table_name, count(*) as row_count from dim_customer
union all
select 'dim_pet', count(*) from dim_pet
union all
select 'dim_seller', count(*) from dim_seller
union all
select 'dim_product', count(*) from dim_product
union all
select 'dim_store', count(*) from dim_store
union all
select 'dim_supplier', count(*) from dim_supplier
union all
select 'fact_sales', count(*) from fact_sales
order by table_name;

select
    count(*) as fact_rows,
    count(*) filter (where customer_key is null) as customer_nulls,
    count(*) filter (where pet_key is null) as pet_nulls,
    count(*) filter (where seller_key is null) as seller_nulls,
    count(*) filter (where product_key is null) as product_nulls,
    count(*) filter (where store_key is null) as store_nulls,
    count(*) filter (where supplier_key is null) as supplier_nulls
from fact_sales;

select round(sum(sale_total_price), 2) as total_sales_sum
from fact_sales;
