select count(*) from mock_data;
select count(*) from fact_sales;
select
    (select sum(sale_total_price) from mock_data),
    (select sum(sale_total_price) from fact_sales);
select
    c.country,
    pr.product_name,
    sum(f.sale_quantity) as total_qty,
    round(sum(f.sale_total_price), 2) as total_sum
from fact_sales f
join dim_customer c on c.customer_key = f.customer_key
join dim_product pr on pr.product_key = f.product_key
group by c.country, pr.product_name
order by total_sum desc
limit 10;