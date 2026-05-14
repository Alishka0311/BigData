INSERT INTO clickhouse.bdtrino_analytics.sales_product_report
SELECT
    CAST(row_number() OVER (ORDER BY total_revenue DESC, product_key) AS decimal(20, 0)) AS sales_rank,
    product_key,
    to_utf8(from_utf8(product_name)) AS product_name,
    to_utf8(from_utf8(category_name)) AS category_name,
    total_revenue,
    total_quantity,
    average_rating,
    total_reviews
FROM (
    SELECT
        p.product_key,
        p.product_name,
        pc.category_name,
    CAST(sum(f.sale_total_price) AS decimal(14, 2)) AS total_revenue,
    CAST(sum(f.sale_quantity) AS bigint) AS total_quantity,
        CAST(avg(p.product_rating) AS decimal(4, 2)) AS average_rating,
        CAST(max(p.product_reviews) AS bigint) AS total_reviews
    FROM clickhouse.bdtrino_analytics.fact_sales f
    JOIN clickhouse.bdtrino_analytics.dim_product p
        ON p.product_key = f.product_key
    LEFT JOIN clickhouse.bdtrino_analytics.dim_product_category pc
        ON pc.product_category_key = p.product_category_key
    GROUP BY p.product_key, p.product_name, pc.category_name
);

INSERT INTO clickhouse.bdtrino_analytics.sales_customer_report
SELECT
    CAST(row_number() OVER (ORDER BY total_revenue DESC, customer_key) AS decimal(20, 0)) AS customer_rank,
    customer_key,
    to_utf8(customer_full_name) AS customer_full_name,
    to_utf8(from_utf8(country)) AS country,
    total_revenue,
    orders_count,
    average_check
FROM (
    SELECT
        c.customer_key,
        trim(
            concat(
                COALESCE(from_utf8(c.first_name), ''),
                ' ',
                COALESCE(from_utf8(c.last_name), '')
            )
        ) AS customer_full_name,
        c.country,
        CAST(sum(f.sale_total_price) AS decimal(14, 2)) AS total_revenue,
        CAST(count(*) AS bigint) AS orders_count,
        CAST(avg(f.sale_total_price) AS decimal(14, 2)) AS average_check
    FROM clickhouse.bdtrino_analytics.fact_sales f
    JOIN clickhouse.bdtrino_analytics.dim_customer c
        ON c.customer_key = f.customer_key
    GROUP BY c.customer_key, c.first_name, c.last_name, c.country
);

INSERT INTO clickhouse.bdtrino_analytics.sales_time_report
SELECT
    CAST(year(sale_date) AS integer) AS period_year,
    CAST(month(sale_date) AS integer) AS period_month,
    to_utf8(format('%04d-%02d', year(sale_date), month(sale_date))) AS year_month,
    CAST(count(*) AS bigint) AS orders_count,
    CAST(sum(sale_total_price) AS decimal(14, 2)) AS total_revenue,
    CAST(avg(sale_total_price) AS decimal(14, 2)) AS average_order_value
FROM clickhouse.bdtrino_analytics.fact_sales
GROUP BY year(sale_date), month(sale_date);

INSERT INTO clickhouse.bdtrino_analytics.sales_store_report
SELECT
    CAST(row_number() OVER (ORDER BY total_revenue DESC, store_key) AS decimal(20, 0)) AS store_rank,
    store_key,
    to_utf8(from_utf8(store_name)) AS store_name,
    to_utf8(from_utf8(store_city)) AS store_city,
    to_utf8(from_utf8(store_country)) AS store_country,
    total_revenue,
    orders_count,
    average_check
FROM (
    SELECT
        s.store_key,
        s.store_name,
        s.store_city,
        s.store_country,
        CAST(sum(f.sale_total_price) AS decimal(14, 2)) AS total_revenue,
        CAST(count(*) AS bigint) AS orders_count,
        CAST(avg(f.sale_total_price) AS decimal(14, 2)) AS average_check
    FROM clickhouse.bdtrino_analytics.fact_sales f
    JOIN clickhouse.bdtrino_analytics.dim_store s
        ON s.store_key = f.store_key
    GROUP BY s.store_key, s.store_name, s.store_city, s.store_country
);

INSERT INTO clickhouse.bdtrino_analytics.sales_supplier_report
SELECT
    CAST(row_number() OVER (ORDER BY total_revenue DESC, supplier_key) AS decimal(20, 0)) AS supplier_rank,
    supplier_key,
    to_utf8(from_utf8(supplier_name)) AS supplier_name,
    to_utf8(from_utf8(supplier_country)) AS supplier_country,
    total_revenue,
    average_product_price,
    products_count
FROM (
    SELECT
        s.supplier_key,
        s.supplier_name,
        s.supplier_country,
        CAST(sum(f.sale_total_price) AS decimal(14, 2)) AS total_revenue,
        CAST(avg(p.product_price) AS decimal(14, 2)) AS average_product_price,
        CAST(count(DISTINCT p.product_key) AS bigint) AS products_count
    FROM clickhouse.bdtrino_analytics.fact_sales f
    JOIN clickhouse.bdtrino_analytics.dim_supplier s
        ON s.supplier_key = f.supplier_key
    JOIN clickhouse.bdtrino_analytics.dim_product p
        ON p.product_key = f.product_key
    GROUP BY s.supplier_key, s.supplier_name, s.supplier_country
);

INSERT INTO clickhouse.bdtrino_analytics.product_quality_report
SELECT
    CAST(row_number() OVER (ORDER BY product_rating DESC, total_quantity_sold DESC, product_key) AS decimal(20, 0)) AS quality_rank,
    product_key,
    to_utf8(from_utf8(product_name)) AS product_name,
    product_rating,
    product_reviews,
    total_quantity_sold,
    total_revenue,
    to_utf8(rating_band) AS rating_band
FROM (
    SELECT
        p.product_key,
        p.product_name,
        p.product_rating,
        p.product_reviews,
        CAST(sum(f.sale_quantity) AS bigint) AS total_quantity_sold,
        CAST(sum(f.sale_total_price) AS decimal(14, 2)) AS total_revenue,
        CASE
            WHEN p.product_rating >= DECIMAL '4.5' THEN 'excellent'
            WHEN p.product_rating >= DECIMAL '3.5' THEN 'good'
            WHEN p.product_rating >= DECIMAL '2.5' THEN 'average'
            ELSE 'low'
        END AS rating_band
    FROM clickhouse.bdtrino_analytics.fact_sales f
    JOIN clickhouse.bdtrino_analytics.dim_product p
        ON p.product_key = f.product_key
    GROUP BY p.product_key, p.product_name, p.product_rating, p.product_reviews
);
