CREATE DATABASE IF NOT EXISTS bdtrino_source;
CREATE DATABASE IF NOT EXISTS bdtrino_analytics;

CREATE TABLE IF NOT EXISTS bdtrino_source.mock_data
(
    id String,
    customer_first_name String,
    customer_last_name String,
    customer_age String,
    customer_email String,
    customer_country String,
    customer_postal_code String,
    customer_pet_type String,
    customer_pet_name String,
    customer_pet_breed String,
    seller_first_name String,
    seller_last_name String,
    seller_email String,
    seller_country String,
    seller_postal_code String,
    product_name String,
    product_category String,
    product_price String,
    product_quantity String,
    sale_date String,
    sale_customer_id String,
    sale_seller_id String,
    sale_product_id String,
    sale_quantity String,
    sale_total_price String,
    store_name String,
    store_location String,
    store_city String,
    store_state String,
    store_country String,
    store_phone String,
    store_email String,
    pet_category String,
    product_weight String,
    product_color String,
    product_size String,
    product_brand String,
    product_material String,
    product_description String,
    product_rating String,
    product_reviews String,
    product_release_date String,
    product_expiry_date String,
    supplier_name String,
    supplier_contact String,
    supplier_email String,
    supplier_phone String,
    supplier_address String,
    supplier_city String,
    supplier_country String
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS bdtrino_analytics.dim_product_category
(
    product_category_key UInt64,
    category_name Nullable(String)
)
ENGINE = MergeTree
ORDER BY product_category_key;

CREATE TABLE IF NOT EXISTS bdtrino_analytics.dim_brand
(
    brand_key UInt64,
    brand_name Nullable(String)
)
ENGINE = MergeTree
ORDER BY brand_key;

CREATE TABLE IF NOT EXISTS bdtrino_analytics.dim_material
(
    material_key UInt64,
    material_name Nullable(String)
)
ENGINE = MergeTree
ORDER BY material_key;

CREATE TABLE IF NOT EXISTS bdtrino_analytics.dim_customer
(
    customer_key UInt64,
    customer_source_id Int64,
    first_name Nullable(String),
    last_name Nullable(String),
    age Nullable(Int32),
    email Nullable(String),
    country Nullable(String),
    postal_code Nullable(String)
)
ENGINE = MergeTree
ORDER BY customer_key;

CREATE TABLE IF NOT EXISTS bdtrino_analytics.dim_pet
(
    pet_key UInt64,
    customer_source_id Int64,
    pet_type Nullable(String),
    pet_name Nullable(String),
    pet_breed Nullable(String),
    pet_category Nullable(String)
)
ENGINE = MergeTree
ORDER BY pet_key;

CREATE TABLE IF NOT EXISTS bdtrino_analytics.dim_seller
(
    seller_key UInt64,
    seller_source_id Int64,
    first_name Nullable(String),
    last_name Nullable(String),
    email Nullable(String),
    country Nullable(String),
    postal_code Nullable(String)
)
ENGINE = MergeTree
ORDER BY seller_key;

CREATE TABLE IF NOT EXISTS bdtrino_analytics.dim_product
(
    product_key UInt64,
    product_source_id Int64,
    product_name Nullable(String),
    product_category_key Nullable(UInt64),
    brand_key Nullable(UInt64),
    material_key Nullable(UInt64),
    product_price Decimal(12, 2),
    product_quantity Int32,
    product_weight Decimal(12, 2),
    product_color Nullable(String),
    product_size Nullable(String),
    product_description Nullable(String),
    product_rating Decimal(4, 2),
    product_reviews Int32,
    product_release_date Date,
    product_expiry_date Date
)
ENGINE = MergeTree
ORDER BY product_key;

CREATE TABLE IF NOT EXISTS bdtrino_analytics.dim_store
(
    store_key UInt64,
    store_name Nullable(String),
    store_location Nullable(String),
    store_city Nullable(String),
    store_state Nullable(String),
    store_country Nullable(String),
    store_phone Nullable(String),
    store_email Nullable(String)
)
ENGINE = MergeTree
ORDER BY store_key;

CREATE TABLE IF NOT EXISTS bdtrino_analytics.dim_supplier
(
    supplier_key UInt64,
    supplier_name Nullable(String),
    supplier_contact Nullable(String),
    supplier_email Nullable(String),
    supplier_phone Nullable(String),
    supplier_address Nullable(String),
    supplier_city Nullable(String),
    supplier_country Nullable(String)
)
ENGINE = MergeTree
ORDER BY supplier_key;

CREATE TABLE IF NOT EXISTS bdtrino_analytics.fact_sales
(
    sale_key UInt64,
    sale_source_row_id Int64,
    sale_date Date,
    customer_key Nullable(UInt64),
    pet_key Nullable(UInt64),
    seller_key Nullable(UInt64),
    product_key Nullable(UInt64),
    store_key Nullable(UInt64),
    supplier_key Nullable(UInt64),
    sale_quantity Int32,
    sale_total_price Decimal(12, 2)
)
ENGINE = MergeTree
ORDER BY sale_key;

CREATE TABLE IF NOT EXISTS bdtrino_analytics.sales_product_report
(
    sales_rank UInt64,
    product_key UInt64,
    product_name Nullable(String),
    category_name Nullable(String),
    total_revenue Decimal(14, 2),
    total_quantity Int64,
    average_rating Decimal(4, 2),
    total_reviews Int64
)
ENGINE = MergeTree
ORDER BY sales_rank;

CREATE TABLE IF NOT EXISTS bdtrino_analytics.sales_customer_report
(
    customer_rank UInt64,
    customer_key UInt64,
    customer_full_name Nullable(String),
    country Nullable(String),
    total_revenue Decimal(14, 2),
    orders_count Int64,
    average_check Decimal(14, 2)
)
ENGINE = MergeTree
ORDER BY customer_rank;

CREATE TABLE IF NOT EXISTS bdtrino_analytics.sales_time_report
(
    period_year Int32,
    period_month Int32,
    year_month String,
    orders_count Int64,
    total_revenue Decimal(14, 2),
    average_order_value Decimal(14, 2)
)
ENGINE = MergeTree
ORDER BY (period_year, period_month);

CREATE TABLE IF NOT EXISTS bdtrino_analytics.sales_store_report
(
    store_rank UInt64,
    store_key UInt64,
    store_name Nullable(String),
    store_city Nullable(String),
    store_country Nullable(String),
    total_revenue Decimal(14, 2),
    orders_count Int64,
    average_check Decimal(14, 2)
)
ENGINE = MergeTree
ORDER BY store_rank;

CREATE TABLE IF NOT EXISTS bdtrino_analytics.sales_supplier_report
(
    supplier_rank UInt64,
    supplier_key UInt64,
    supplier_name Nullable(String),
    supplier_country Nullable(String),
    total_revenue Decimal(14, 2),
    average_product_price Decimal(14, 2),
    products_count Int64
)
ENGINE = MergeTree
ORDER BY supplier_rank;

CREATE TABLE IF NOT EXISTS bdtrino_analytics.product_quality_report
(
    quality_rank UInt64,
    product_key UInt64,
    product_name Nullable(String),
    product_rating Decimal(4, 2),
    product_reviews Int32,
    total_quantity_sold Int64,
    total_revenue Decimal(14, 2),
    rating_band String
)
ENGINE = MergeTree
ORDER BY quality_rank;
