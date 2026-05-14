INSERT INTO clickhouse.bdtrino_analytics.dim_product_category
WITH source_data AS (
    SELECT from_utf8(product_category) AS product_category
    FROM clickhouse.bdtrino_source.mock_data
    UNION ALL
    SELECT CAST(product_category AS varchar) AS product_category
    FROM postgresql.public.mock_data
)
SELECT
    CAST(row_number() OVER (ORDER BY category_name) AS decimal(20, 0)) AS product_category_key,
    to_utf8(category_name) AS category_name
FROM (
    SELECT DISTINCT NULLIF(trim(product_category), '') AS category_name
    FROM source_data
)
WHERE category_name IS NOT NULL;

INSERT INTO clickhouse.bdtrino_analytics.dim_brand
WITH source_data AS (
    SELECT from_utf8(product_brand) AS product_brand
    FROM clickhouse.bdtrino_source.mock_data
    UNION ALL
    SELECT CAST(product_brand AS varchar) AS product_brand
    FROM postgresql.public.mock_data
)
SELECT
    CAST(row_number() OVER (ORDER BY brand_name) AS decimal(20, 0)) AS brand_key,
    to_utf8(brand_name) AS brand_name
FROM (
    SELECT DISTINCT NULLIF(trim(product_brand), '') AS brand_name
    FROM source_data
)
WHERE brand_name IS NOT NULL;

INSERT INTO clickhouse.bdtrino_analytics.dim_material
WITH source_data AS (
    SELECT from_utf8(product_material) AS product_material
    FROM clickhouse.bdtrino_source.mock_data
    UNION ALL
    SELECT CAST(product_material AS varchar) AS product_material
    FROM postgresql.public.mock_data
)
SELECT
    CAST(row_number() OVER (ORDER BY material_name) AS decimal(20, 0)) AS material_key,
    to_utf8(material_name) AS material_name
FROM (
    SELECT DISTINCT NULLIF(trim(product_material), '') AS material_name
    FROM source_data
)
WHERE material_name IS NOT NULL;

INSERT INTO clickhouse.bdtrino_analytics.dim_customer
WITH source_data AS (
    SELECT
        from_utf8(sale_customer_id) AS sale_customer_id,
        from_utf8(customer_first_name) AS customer_first_name,
        from_utf8(customer_last_name) AS customer_last_name,
        from_utf8(customer_age) AS customer_age,
        from_utf8(customer_email) AS customer_email,
        from_utf8(customer_country) AS customer_country,
        from_utf8(customer_postal_code) AS customer_postal_code,
        from_utf8(id) AS id
    FROM clickhouse.bdtrino_source.mock_data
    UNION ALL
    SELECT
        CAST(sale_customer_id AS varchar) AS sale_customer_id,
        CAST(customer_first_name AS varchar) AS customer_first_name,
        CAST(customer_last_name AS varchar) AS customer_last_name,
        CAST(customer_age AS varchar) AS customer_age,
        CAST(customer_email AS varchar) AS customer_email,
        CAST(customer_country AS varchar) AS customer_country,
        CAST(customer_postal_code AS varchar) AS customer_postal_code,
        CAST(id AS varchar) AS id
    FROM postgresql.public.mock_data
),
clean_data AS (
    SELECT
        try_cast(NULLIF(trim(sale_customer_id), '') AS bigint) AS sale_customer_id,
        NULLIF(trim(customer_first_name), '') AS customer_first_name,
        NULLIF(trim(customer_last_name), '') AS customer_last_name,
        try_cast(NULLIF(trim(customer_age), '') AS integer) AS customer_age,
        NULLIF(trim(customer_email), '') AS customer_email,
        NULLIF(trim(customer_country), '') AS customer_country,
        NULLIF(trim(customer_postal_code), '') AS customer_postal_code,
        try_cast(NULLIF(trim(id), '') AS bigint) AS row_id
    FROM source_data
),
ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY sale_customer_id ORDER BY row_id) AS rn
    FROM clean_data
    WHERE sale_customer_id IS NOT NULL
)
SELECT
    CAST(row_number() OVER (ORDER BY sale_customer_id) AS decimal(20, 0)) AS customer_key,
    sale_customer_id AS customer_source_id,
    to_utf8(customer_first_name) AS first_name,
    to_utf8(customer_last_name) AS last_name,
    customer_age AS age,
    to_utf8(customer_email) AS email,
    to_utf8(customer_country) AS country,
    to_utf8(customer_postal_code) AS postal_code
FROM ranked
WHERE rn = 1;

INSERT INTO clickhouse.bdtrino_analytics.dim_pet
WITH source_data AS (
    SELECT
        from_utf8(sale_customer_id) AS sale_customer_id,
        from_utf8(customer_pet_type) AS customer_pet_type,
        from_utf8(customer_pet_name) AS customer_pet_name,
        from_utf8(customer_pet_breed) AS customer_pet_breed,
        from_utf8(pet_category) AS pet_category
    FROM clickhouse.bdtrino_source.mock_data
    UNION ALL
    SELECT
        CAST(sale_customer_id AS varchar) AS sale_customer_id,
        CAST(customer_pet_type AS varchar) AS customer_pet_type,
        CAST(customer_pet_name AS varchar) AS customer_pet_name,
        CAST(customer_pet_breed AS varchar) AS customer_pet_breed,
        CAST(pet_category AS varchar) AS pet_category
    FROM postgresql.public.mock_data
),
clean_data AS (
    SELECT DISTINCT
        try_cast(NULLIF(trim(sale_customer_id), '') AS bigint) AS customer_source_id,
        NULLIF(trim(customer_pet_type), '') AS pet_type,
        NULLIF(trim(customer_pet_name), '') AS pet_name,
        NULLIF(trim(customer_pet_breed), '') AS pet_breed,
        NULLIF(trim(pet_category), '') AS pet_category
    FROM source_data
)
SELECT
    CAST(
        row_number() OVER (
            ORDER BY customer_source_id, pet_name, pet_type, pet_breed, pet_category
        ) AS decimal(20, 0)
    ) AS pet_key,
    customer_source_id,
    to_utf8(pet_type) AS pet_type,
    to_utf8(pet_name) AS pet_name,
    to_utf8(pet_breed) AS pet_breed,
    to_utf8(pet_category) AS pet_category
FROM clean_data
WHERE customer_source_id IS NOT NULL;

INSERT INTO clickhouse.bdtrino_analytics.dim_seller
WITH source_data AS (
    SELECT
        from_utf8(sale_seller_id) AS sale_seller_id,
        from_utf8(seller_first_name) AS seller_first_name,
        from_utf8(seller_last_name) AS seller_last_name,
        from_utf8(seller_email) AS seller_email,
        from_utf8(seller_country) AS seller_country,
        from_utf8(seller_postal_code) AS seller_postal_code,
        from_utf8(id) AS id
    FROM clickhouse.bdtrino_source.mock_data
    UNION ALL
    SELECT
        CAST(sale_seller_id AS varchar) AS sale_seller_id,
        CAST(seller_first_name AS varchar) AS seller_first_name,
        CAST(seller_last_name AS varchar) AS seller_last_name,
        CAST(seller_email AS varchar) AS seller_email,
        CAST(seller_country AS varchar) AS seller_country,
        CAST(seller_postal_code AS varchar) AS seller_postal_code,
        CAST(id AS varchar) AS id
    FROM postgresql.public.mock_data
),
clean_data AS (
    SELECT
        try_cast(NULLIF(trim(sale_seller_id), '') AS bigint) AS sale_seller_id,
        NULLIF(trim(seller_first_name), '') AS seller_first_name,
        NULLIF(trim(seller_last_name), '') AS seller_last_name,
        NULLIF(trim(seller_email), '') AS seller_email,
        NULLIF(trim(seller_country), '') AS seller_country,
        NULLIF(trim(seller_postal_code), '') AS seller_postal_code,
        try_cast(NULLIF(trim(id), '') AS bigint) AS row_id
    FROM source_data
),
ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY sale_seller_id ORDER BY row_id) AS rn
    FROM clean_data
    WHERE sale_seller_id IS NOT NULL
)
SELECT
    CAST(row_number() OVER (ORDER BY sale_seller_id) AS decimal(20, 0)) AS seller_key,
    sale_seller_id AS seller_source_id,
    to_utf8(seller_first_name) AS first_name,
    to_utf8(seller_last_name) AS last_name,
    to_utf8(seller_email) AS email,
    to_utf8(seller_country) AS country,
    to_utf8(seller_postal_code) AS postal_code
FROM ranked
WHERE rn = 1;

INSERT INTO clickhouse.bdtrino_analytics.dim_product
WITH source_data AS (
    SELECT
        from_utf8(sale_product_id) AS sale_product_id,
        from_utf8(product_name) AS product_name,
        from_utf8(product_category) AS product_category,
        from_utf8(product_price) AS product_price,
        from_utf8(product_quantity) AS product_quantity,
        from_utf8(product_weight) AS product_weight,
        from_utf8(product_color) AS product_color,
        from_utf8(product_size) AS product_size,
        from_utf8(product_brand) AS product_brand,
        from_utf8(product_material) AS product_material,
        from_utf8(product_description) AS product_description,
        from_utf8(product_rating) AS product_rating,
        from_utf8(product_reviews) AS product_reviews,
        from_utf8(product_release_date) AS product_release_date,
        from_utf8(product_expiry_date) AS product_expiry_date,
        from_utf8(id) AS id
    FROM clickhouse.bdtrino_source.mock_data
    UNION ALL
    SELECT
        CAST(sale_product_id AS varchar) AS sale_product_id,
        CAST(product_name AS varchar) AS product_name,
        CAST(product_category AS varchar) AS product_category,
        CAST(product_price AS varchar) AS product_price,
        CAST(product_quantity AS varchar) AS product_quantity,
        CAST(product_weight AS varchar) AS product_weight,
        CAST(product_color AS varchar) AS product_color,
        CAST(product_size AS varchar) AS product_size,
        CAST(product_brand AS varchar) AS product_brand,
        CAST(product_material AS varchar) AS product_material,
        CAST(product_description AS varchar) AS product_description,
        CAST(product_rating AS varchar) AS product_rating,
        CAST(product_reviews AS varchar) AS product_reviews,
        CAST(product_release_date AS varchar) AS product_release_date,
        CAST(product_expiry_date AS varchar) AS product_expiry_date,
        CAST(id AS varchar) AS id
    FROM postgresql.public.mock_data
),
clean_data AS (
    SELECT
        try_cast(NULLIF(trim(sale_product_id), '') AS bigint) AS sale_product_id,
        NULLIF(trim(product_name), '') AS product_name,
        NULLIF(trim(product_category), '') AS product_category,
        try_cast(NULLIF(trim(product_price), '') AS decimal(12, 2)) AS product_price,
        try_cast(NULLIF(trim(product_quantity), '') AS integer) AS product_quantity,
        try_cast(NULLIF(trim(product_weight), '') AS decimal(12, 2)) AS product_weight,
        NULLIF(trim(product_color), '') AS product_color,
        NULLIF(trim(product_size), '') AS product_size,
        NULLIF(trim(product_brand), '') AS product_brand,
        NULLIF(trim(product_material), '') AS product_material,
        NULLIF(trim(product_description), '') AS product_description,
        try_cast(NULLIF(trim(product_rating), '') AS decimal(4, 2)) AS product_rating,
        try_cast(NULLIF(trim(product_reviews), '') AS integer) AS product_reviews,
        try(CAST(date_parse(NULLIF(trim(product_release_date), ''), '%c/%e/%Y') AS date)) AS product_release_date,
        try(CAST(date_parse(NULLIF(trim(product_expiry_date), ''), '%c/%e/%Y') AS date)) AS product_expiry_date,
        try_cast(NULLIF(trim(id), '') AS bigint) AS row_id
    FROM source_data
),
ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY sale_product_id ORDER BY row_id) AS rn
    FROM clean_data
    WHERE sale_product_id IS NOT NULL
)
SELECT
    CAST(row_number() OVER (ORDER BY r.sale_product_id) AS decimal(20, 0)) AS product_key,
    r.sale_product_id AS product_source_id,
    to_utf8(r.product_name) AS product_name,
    c.product_category_key,
    b.brand_key,
    m.material_key,
    COALESCE(r.product_price, DECIMAL '0.00') AS product_price,
    COALESCE(r.product_quantity, 0) AS product_quantity,
    COALESCE(r.product_weight, DECIMAL '0.00') AS product_weight,
    to_utf8(r.product_color) AS product_color,
    to_utf8(r.product_size) AS product_size,
    to_utf8(r.product_description) AS product_description,
    COALESCE(r.product_rating, DECIMAL '0.00') AS product_rating,
    COALESCE(r.product_reviews, 0) AS product_reviews,
    COALESCE(r.product_release_date, DATE '1970-01-01') AS product_release_date,
    COALESCE(r.product_expiry_date, DATE '1970-01-01') AS product_expiry_date
FROM ranked r
LEFT JOIN clickhouse.bdtrino_analytics.dim_product_category c
    ON r.product_category = from_utf8(c.category_name)
LEFT JOIN clickhouse.bdtrino_analytics.dim_brand b
    ON r.product_brand = from_utf8(b.brand_name)
LEFT JOIN clickhouse.bdtrino_analytics.dim_material m
    ON r.product_material = from_utf8(m.material_name)
WHERE r.rn = 1;

INSERT INTO clickhouse.bdtrino_analytics.dim_store
WITH source_data AS (
    SELECT
        from_utf8(store_name) AS store_name,
        from_utf8(store_location) AS store_location,
        from_utf8(store_city) AS store_city,
        from_utf8(store_state) AS store_state,
        from_utf8(store_country) AS store_country,
        from_utf8(store_phone) AS store_phone,
        from_utf8(store_email) AS store_email
    FROM clickhouse.bdtrino_source.mock_data
    UNION ALL
    SELECT
        CAST(store_name AS varchar) AS store_name,
        CAST(store_location AS varchar) AS store_location,
        CAST(store_city AS varchar) AS store_city,
        CAST(store_state AS varchar) AS store_state,
        CAST(store_country AS varchar) AS store_country,
        CAST(store_phone AS varchar) AS store_phone,
        CAST(store_email AS varchar) AS store_email
    FROM postgresql.public.mock_data
),
clean_data AS (
    SELECT DISTINCT
        NULLIF(trim(store_name), '') AS store_name,
        NULLIF(trim(store_location), '') AS store_location,
        NULLIF(trim(store_city), '') AS store_city,
        NULLIF(trim(store_state), '') AS store_state,
        NULLIF(trim(store_country), '') AS store_country,
        NULLIF(trim(store_phone), '') AS store_phone,
        NULLIF(trim(store_email), '') AS store_email
    FROM source_data
)
SELECT
    CAST(
        row_number() OVER (
            ORDER BY store_name, store_email, store_phone, store_city
        ) AS decimal(20, 0)
    ) AS store_key,
    to_utf8(store_name) AS store_name,
    to_utf8(store_location) AS store_location,
    to_utf8(store_city) AS store_city,
    to_utf8(store_state) AS store_state,
    to_utf8(store_country) AS store_country,
    to_utf8(store_phone) AS store_phone,
    to_utf8(store_email) AS store_email
FROM clean_data;

INSERT INTO clickhouse.bdtrino_analytics.dim_supplier
WITH source_data AS (
    SELECT
        from_utf8(supplier_name) AS supplier_name,
        from_utf8(supplier_contact) AS supplier_contact,
        from_utf8(supplier_email) AS supplier_email,
        from_utf8(supplier_phone) AS supplier_phone,
        from_utf8(supplier_address) AS supplier_address,
        from_utf8(supplier_city) AS supplier_city,
        from_utf8(supplier_country) AS supplier_country
    FROM clickhouse.bdtrino_source.mock_data
    UNION ALL
    SELECT
        CAST(supplier_name AS varchar) AS supplier_name,
        CAST(supplier_contact AS varchar) AS supplier_contact,
        CAST(supplier_email AS varchar) AS supplier_email,
        CAST(supplier_phone AS varchar) AS supplier_phone,
        CAST(supplier_address AS varchar) AS supplier_address,
        CAST(supplier_city AS varchar) AS supplier_city,
        CAST(supplier_country AS varchar) AS supplier_country
    FROM postgresql.public.mock_data
),
clean_data AS (
    SELECT DISTINCT
        NULLIF(trim(supplier_name), '') AS supplier_name,
        NULLIF(trim(supplier_contact), '') AS supplier_contact,
        NULLIF(trim(supplier_email), '') AS supplier_email,
        NULLIF(trim(supplier_phone), '') AS supplier_phone,
        NULLIF(trim(supplier_address), '') AS supplier_address,
        NULLIF(trim(supplier_city), '') AS supplier_city,
        NULLIF(trim(supplier_country), '') AS supplier_country
    FROM source_data
)
SELECT
    CAST(
        row_number() OVER (
            ORDER BY supplier_name, supplier_email, supplier_phone, supplier_city
        ) AS decimal(20, 0)
    ) AS supplier_key,
    to_utf8(supplier_name) AS supplier_name,
    to_utf8(supplier_contact) AS supplier_contact,
    to_utf8(supplier_email) AS supplier_email,
    to_utf8(supplier_phone) AS supplier_phone,
    to_utf8(supplier_address) AS supplier_address,
    to_utf8(supplier_city) AS supplier_city,
    to_utf8(supplier_country) AS supplier_country
FROM clean_data;

INSERT INTO clickhouse.bdtrino_analytics.fact_sales
WITH source_data AS (
    SELECT
        from_utf8(id) AS id,
        from_utf8(sale_date) AS sale_date,
        from_utf8(sale_customer_id) AS sale_customer_id,
        from_utf8(sale_seller_id) AS sale_seller_id,
        from_utf8(sale_product_id) AS sale_product_id,
        from_utf8(sale_quantity) AS sale_quantity,
        from_utf8(sale_total_price) AS sale_total_price,
        from_utf8(customer_pet_type) AS customer_pet_type,
        from_utf8(customer_pet_name) AS customer_pet_name,
        from_utf8(customer_pet_breed) AS customer_pet_breed,
        from_utf8(pet_category) AS pet_category,
        from_utf8(store_name) AS store_name,
        from_utf8(store_location) AS store_location,
        from_utf8(store_city) AS store_city,
        from_utf8(store_state) AS store_state,
        from_utf8(store_country) AS store_country,
        from_utf8(store_phone) AS store_phone,
        from_utf8(store_email) AS store_email,
        from_utf8(supplier_name) AS supplier_name,
        from_utf8(supplier_contact) AS supplier_contact,
        from_utf8(supplier_email) AS supplier_email,
        from_utf8(supplier_phone) AS supplier_phone,
        from_utf8(supplier_address) AS supplier_address,
        from_utf8(supplier_city) AS supplier_city,
        from_utf8(supplier_country) AS supplier_country
    FROM clickhouse.bdtrino_source.mock_data
    UNION ALL
    SELECT
        CAST(id AS varchar) AS id,
        CAST(sale_date AS varchar) AS sale_date,
        CAST(sale_customer_id AS varchar) AS sale_customer_id,
        CAST(sale_seller_id AS varchar) AS sale_seller_id,
        CAST(sale_product_id AS varchar) AS sale_product_id,
        CAST(sale_quantity AS varchar) AS sale_quantity,
        CAST(sale_total_price AS varchar) AS sale_total_price,
        CAST(customer_pet_type AS varchar) AS customer_pet_type,
        CAST(customer_pet_name AS varchar) AS customer_pet_name,
        CAST(customer_pet_breed AS varchar) AS customer_pet_breed,
        CAST(pet_category AS varchar) AS pet_category,
        CAST(store_name AS varchar) AS store_name,
        CAST(store_location AS varchar) AS store_location,
        CAST(store_city AS varchar) AS store_city,
        CAST(store_state AS varchar) AS store_state,
        CAST(store_country AS varchar) AS store_country,
        CAST(store_phone AS varchar) AS store_phone,
        CAST(store_email AS varchar) AS store_email,
        CAST(supplier_name AS varchar) AS supplier_name,
        CAST(supplier_contact AS varchar) AS supplier_contact,
        CAST(supplier_email AS varchar) AS supplier_email,
        CAST(supplier_phone AS varchar) AS supplier_phone,
        CAST(supplier_address AS varchar) AS supplier_address,
        CAST(supplier_city AS varchar) AS supplier_city,
        CAST(supplier_country AS varchar) AS supplier_country
    FROM postgresql.public.mock_data
),
clean_data AS (
    SELECT
        try_cast(NULLIF(trim(id), '') AS bigint) AS sale_source_row_id,
        try(CAST(date_parse(NULLIF(trim(sale_date), ''), '%c/%e/%Y') AS date)) AS sale_date,
        try_cast(NULLIF(trim(sale_customer_id), '') AS bigint) AS sale_customer_id,
        try_cast(NULLIF(trim(sale_seller_id), '') AS bigint) AS sale_seller_id,
        try_cast(NULLIF(trim(sale_product_id), '') AS bigint) AS sale_product_id,
        try_cast(NULLIF(trim(sale_quantity), '') AS integer) AS sale_quantity,
        try_cast(NULLIF(trim(sale_total_price), '') AS decimal(12, 2)) AS sale_total_price,
        NULLIF(trim(customer_pet_type), '') AS customer_pet_type,
        NULLIF(trim(customer_pet_name), '') AS customer_pet_name,
        NULLIF(trim(customer_pet_breed), '') AS customer_pet_breed,
        NULLIF(trim(pet_category), '') AS pet_category,
        NULLIF(trim(store_name), '') AS store_name,
        NULLIF(trim(store_location), '') AS store_location,
        NULLIF(trim(store_city), '') AS store_city,
        NULLIF(trim(store_state), '') AS store_state,
        NULLIF(trim(store_country), '') AS store_country,
        NULLIF(trim(store_phone), '') AS store_phone,
        NULLIF(trim(store_email), '') AS store_email,
        NULLIF(trim(supplier_name), '') AS supplier_name,
        NULLIF(trim(supplier_contact), '') AS supplier_contact,
        NULLIF(trim(supplier_email), '') AS supplier_email,
        NULLIF(trim(supplier_phone), '') AS supplier_phone,
        NULLIF(trim(supplier_address), '') AS supplier_address,
        NULLIF(trim(supplier_city), '') AS supplier_city,
        NULLIF(trim(supplier_country), '') AS supplier_country
    FROM source_data
)
SELECT
    CAST(
        row_number() OVER (
            ORDER BY
                COALESCE(sale_date, DATE '1970-01-01'),
                sale_source_row_id,
                sale_customer_id,
                sale_seller_id,
                sale_product_id
        ) AS decimal(20, 0)
    ) AS sale_key,
    sale_source_row_id,
    COALESCE(sale_date, DATE '1970-01-01') AS sale_date,
    c.customer_key,
    p.pet_key,
    s.seller_key,
    pr.product_key,
    st.store_key,
    sp.supplier_key,
    COALESCE(d.sale_quantity, 0) AS sale_quantity,
    COALESCE(d.sale_total_price, DECIMAL '0.00') AS sale_total_price
FROM clean_data d
LEFT JOIN clickhouse.bdtrino_analytics.dim_customer c
    ON d.sale_customer_id = c.customer_source_id
LEFT JOIN clickhouse.bdtrino_analytics.dim_pet p
    ON d.sale_customer_id = p.customer_source_id
    AND d.customer_pet_type IS NOT DISTINCT FROM from_utf8(p.pet_type)
    AND d.customer_pet_name IS NOT DISTINCT FROM from_utf8(p.pet_name)
    AND d.customer_pet_breed IS NOT DISTINCT FROM from_utf8(p.pet_breed)
    AND d.pet_category IS NOT DISTINCT FROM from_utf8(p.pet_category)
LEFT JOIN clickhouse.bdtrino_analytics.dim_seller s
    ON d.sale_seller_id = s.seller_source_id
LEFT JOIN clickhouse.bdtrino_analytics.dim_product pr
    ON d.sale_product_id = pr.product_source_id
LEFT JOIN clickhouse.bdtrino_analytics.dim_store st
    ON d.store_name IS NOT DISTINCT FROM from_utf8(st.store_name)
    AND d.store_location IS NOT DISTINCT FROM from_utf8(st.store_location)
    AND d.store_city IS NOT DISTINCT FROM from_utf8(st.store_city)
    AND d.store_state IS NOT DISTINCT FROM from_utf8(st.store_state)
    AND d.store_country IS NOT DISTINCT FROM from_utf8(st.store_country)
    AND d.store_phone IS NOT DISTINCT FROM from_utf8(st.store_phone)
    AND d.store_email IS NOT DISTINCT FROM from_utf8(st.store_email)
LEFT JOIN clickhouse.bdtrino_analytics.dim_supplier sp
    ON d.supplier_name IS NOT DISTINCT FROM from_utf8(sp.supplier_name)
    AND d.supplier_contact IS NOT DISTINCT FROM from_utf8(sp.supplier_contact)
    AND d.supplier_email IS NOT DISTINCT FROM from_utf8(sp.supplier_email)
    AND d.supplier_phone IS NOT DISTINCT FROM from_utf8(sp.supplier_phone)
    AND d.supplier_address IS NOT DISTINCT FROM from_utf8(sp.supplier_address)
    AND d.supplier_city IS NOT DISTINCT FROM from_utf8(sp.supplier_city)
    AND d.supplier_country IS NOT DISTINCT FROM from_utf8(sp.supplier_country);
