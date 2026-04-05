
insert into dim_product_category (category_name)
select distinct product_category
from mock_data
where product_category is not null
  and trim(product_category) <> '';

insert into dim_brand (brand_name)
select distinct product_brand
from mock_data
where product_brand is not null
  and trim(product_brand) <> '';

insert into dim_material (material_name)
select distinct product_material
from mock_data
where product_material is not null
  and trim(product_material) <> '';

insert into dim_customer (
    customer_source_id,
    first_name,
    last_name,
    age,
    email,
    country,
    postal_code
)
select
    sale_customer_id,
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code
from (
    select *,
           row_number() over (
               partition by sale_customer_id
               order by id
           ) as rn
    from mock_data
    where sale_customer_id is not null
) t
where rn = 1;

insert into dim_pet (
    customer_source_id,
    pet_type,
    pet_name,
    pet_breed,
    pet_category
)
select distinct
    sale_customer_id,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed,
    pet_category
from mock_data;

insert into dim_seller (
    seller_source_id,
    first_name,
    last_name,
    email,
    country,
    postal_code
)
select
    sale_seller_id,
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
from (
    select *,
           row_number() over (
               partition by sale_seller_id
               order by id
           ) as rn
    from mock_data
    where sale_seller_id is not null
) t
where rn = 1;

insert into dim_product (
    product_source_id,
    product_name,
    product_category_key,
    brand_key,
    material_key,
    product_price,
    product_quantity,
    product_weight,
    product_color,
    product_size,
    product_description,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date
)
select
    sale_product_id,
    product_name,
    product_category_key,
    brand_key,
    material_key,
    product_price,
    product_quantity,
    product_weight,
    product_color,
    product_size,
    product_description,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date
from (
    select
        m.*,
        c.product_category_key,
        b.brand_key,
        mt.material_key,
        row_number() over (
            partition by m.sale_product_id
            order by m.id
        ) as rn
    from mock_data m
    left join dim_product_category c
        on c.category_name = m.product_category
    left join dim_brand b
        on b.brand_name = m.product_brand
    left join dim_material mt
        on mt.material_name = m.product_material
    where m.sale_product_id is not null
) t
where rn = 1;

insert into dim_store (
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
)
select distinct
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
from mock_data;

insert into dim_supplier (
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
)
select distinct
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
from mock_data;