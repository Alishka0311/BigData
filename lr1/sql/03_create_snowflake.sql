drop table if exists fact_sales cascade;
drop table if exists dim_pet cascade;
drop table if exists dim_customer cascade;
drop table if exists dim_seller cascade;
drop table if exists dim_product cascade;
drop table if exists dim_product_category cascade;
drop table if exists dim_brand cascade;
drop table if exists dim_material cascade;
drop table if exists dim_store cascade;
drop table if exists dim_supplier cascade;

create table dim_customer (
    customer_key bigserial primary key,
    customer_source_id integer unique,
    first_name text,
    last_name text,
    age integer,
    email text,
    country text,
    postal_code text
);

create table dim_pet (
    pet_key bigserial primary key,
    customer_source_id integer,
    pet_type text,
    pet_name text,
    pet_breed text,
    pet_category text,
    unique (customer_source_id, pet_name, pet_type, pet_breed)
);

create table dim_seller (
    seller_key bigserial primary key,
    seller_source_id integer unique,
    first_name text,
    last_name text,
    email text,
    country text,
    postal_code text
);

create table dim_product_category (
    product_category_key bigserial primary key,
    category_name text unique
);

create table dim_brand (
    brand_key bigserial primary key,
    brand_name text unique
);

create table dim_material (
    material_key bigserial primary key,
    material_name text unique
);

create table dim_product (
    product_key bigserial primary key,
    product_source_id integer unique,
    product_name text,
    product_category_key bigint references dim_product_category(product_category_key),
    brand_key bigint references dim_brand(brand_key),
    material_key bigint references dim_material(material_key),
    product_price numeric(12,2),
    product_quantity integer,
    product_weight numeric(12,2),
    product_color text,
    product_size text,
    product_description text,
    product_rating numeric(3,1),
    product_reviews integer,
    product_release_date date,
    product_expiry_date date
);

create table dim_store (
    store_key bigserial primary key,
    store_name text,
    store_location text,
    store_city text,
    store_state text,
    store_country text,
    store_phone text,
    store_email text,
    unique (store_name, store_email, store_phone)
);

create table dim_supplier (
    supplier_key bigserial primary key,
    supplier_name text,
    supplier_contact text,
    supplier_email text,
    supplier_phone text,
    supplier_address text,
    supplier_city text,
    supplier_country text,
    unique (supplier_name, supplier_email, supplier_phone)
);

create table fact_sales (
    sale_key bigserial primary key,
    sale_source_row_id integer,
    sale_date date,
    customer_key bigint references dim_customer(customer_key),
    pet_key bigint references dim_pet(pet_key),
    seller_key bigint references dim_seller(seller_key),
    product_key bigint references dim_product(product_key),
    store_key bigint references dim_store(store_key),
    supplier_key bigint references dim_supplier(supplier_key),
    sale_quantity integer,
    sale_total_price numeric(12,2)
);