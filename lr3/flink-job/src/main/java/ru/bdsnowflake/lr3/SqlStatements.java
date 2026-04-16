package ru.bdsnowflake.lr3;

public final class SqlStatements {
    private SqlStatements() {
    }

    public static final String UPSERT_CUSTOMER = """
            INSERT INTO dim_customer (
                customer_key, customer_source_id, customer_first_name, customer_last_name, customer_age, customer_email,
                customer_country, customer_postal_code, customer_pet_type, customer_pet_name,
                customer_pet_breed, pet_category
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (customer_key) DO UPDATE SET
                customer_source_id = EXCLUDED.customer_source_id,
                customer_first_name = EXCLUDED.customer_first_name,
                customer_last_name = EXCLUDED.customer_last_name,
                customer_age = EXCLUDED.customer_age,
                customer_email = EXCLUDED.customer_email,
                customer_country = EXCLUDED.customer_country,
                customer_postal_code = EXCLUDED.customer_postal_code,
                customer_pet_type = EXCLUDED.customer_pet_type,
                customer_pet_name = EXCLUDED.customer_pet_name,
                customer_pet_breed = EXCLUDED.customer_pet_breed,
                pet_category = EXCLUDED.pet_category
            """;

    public static final String UPSERT_PET = """
            INSERT INTO dim_pet (
                pet_key, customer_key, pet_type, pet_name, pet_breed, pet_category
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (pet_key) DO UPDATE SET
                customer_key = EXCLUDED.customer_key,
                pet_type = EXCLUDED.pet_type,
                pet_name = EXCLUDED.pet_name,
                pet_breed = EXCLUDED.pet_breed,
                pet_category = EXCLUDED.pet_category
            """;

    public static final String UPSERT_SELLER = """
            INSERT INTO dim_seller (
                seller_key, seller_source_id, seller_first_name, seller_last_name, seller_email, seller_country, seller_postal_code
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (seller_key) DO UPDATE SET
                seller_source_id = EXCLUDED.seller_source_id,
                seller_first_name = EXCLUDED.seller_first_name,
                seller_last_name = EXCLUDED.seller_last_name,
                seller_email = EXCLUDED.seller_email,
                seller_country = EXCLUDED.seller_country,
                seller_postal_code = EXCLUDED.seller_postal_code
            """;

    public static final String UPSERT_PRODUCT = """
            INSERT INTO dim_product (
                product_key, product_source_id, product_name, product_category, product_price, product_quantity,
                product_weight, product_color, product_size, product_brand, product_material,
                product_description, product_rating, product_reviews, product_release_date, product_expiry_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (product_key) DO UPDATE SET
                product_source_id = EXCLUDED.product_source_id,
                product_name = EXCLUDED.product_name,
                product_category = EXCLUDED.product_category,
                product_price = EXCLUDED.product_price,
                product_quantity = EXCLUDED.product_quantity,
                product_weight = EXCLUDED.product_weight,
                product_color = EXCLUDED.product_color,
                product_size = EXCLUDED.product_size,
                product_brand = EXCLUDED.product_brand,
                product_material = EXCLUDED.product_material,
                product_description = EXCLUDED.product_description,
                product_rating = EXCLUDED.product_rating,
                product_reviews = EXCLUDED.product_reviews,
                product_release_date = EXCLUDED.product_release_date,
                product_expiry_date = EXCLUDED.product_expiry_date
            """;

    public static final String UPSERT_STORE = """
            INSERT INTO dim_store (
                store_key, store_name, store_location, store_city, store_state, store_country, store_phone, store_email
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (store_key) DO UPDATE SET
                store_name = EXCLUDED.store_name,
                store_location = EXCLUDED.store_location,
                store_city = EXCLUDED.store_city,
                store_state = EXCLUDED.store_state,
                store_country = EXCLUDED.store_country,
                store_phone = EXCLUDED.store_phone,
                store_email = EXCLUDED.store_email
            """;

    public static final String UPSERT_SUPPLIER = """
            INSERT INTO dim_supplier (
                supplier_key, supplier_name, supplier_contact, supplier_email, supplier_phone,
                supplier_address, supplier_city, supplier_country
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (supplier_key) DO UPDATE SET
                supplier_name = EXCLUDED.supplier_name,
                supplier_contact = EXCLUDED.supplier_contact,
                supplier_email = EXCLUDED.supplier_email,
                supplier_phone = EXCLUDED.supplier_phone,
                supplier_address = EXCLUDED.supplier_address,
                supplier_city = EXCLUDED.supplier_city,
                supplier_country = EXCLUDED.supplier_country
            """;

    public static final String UPSERT_FACT_SALE = """
            INSERT INTO fact_sales (
                sale_key, source_file, sale_source_row_id, sale_date, sale_customer_id, sale_seller_id, sale_product_id,
                sale_quantity, sale_total_price, customer_key, pet_key, seller_key, product_key, store_key, supplier_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (sale_key) DO UPDATE SET
                source_file = EXCLUDED.source_file,
                sale_source_row_id = EXCLUDED.sale_source_row_id,
                sale_date = EXCLUDED.sale_date,
                sale_customer_id = EXCLUDED.sale_customer_id,
                sale_seller_id = EXCLUDED.sale_seller_id,
                sale_product_id = EXCLUDED.sale_product_id,
                sale_quantity = EXCLUDED.sale_quantity,
                sale_total_price = EXCLUDED.sale_total_price,
                customer_key = EXCLUDED.customer_key,
                pet_key = EXCLUDED.pet_key,
                seller_key = EXCLUDED.seller_key,
                product_key = EXCLUDED.product_key,
                store_key = EXCLUDED.store_key,
                supplier_key = EXCLUDED.supplier_key
            """;
}
