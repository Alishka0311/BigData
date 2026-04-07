from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


CSV_PATH = "/opt/project/lr2/data/*.csv"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/bdspark"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}


RAW_COLUMNS = [
    "id",
    "customer_first_name",
    "customer_last_name",
    "customer_age",
    "customer_email",
    "customer_country",
    "customer_postal_code",
    "customer_pet_type",
    "customer_pet_name",
    "customer_pet_breed",
    "seller_first_name",
    "seller_last_name",
    "seller_email",
    "seller_country",
    "seller_postal_code",
    "product_name",
    "product_category",
    "product_price",
    "product_quantity",
    "sale_date",
    "sale_customer_id",
    "sale_seller_id",
    "sale_product_id",
    "sale_quantity",
    "sale_total_price",
    "store_name",
    "store_location",
    "store_city",
    "store_state",
    "store_country",
    "store_phone",
    "store_email",
    "pet_category",
    "product_weight",
    "product_color",
    "product_size",
    "product_brand",
    "product_material",
    "product_description",
    "product_rating",
    "product_reviews",
    "product_release_date",
    "product_expiry_date",
    "supplier_name",
    "supplier_contact",
    "supplier_email",
    "supplier_phone",
    "supplier_address",
    "supplier_city",
    "supplier_country",
]


def null_if_blank(column_name: str):
    trimmed = F.trim(F.col(column_name))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def with_surrogate_key(df, key_column, order_columns):
    window = Window.orderBy(*[F.col(col_name).asc_nulls_last() for col_name in order_columns])
    return df.withColumn(key_column, F.row_number().over(window).cast("long"))


def write_table(df, table_name):
    (
        df.write.mode("overwrite")
        .jdbc(url=POSTGRES_URL, table=table_name, properties=POSTGRES_PROPERTIES)
    )


def main():
    spark = (
        SparkSession.builder.appName("lr2-etl-to-postgres")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_schema = T.StructType([T.StructField(column, T.StringType(), True) for column in RAW_COLUMNS])

    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("quote", '"')
        .schema(raw_schema)
        .load(CSV_PATH)
    )

    df = raw_df.select(
        F.col("id").cast("int").alias("id"),
        null_if_blank("customer_first_name").alias("customer_first_name"),
        null_if_blank("customer_last_name").alias("customer_last_name"),
        F.col("customer_age").cast("int").alias("customer_age"),
        null_if_blank("customer_email").alias("customer_email"),
        null_if_blank("customer_country").alias("customer_country"),
        null_if_blank("customer_postal_code").alias("customer_postal_code"),
        null_if_blank("customer_pet_type").alias("customer_pet_type"),
        null_if_blank("customer_pet_name").alias("customer_pet_name"),
        null_if_blank("customer_pet_breed").alias("customer_pet_breed"),
        null_if_blank("seller_first_name").alias("seller_first_name"),
        null_if_blank("seller_last_name").alias("seller_last_name"),
        null_if_blank("seller_email").alias("seller_email"),
        null_if_blank("seller_country").alias("seller_country"),
        null_if_blank("seller_postal_code").alias("seller_postal_code"),
        null_if_blank("product_name").alias("product_name"),
        null_if_blank("product_category").alias("product_category"),
        F.col("product_price").cast(T.DecimalType(12, 2)).alias("product_price"),
        F.col("product_quantity").cast("int").alias("product_quantity"),
        F.to_date("sale_date", "M/d/yyyy").alias("sale_date"),
        F.col("sale_customer_id").cast("int").alias("sale_customer_id"),
        F.col("sale_seller_id").cast("int").alias("sale_seller_id"),
        F.col("sale_product_id").cast("int").alias("sale_product_id"),
        F.col("sale_quantity").cast("int").alias("sale_quantity"),
        F.col("sale_total_price").cast(T.DecimalType(12, 2)).alias("sale_total_price"),
        null_if_blank("store_name").alias("store_name"),
        null_if_blank("store_location").alias("store_location"),
        null_if_blank("store_city").alias("store_city"),
        null_if_blank("store_state").alias("store_state"),
        null_if_blank("store_country").alias("store_country"),
        null_if_blank("store_phone").alias("store_phone"),
        null_if_blank("store_email").alias("store_email"),
        null_if_blank("pet_category").alias("pet_category"),
        F.col("product_weight").cast(T.DecimalType(12, 2)).alias("product_weight"),
        null_if_blank("product_color").alias("product_color"),
        null_if_blank("product_size").alias("product_size"),
        null_if_blank("product_brand").alias("product_brand"),
        null_if_blank("product_material").alias("product_material"),
        F.col("product_description").alias("product_description"),
        F.col("product_rating").cast(T.DecimalType(3, 1)).alias("product_rating"),
        F.col("product_reviews").cast("int").alias("product_reviews"),
        F.to_date("product_release_date", "M/d/yyyy").alias("product_release_date"),
        F.to_date("product_expiry_date", "M/d/yyyy").alias("product_expiry_date"),
        null_if_blank("supplier_name").alias("supplier_name"),
        null_if_blank("supplier_contact").alias("supplier_contact"),
        null_if_blank("supplier_email").alias("supplier_email"),
        null_if_blank("supplier_phone").alias("supplier_phone"),
        null_if_blank("supplier_address").alias("supplier_address"),
        null_if_blank("supplier_city").alias("supplier_city"),
        null_if_blank("supplier_country").alias("supplier_country"),
    )

    write_table(df, "mock_data")

    customer_window = Window.partitionBy("sale_customer_id").orderBy(F.col("id").asc())
    seller_window = Window.partitionBy("sale_seller_id").orderBy(F.col("id").asc())
    product_window = Window.partitionBy("sale_product_id").orderBy(F.col("id").asc())

    dim_product_category = (
        df.select(F.col("product_category").alias("category_name"))
        .where(F.col("category_name").isNotNull())
        .dropDuplicates(["category_name"])
    )
    dim_product_category = with_surrogate_key(
        dim_product_category,
        "product_category_key",
        ["category_name"],
    ).select("product_category_key", "category_name")

    dim_brand = (
        df.select(F.col("product_brand").alias("brand_name"))
        .where(F.col("brand_name").isNotNull())
        .dropDuplicates(["brand_name"])
    )
    dim_brand = with_surrogate_key(dim_brand, "brand_key", ["brand_name"]).select(
        "brand_key",
        "brand_name",
    )

    dim_material = (
        df.select(F.col("product_material").alias("material_name"))
        .where(F.col("material_name").isNotNull())
        .dropDuplicates(["material_name"])
    )
    dim_material = with_surrogate_key(
        dim_material,
        "material_key",
        ["material_name"],
    ).select("material_key", "material_name")

    dim_customer = (
        df.where(F.col("sale_customer_id").isNotNull())
        .withColumn("rn", F.row_number().over(customer_window))
        .where(F.col("rn") == 1)
        .select(
            F.col("sale_customer_id").alias("customer_source_id"),
            F.col("customer_first_name").alias("first_name"),
            F.col("customer_last_name").alias("last_name"),
            F.col("customer_age").alias("age"),
            F.col("customer_email").alias("email"),
            F.col("customer_country").alias("country"),
            F.col("customer_postal_code").alias("postal_code"),
        )
    )
    dim_customer = with_surrogate_key(
        dim_customer,
        "customer_key",
        ["customer_source_id"],
    ).select(
        "customer_key",
        "customer_source_id",
        "first_name",
        "last_name",
        "age",
        "email",
        "country",
        "postal_code",
    )

    dim_pet = (
        df.select(
            F.col("sale_customer_id").alias("customer_source_id"),
            F.col("customer_pet_type").alias("pet_type"),
            F.col("customer_pet_name").alias("pet_name"),
            F.col("customer_pet_breed").alias("pet_breed"),
            F.col("pet_category").alias("pet_category"),
        )
        .dropDuplicates(
            [
                "customer_source_id",
                "pet_type",
                "pet_name",
                "pet_breed",
                "pet_category",
            ]
        )
    )
    dim_pet = with_surrogate_key(
        dim_pet,
        "pet_key",
        ["customer_source_id", "pet_name", "pet_type", "pet_breed", "pet_category"],
    ).select(
        "pet_key",
        "customer_source_id",
        "pet_type",
        "pet_name",
        "pet_breed",
        "pet_category",
    )

    dim_seller = (
        df.where(F.col("sale_seller_id").isNotNull())
        .withColumn("rn", F.row_number().over(seller_window))
        .where(F.col("rn") == 1)
        .select(
            F.col("sale_seller_id").alias("seller_source_id"),
            F.col("seller_first_name").alias("first_name"),
            F.col("seller_last_name").alias("last_name"),
            F.col("seller_email").alias("email"),
            F.col("seller_country").alias("country"),
            F.col("seller_postal_code").alias("postal_code"),
        )
    )
    dim_seller = with_surrogate_key(
        dim_seller,
        "seller_key",
        ["seller_source_id"],
    ).select(
        "seller_key",
        "seller_source_id",
        "first_name",
        "last_name",
        "email",
        "country",
        "postal_code",
    )

    product_base = (
        df.where(F.col("sale_product_id").isNotNull())
        .withColumn("rn", F.row_number().over(product_window))
        .where(F.col("rn") == 1)
        .alias("p")
    )

    dim_product = (
        product_base
        .join(
            dim_product_category.alias("pc"),
            F.col("p.product_category") == F.col("pc.category_name"),
            "left",
        )
        .join(
            dim_brand.alias("b"),
            F.col("p.product_brand") == F.col("b.brand_name"),
            "left",
        )
        .join(
            dim_material.alias("mt"),
            F.col("p.product_material") == F.col("mt.material_name"),
            "left",
        )
        .select(
            F.col("p.sale_product_id").alias("product_source_id"),
            F.col("p.product_name").alias("product_name"),
            F.col("pc.product_category_key").alias("product_category_key"),
            F.col("b.brand_key").alias("brand_key"),
            F.col("mt.material_key").alias("material_key"),
            F.col("p.product_price").alias("product_price"),
            F.col("p.product_quantity").alias("product_quantity"),
            F.col("p.product_weight").alias("product_weight"),
            F.col("p.product_color").alias("product_color"),
            F.col("p.product_size").alias("product_size"),
            F.col("p.product_description").alias("product_description"),
            F.col("p.product_rating").alias("product_rating"),
            F.col("p.product_reviews").alias("product_reviews"),
            F.col("p.product_release_date").alias("product_release_date"),
            F.col("p.product_expiry_date").alias("product_expiry_date"),
        )
    )
    dim_product = with_surrogate_key(
        dim_product,
        "product_key",
        ["product_source_id"],
    ).select(
        "product_key",
        "product_source_id",
        "product_name",
        "product_category_key",
        "brand_key",
        "material_key",
        "product_price",
        "product_quantity",
        "product_weight",
        "product_color",
        "product_size",
        "product_description",
        "product_rating",
        "product_reviews",
        "product_release_date",
        "product_expiry_date",
    )

    dim_store = (
        df.select(
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
            "store_phone",
            "store_email",
        )
        .dropDuplicates(
            [
                "store_name",
                "store_location",
                "store_city",
                "store_state",
                "store_country",
                "store_phone",
                "store_email",
            ]
        )
    )
    dim_store = with_surrogate_key(
        dim_store,
        "store_key",
        ["store_name", "store_email", "store_phone", "store_city"],
    ).select(
        "store_key",
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email",
    )

    dim_supplier = (
        df.select(
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country",
        )
        .dropDuplicates(
            [
                "supplier_name",
                "supplier_contact",
                "supplier_email",
                "supplier_phone",
                "supplier_address",
                "supplier_city",
                "supplier_country",
            ]
        )
    )
    dim_supplier = with_surrogate_key(
        dim_supplier,
        "supplier_key",
        ["supplier_name", "supplier_email", "supplier_phone", "supplier_city"],
    ).select(
        "supplier_key",
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country",
    )

    fact_sales = (
        df.alias("m")
        .join(
            dim_customer.alias("c"),
            F.col("m.sale_customer_id") == F.col("c.customer_source_id"),
            "left",
        )
        .join(
            dim_pet.alias("p"),
            (F.col("m.sale_customer_id") == F.col("p.customer_source_id"))
            & F.col("m.customer_pet_name").eqNullSafe(F.col("p.pet_name"))
            & F.col("m.customer_pet_type").eqNullSafe(F.col("p.pet_type"))
            & F.col("m.customer_pet_breed").eqNullSafe(F.col("p.pet_breed"))
            & F.col("m.pet_category").eqNullSafe(F.col("p.pet_category")),
            "left",
        )
        .join(
            dim_seller.alias("s"),
            F.col("m.sale_seller_id") == F.col("s.seller_source_id"),
            "left",
        )
        .join(
            dim_product.alias("pr"),
            F.col("m.sale_product_id") == F.col("pr.product_source_id"),
            "left",
        )
        .join(
            dim_store.alias("st"),
            F.col("m.store_name").eqNullSafe(F.col("st.store_name"))
            & F.col("m.store_location").eqNullSafe(F.col("st.store_location"))
            & F.col("m.store_city").eqNullSafe(F.col("st.store_city"))
            & F.col("m.store_state").eqNullSafe(F.col("st.store_state"))
            & F.col("m.store_country").eqNullSafe(F.col("st.store_country"))
            & F.col("m.store_phone").eqNullSafe(F.col("st.store_phone"))
            & F.col("m.store_email").eqNullSafe(F.col("st.store_email")),
            "left",
        )
        .join(
            dim_supplier.alias("sp"),
            F.col("m.supplier_name").eqNullSafe(F.col("sp.supplier_name"))
            & F.col("m.supplier_contact").eqNullSafe(F.col("sp.supplier_contact"))
            & F.col("m.supplier_email").eqNullSafe(F.col("sp.supplier_email"))
            & F.col("m.supplier_phone").eqNullSafe(F.col("sp.supplier_phone"))
            & F.col("m.supplier_address").eqNullSafe(F.col("sp.supplier_address"))
            & F.col("m.supplier_city").eqNullSafe(F.col("sp.supplier_city"))
            & F.col("m.supplier_country").eqNullSafe(F.col("sp.supplier_country")),
            "left",
        )
        .select(
            F.col("m.id").alias("sale_source_row_id"),
            F.col("m.sale_date"),
            F.col("c.customer_key"),
            F.col("p.pet_key"),
            F.col("s.seller_key"),
            F.col("pr.product_key"),
            F.col("st.store_key"),
            F.col("sp.supplier_key"),
            F.col("m.sale_quantity"),
            F.col("m.sale_total_price"),
        )
    )
    fact_sales = with_surrogate_key(
        fact_sales,
        "sale_key",
        ["sale_source_row_id"],
    ).select(
        "sale_key",
        "sale_source_row_id",
        "sale_date",
        "customer_key",
        "pet_key",
        "seller_key",
        "product_key",
        "store_key",
        "supplier_key",
        "sale_quantity",
        "sale_total_price",
    )

    for table_name, table_df in [
        ("dim_product_category", dim_product_category),
        ("dim_brand", dim_brand),
        ("dim_material", dim_material),
        ("dim_customer", dim_customer),
        ("dim_pet", dim_pet),
        ("dim_seller", dim_seller),
        ("dim_product", dim_product),
        ("dim_store", dim_store),
        ("dim_supplier", dim_supplier),
        ("fact_sales", fact_sales),
    ]:
        write_table(table_df, table_name)

    spark.stop()


if __name__ == "__main__":
    main()
