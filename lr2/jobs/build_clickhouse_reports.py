from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


POSTGRES_URL = "jdbc:postgresql://postgres:5432/bdspark"
CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/bdspark_reports"

POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

CLICKHOUSE_PROPERTIES = {
    "user": "default",
    "password": "clickhouse",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}


def read_postgres_table(spark, table_name):
    return spark.read.jdbc(POSTGRES_URL, table_name, properties=POSTGRES_PROPERTIES)


def write_clickhouse_table(df, table_name):
    (
        df.write.mode("append")
        .jdbc(url=CLICKHOUSE_URL, table=table_name, properties=CLICKHOUSE_PROPERTIES)
    )


def main():
    spark = (
        SparkSession.builder.appName("lr2-clickhouse-reports")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    fact_sales = read_postgres_table(spark, "fact_sales")
    dim_customer = read_postgres_table(spark, "dim_customer")
    dim_product = read_postgres_table(spark, "dim_product")
    dim_product_category = read_postgres_table(spark, "dim_product_category")
    dim_store = read_postgres_table(spark, "dim_store")
    dim_supplier = read_postgres_table(spark, "dim_supplier")

    fact_enriched = (
        fact_sales.alias("f")
        .join(dim_customer.alias("c"), "customer_key", "left")
        .join(dim_product.alias("p"), "product_key", "left")
        .join(
            dim_product_category.alias("pc"),
            F.col("p.product_category_key") == F.col("pc.product_category_key"),
            "left",
        )
        .join(dim_store.alias("st"), "store_key", "left")
        .join(dim_supplier.alias("sp"), "supplier_key", "left")
        .select(
            F.col("f.sale_key"),
            F.col("f.sale_date"),
            F.col("f.customer_key"),
            F.col("c.first_name").alias("customer_first_name"),
            F.col("c.last_name").alias("customer_last_name"),
            F.col("c.country").alias("customer_country"),
            F.col("f.product_key"),
            F.col("p.product_name"),
            F.col("pc.category_name").alias("product_category"),
            F.col("p.product_rating"),
            F.col("p.product_reviews"),
            F.col("p.product_price"),
            F.col("f.store_key"),
            F.col("st.store_name"),
            F.col("st.store_city"),
            F.col("st.store_state"),
            F.col("st.store_country"),
            F.col("f.supplier_key"),
            F.col("sp.supplier_name"),
            F.col("sp.supplier_country"),
            F.col("f.sale_quantity"),
            F.col("f.sale_total_price"),
        )
        .withColumn("sale_year", F.year("sale_date"))
        .withColumn("sale_month", F.month("sale_date"))
    )

    product_window = Window.orderBy(F.col("total_revenue").desc(), F.col("product_name").asc())
    customer_window = Window.orderBy(F.col("total_revenue").desc(), F.col("customer_key").asc())
    store_window = Window.orderBy(F.col("total_revenue").desc(), F.col("store_name").asc())
    supplier_window = Window.orderBy(F.col("total_revenue").desc(), F.col("supplier_name").asc())
    rating_desc_window = Window.orderBy(F.col("product_rating").desc_nulls_last(), F.col("product_name").asc())
    rating_asc_window = Window.orderBy(F.col("product_rating").asc_nulls_last(), F.col("product_name").asc())
    reviews_window = Window.orderBy(F.col("product_reviews").desc_nulls_last(), F.col("product_name").asc())

    sales_product_report = (
        fact_enriched.groupBy("product_key", "product_name", "product_category")
        .agg(
            F.sum("sale_quantity").alias("total_quantity"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.round(F.avg("product_rating"), 2).alias("avg_rating"),
            F.max("product_reviews").alias("reviews_count"),
            F.countDistinct("sale_key").alias("sales_count"),
        )
        .withColumn("sales_rank", F.row_number().over(product_window))
        .orderBy("sales_rank")
    )

    sales_customer_report = (
        fact_enriched.groupBy(
            "customer_key",
            "customer_first_name",
            "customer_last_name",
            "customer_country",
        )
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.countDistinct("sale_key").alias("orders_count"),
        )
        .withColumn("avg_check", F.round(F.col("total_revenue") / F.col("orders_count"), 2))
        .withColumn("customer_rank", F.row_number().over(customer_window))
        .orderBy("customer_rank")
    )

    sales_time_report = (
        fact_enriched.groupBy("sale_year", "sale_month")
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.countDistinct("sale_key").alias("orders_count"),
        )
        .withColumn("avg_order_value", F.round(F.col("total_revenue") / F.col("orders_count"), 2))
        .withColumn("period", F.format_string("%04d-%02d", F.col("sale_year"), F.col("sale_month")))
        .orderBy("sale_year", "sale_month")
    )

    sales_store_report = (
        fact_enriched.groupBy(
            "store_key",
            "store_name",
            "store_city",
            "store_state",
            "store_country",
        )
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.countDistinct("sale_key").alias("orders_count"),
        )
        .withColumn("avg_check", F.round(F.col("total_revenue") / F.col("orders_count"), 2))
        .withColumn("store_rank", F.row_number().over(store_window))
        .orderBy("store_rank")
    )

    sales_supplier_report = (
        fact_enriched.groupBy("supplier_key", "supplier_name", "supplier_country")
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.round(F.avg("product_price"), 2).alias("avg_product_price"),
            F.sum("sale_quantity").alias("total_quantity"),
        )
        .withColumn("supplier_rank", F.row_number().over(supplier_window))
        .orderBy("supplier_rank")
    )

    product_quality_report = (
        fact_enriched.groupBy("product_key", "product_name", "product_category")
        .agg(
            F.round(F.avg("product_rating"), 2).alias("product_rating"),
            F.max("product_reviews").alias("product_reviews"),
            F.sum("sale_quantity").alias("total_quantity"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        )
        .withColumn("rating_rank_desc", F.row_number().over(rating_desc_window))
        .withColumn("rating_rank_asc", F.row_number().over(rating_asc_window))
        .withColumn("reviews_rank", F.row_number().over(reviews_window))
        .orderBy("rating_rank_desc")
    )

    for table_name, table_df in [
        ("sales_product_report", sales_product_report),
        ("sales_customer_report", sales_customer_report),
        ("sales_time_report", sales_time_report),
        ("sales_store_report", sales_store_report),
        ("sales_supplier_report", sales_supplier_report),
        ("product_quality_report", product_quality_report),
    ]:
        write_clickhouse_table(table_df, table_name)

    spark.stop()


if __name__ == "__main__":
    main()
