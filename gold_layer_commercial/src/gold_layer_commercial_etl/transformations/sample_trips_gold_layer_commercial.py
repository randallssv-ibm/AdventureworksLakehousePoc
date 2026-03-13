from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.window import Window

@dp.temporary_table(
    name="geography_mapping",
    quality="silver",
    description="Sales aggregations",
)
def geography_mapping():
    # Load the base table
    df = spark.read.table("personsilver.address.dim_geo")

    # Define the window specification
    # We partition by the geographic grain and order by postal_code to find the "latest" key
    window_spec = Window.partitionBy("city", "state_province", "country_region") \
                        .orderBy(F.col("postal_code").desc())

    # Add the geography_key as a new column using the window function
    df_transformed = df.withColumn(
        "geography_key", 
        F.max("geography_key").over(window_spec)
    )

    # Return only the requested columns
    return df_transformed.select("geography_key", "natural_geography_key")



@dp.table(
    name="fact_sales_gold",
    quality="gold",
    description="Sales aggregations",
)
def fact_sales():
    # 1. Loading the tables using your standard spark.read.table approach
    fs = spark.read.table("commercialsilver.sales.fact_sales")
    dg = spark.read.table("personsilver.address.dim_geo")
    gm = spark.read.table("geography_mapping")
    dp = spark.read.table("productionsilver.production.dim_product")
    dc = spark.read.table("commercialsilver.sales.dim_customer")
    ds = spark.read.table("commercialsilver.sales.dim_store")


    # 2. Joining the tables
    final_df = fs.alias("fs") \
        .join(dp.alias("dp"), F.col("fs.product_key") == F.col("dp.product_key"), "left") \
        .join(gm.alias("gm"), F.col("fs.geography_key") == F.col("gm.natural_geography_key"), "left") \
        .join(dg.alias("dg"), F.col("gm.geography_key") == F.col("dg.geography_key"), "left") \
        .join(dc.alias("dc"), F.col("fs.customer_id") == F.col("dc.customer_key"), "left") \
        .join(ds.alias("ds"), F.col("fs.customer_id") == F.col("ds.store_key"), "left")

    # 3. Final Selection
    return final_df.select(
        F.col("fs.sales_order_id"),
        F.col("fs.sales_order_detail_id"),
        F.col("dp.product_key"),
        F.col("fs.geography_key").alias("fs_geo_key"),
        F.col("dg.geography_key"),
        F.col("fs.order_date"),
        F.col("fs.order_status"),
        F.col("dc.customer_key"),
        F.col("ds.store_key"),
        F.col("fs.sales_person_id"),
        F.col("fs.order_qty"),
        F.col("fs.unit_price"),
        F.col("fs.unit_price_discount"),
        F.col("fs.line_total"),
        F.col("fs.sub_total"),
        F.col("fs.tax_amt"),
        F.col("fs.freight"),
        F.col("fs.total_due")
    )