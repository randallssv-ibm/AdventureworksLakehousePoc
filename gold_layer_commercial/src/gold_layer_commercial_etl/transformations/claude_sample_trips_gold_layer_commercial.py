from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.window import Window


# ============================================================
# Geography mapping — temporary table (not persisted)
# Normalizes the raw natural_geography_key (address_id_postalcode)
# to a canonical geography_key per city/state/country grain.
# Reads from the silver dim_geo table (static batch).
# ============================================================

@dp.temporary_table(
    name="geography_mapping",
    quality="silver",
    description="Maps natural address key to canonical geography_key using MAX OVER PARTITION BY city/state/country",
)
def geography_mapping():
    df = spark.read.table("personsilver.address.dim_geo")

    window_spec = (
        Window
        .partitionBy("city", "state_province", "country_region")
        .orderBy(F.col("postal_code").desc())
    )

    return (
        df
        .withColumn("geography_key", F.max("geography_key").over(window_spec))
        .select("geography_key", "natural_geography_key")
    )


# ============================================================
# Gold fact_sales — fully denormalized sales fact
# Reads silver fact_sales + joins all dim tables + resolved
# geography key via the geography_mapping temp table above.
# ============================================================

@dp.table(
    name="fact_sales_gold",
    quality="gold",
    description="Gold fact table for sales orders — fully enriched with product, geography, customer and store dimension keys",
)
def fact_sales_gold():
    # Silver sources (all static batch reads)
    fs        = spark.read.table("commercialsilver.sales.fact_sales").alias("fs")
    gm        = spark.read.table("geography_mapping").alias("gm")
    dg        = spark.read.table("personsilver.address.dim_geo").alias("dg")
    dim_prod  = spark.read.table("productionsilver.production.dim_product").alias("dp")  # named dim_prod to avoid shadowing the dp pipeline import
    dc        = spark.read.table("commercialsilver.sales.dim_customer").alias("dc")
    ds        = spark.read.table("commercialsilver.sales.dim_store").alias("ds")

    return (
        fs
        .join(dim_prod, F.col("fs.product_key")    == F.col("dp.product_key"),           "left")
        .join(gm,       F.col("fs.geography_key")  == F.col("gm.natural_geography_key"),  "left")
        .join(dg,       F.col("gm.geography_key")  == F.col("dg.geography_key"),          "left")
        .join(dc,       F.col("fs.customer_id")    == F.col("dc.customer_key"),           "left")
        .join(ds,       F.col("fs.customer_id")    == F.col("ds.store_key"),              "left")
        .select(
            F.col("fs.sales_order_id"),
            F.col("fs.sales_order_detail_id"),
            F.col("dp.product_key"),
            F.col("fs.geography_key").alias("fs_geo_key"),  # pre-lookup key, kept for traceability
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
            F.col("fs.total_due"),
        )
    )
