from pyspark.sql import functions as F, Window
from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.table(
    name="dim_product",
    comment="Gold product dimension — enriched with price_tier (quartile within category). price_margin and margin_pct live in the metrics view.",
)
def dim_product():
    dim_prod = spark.read.table("productionsilver.production.dim_product")

    # Price tier: ntile(4) partitioned by product_category, ordered by list_price
    # 1st quartile = Low, 2nd = MidLow, 3rd = MidHigh, 4th = High
    price_window = Window.partitionBy("product_category_name").orderBy("product_list_price")
    dim_prod = dim_prod.withColumn("price_ntile", F.ntile(4).over(price_window))

    price_tier = (
        F.when(col("price_ntile") == 1, "Low")
         .when(col("price_ntile") == 2, "MidLow")
         .when(col("price_ntile") == 3, "MidHigh")
         .otherwise("High")
    )

    return dim_prod.select(
        col("product_key"),
        col("product_number"),
        col("product_name"),
        col("product_subcategory_name"),
        col("product_category_name"),
        col("product_color"),
        col("product_class"),
        col("product_style"),
        col("product_size"),
        col("product_size_unit_measure_code"),
        col("product_weight"),
        col("product_weight_unit_measure_code"),
        col("product_standard_cost"),
        col("product_list_price"),
        price_tier.alias("product_price_tier"),
        col("product_safety_stock_level"),
        col("product_reorder_point"),
        col("product_days_to_manufacture"),
        col("product_make_flag"),
        col("product_finished_goods_flag"),
        col("product_line"),
    )
