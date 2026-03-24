from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.table(name="dim_product")
def dim_product():
    # Silver dim_product is already at product_key grain — straight pass-through.
    dim_prod = spark.read.table("productionsilver.production.dim_product")
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
        col("product_safety_stock_level"),
        col("product_reorder_point"),
        col("product_days_to_manufacture"),
        col("product_make_flag"),
        col("product_finished_goods_flag"),
        col("product_line"),
    )
