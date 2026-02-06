from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name = "dim_product",
    comment="Dimensional model for Product data."
)
def dim_product():
    # 1. El Stream principal (Driving table)
    product = (
        spark.readStream.table("dev_bronze.stg_production.stg_product")
        .alias("p")
    )

    # 2. Lookups estáticos para evitar errores de Stream-Stream Join
    product_subcategory = dp.read("dev_bronze.stg_production.stg_product_subcategory").alias("ps")
    product_category = dp.read("dev_bronze.stg_production.stg_product_category").alias("pc")

    # 3. Transformación y Joins
    return (
        product
        .join(
            product_subcategory,
            F.col("p.ProductSubcategoryID") == F.col("ps.ProductSubcategoryID"),
            "left"
        )
        .join(
            product_category,
            F.col("ps.ProductCategoryID") == F.col("pc.ProductCategoryID"),
            "left"
        )
        .select(
            F.col("p.ProductID").alias("product_key"),
            F.col("p.ProductNumber").alias("product_number"),
            F.col("p.Name").alias("product_name"),
            
            # Manejo de IFNULL -> F.coalesce
            F.coalesce(F.col("ps.Name"), F.lit("Missing")).alias("product_subcategory_name"),
            F.coalesce(F.col("pc.Name"), F.lit("Missing")).alias("product_category_name"),
            F.coalesce(F.col("p.Color"), F.lit("Missing")).alias("product_color"),
            F.coalesce(F.col("p.Class"), F.lit("-")).alias("product_class"),
            F.coalesce(F.col("p.Style"), F.lit("-")).alias("product_style"),
            F.coalesce(F.col("p.Size"), F.lit("-")).alias("product_size"),
            F.coalesce(F.col("p.SizeUnitMeasureCode"), F.lit("-")).alias("product_size_unit_measure_code"),
            
            # Cast de weight a string para el coalesce
            F.coalesce(F.col("p.Weight").cast("string"), F.lit("-")).alias("product_weight"),
            
            F.coalesce(F.col("p.WeightUnitMeasureCode"), F.lit("Missing")).alias("product_weight_unit_measure_code"),
            F.col("p.StandardCost").alias("product_standard_cost"),
            F.col("p.ListPrice").alias("product_list_price"),
            F.col("p.SafetyStockLevel").alias("product_safety_stock_level"),
            F.col("p.ReorderPoint").alias("product_reorder_point"),
            F.col("p.DaysToManufacture").alias("product_days_to_manufacture"),
            F.col("p.MakeFlag").alias("product_make_flag"),
            F.col("p.FinishedGoodsFlag").alias("product_finished_goods_flag"),
            F.col("p.ProductLine").alias("product_line")
        )
    )