from pyspark import pipelines as dp
from pyspark.sql.functions import col, concat_ws, trim, regexp_replace, coalesce, greatest, when, lit

@dp.table(
    name="dim_store",
    comment="Store Dimension One row per store customer. Key uses customer_id to align with fact sales foreign key."
)
def dim_individual_customer():
    # 1. Read from the bronze layer tables 
    customer_df = spark.readStream.option("readChangeFeed", "True").table("dev_bronze.stg_sales.stg_customer")
    store_df = spark.readStream.option("readChangeFeed", "True").table("dev_bronze.stg_sales.stg_store")

    # 2. Join and Transform 
    return (
        customer_df.alias("c").filter(col("CustomerID").isNotNull())
        .join(store_df.alias("s"), col("c.CustomerID") == col("s.BusinessEntityID"), "left")
        .select(
            col("c.CustomerID").alias("store_key"),
            col("c.AccountNumber").alias("store_account_number"),
            col("s.Name").alias("store_name"),
            col("c.ModifiedDate").alias("store_modified_date"),
            greatest(col("p.ModifiedDate"), col("c.ModifiedDate")).alias("store_last_modified_date")
        )
    )