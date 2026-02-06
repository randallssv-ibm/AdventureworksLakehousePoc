from pyspark import pipelines as dp
from pyspark.sql.functions import col, greatest, expr

@dp.table(
    name="dim_store",
    comment="High-frequency refresh using 5min watermark and 2min windows for stream-stream join."
)
def dim_store_watermarked():
    # 1. Customer Stream: 5-minute threshold for late data
    customer_df = (
        spark.readStream.table("dev_bronze.stg_sales.stg_customer")
        .withWatermark("ModifiedDate", "5 minutes")
        .alias("c")
    )


    # 2. Store Stream: 5-minute threshold for late data
    store_df = (
        spark.readStream.table("dev_bronze.stg_sales.stg_store")
        .withWatermark("ModifiedDate", "5 minutes")
        .alias("s")
    )
    # 3. Stream-Stream Left Join with 2min interval
    return (
        customer_df.filter(col("c.StoreID").isNotNull())
        .join(
            store_df,
            expr("""
                c.StoreID = s.BusinessEntityID AND
                s.ModifiedDate >= c.ModifiedDate - interval 2 minutes AND
                s.ModifiedDate <= c.ModifiedDate + interval 2 minutes
            """),
            "left"
        )
        .select(
            col("c.CustomerID").alias("store_key"),
            col("c.AccountNumber").alias("store_account_number"),
            col("s.Name").alias("store_name"),
            greatest(col("c.ModifiedDate"), col("s.ModifiedDate")).alias("store_last_modified_date")
        )
    )