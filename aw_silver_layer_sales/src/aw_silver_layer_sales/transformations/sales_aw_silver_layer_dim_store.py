import dlt
from pyspark.sql.functions import col, greatest, expr

@dlt.table(
    name="dim_store_watermarked",
    comment="High-frequency refresh using 5min watermark and 2min windows for stream-stream join."
)
def dim_store_watermarked():
    # 1. Customer Stream: 5-minute threshold for late data
    customer_df = (
        dlt.read_stream("stg_adventureworks_sales__customer")
        .withWatermark("modified_date", "5 minutes")
        .alias("c")
    )

    # 2. Store Stream: 5-minute threshold for late data
    store_df = (
        dlt.read_stream("stg_adventureworks_sales__store")
        .withWatermark("modified_date", "5 minutes")
        .alias("s")
    )

    # 3. Stream-Stream Left Join with 2min interval
    return (
        customer_df.filter(col("c.store_id").isNotNull())
        .join(
            store_df,
            expr("""
                c.store_id = s.business_entity_id AND
                s.modified_date >= c.modified_date - interval 2 minutes AND
                s.modified_date <= c.modified_date + interval 2 minutes
            """),
            "left"
        )
        .select(
            col("c.customer_id").alias("store_key"),
            col("c.account_number").alias("store_account_number"),
            col("s.name").alias("store_name"),
            greatest(col("c.modified_date"), col("s.modified_date")).alias("store_last_modified_date")
        )
    )