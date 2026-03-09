from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

@dp.table(
    name="fact_sales_batch",
    comment="Fact table with sales orders - raw stream before apply_changes"
)
def fact_sales_raw():
    # 1. Header Stream (Watermark 5 min)
    salesOrderHeader = (
        spark.readStream.table("adventureworksbronze.sales.salesorderheader")
        .withWatermark("ModifiedDate", "5 minutes")
        .alias("soh")
    )

    # 2. Detail Stream (Watermark 5 min)
    salesOrderDetail = (
        spark.readStream.table("adventureworksbronze.sales.salesorderdetail")
        .withWatermark("ModifiedDate", "5 minutes")
        .alias("sod")
    )

    # 3. Address Stream (Watermark 5 min)
    address = (
        spark.readStream.table("adventureworksbronze.person.address")
        .withWatermark("ModifiedDate", "5 minutes")
        .alias("a")
    )

    # Join with time constraints for stateful stream processing
    dfJoined = (
        salesOrderDetail.join(
            salesOrderHeader,
            expr("""
                sod.SalesOrderID = soh.SalesOrderID 
                AND soh.ModifiedDate >= sod.ModifiedDate - interval 2 minutes 
                AND soh.ModifiedDate <= sod.ModifiedDate + interval 5 minutes
            """),
            "left"
        ).join(
            address,
            expr("""
                soh.BillToAddressID = a.AddressID AND
                a.ModifiedDate >= soh.ModifiedDate - interval 2 minutes AND
                a.ModifiedDate <= soh.ModifiedDate + interval 5 minutes
            """),
            "left"
        )
    )

    # Final projection in snake_case
    return dfJoined.select(
        col("sod.SalesOrderID").alias("sales_order_id"),
        col("sod.SalesOrderDetailID").alias("sales_order_detail_id"),
        col("sod.ProductID").alias("product_key"),
        F.to_date(col("soh.OrderDate")).alias("order_date"),
        F.concat(col("a.AddressID"), F.lit("_"), col("a.PostalCode")).alias("geography_key"),
        col("soh.Status").alias("order_status"),
        col("soh.CustomerID").alias("customer_id"),
        col("soh.SalesPersonID").alias("sales_person_id"),
        col("sod.OrderQty").alias("order_qty"),
        col("sod.UnitPrice").alias("unit_price"),
        col("sod.UnitPriceDiscount").alias("unit_price_discount"),
        col("sod.LineTotal").alias("line_total"),
        col("soh.SubTotal").alias("sub_total"),
        col("soh.TaxAmt").alias("tax_amt"),
        col("soh.Freight").alias("freight"),
        col("soh.TotalDue").alias("total_due"),
        col("soh.ModifiedDate").alias("modified_date")  # needed as sequence for apply_changes
    )


# SCD Type 1 — overwrites existing rows on key match (no history retained)
dp.apply_changes(
    target="fact_sales",
    source="fact_sales_batch",
    keys=["sales_order_detail_id"],           # grain of the fact: one row per detail line
    sequence_by="modified_date",              # latest ModifiedDate wins
    apply_as_truncate_and_insert=False,        # SCD1: upsert, not full reload
    stored_as_scd_type=1,                     # overwrite on change
    comment="Fact table with sales orders, SCD Type 1 upsert via apply_changes"
)