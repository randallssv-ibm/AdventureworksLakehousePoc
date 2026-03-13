from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


# ============================================================
# CDC PIPELINE — runs continuously
# Stream-stream join with watermarks and time-bounded intervals.
# ============================================================

@dp.temporary_view(
    name="fact_sales_batch",
    comment="Stream-stream join of sales orders, headers and billing address with watermark intervals"
)
def fact_sales_batch():
    salesOrderDetail = (
        spark.readStream.table("adventureworksbronze.sales.salesorderdetail")
        .withWatermark("ModifiedDate", "5 minutes")
        .alias("sod")
    )
    salesOrderHeader = (
        spark.readStream.table("adventureworksbronze.sales.salesorderheader")
        .withWatermark("ModifiedDate", "5 minutes")
        .alias("soh")
    )
    address = (
        spark.readStream.table("adventureworksbronze.person.address")
        .withWatermark("ModifiedDate", "5 minutes")
        .alias("a")
    )

    return (
        salesOrderDetail
        .join(
            salesOrderHeader,
            expr("""
                sod.SalesOrderID = soh.SalesOrderID
                AND soh.ModifiedDate >= sod.ModifiedDate - interval 2 minutes
                AND soh.ModifiedDate <= sod.ModifiedDate + interval 5 minutes
            """),
            "left",
        )
        .join(
            address,
            expr("""
                soh.BillToAddressID = a.AddressID
                AND a.ModifiedDate >= soh.ModifiedDate - interval 2 minutes
                AND a.ModifiedDate <= soh.ModifiedDate + interval 5 minutes
            """),
            "left",
        )
        .select(
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
            col("soh.ModifiedDate").alias("modified_date"),
        )
    )


dp.create_streaming_table(
    name="fact_sales",
    comment="Silver fact table — AdventureWorks sales transactions joined with order headers and billing addresses",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="fact_sales",
    source="fact_sales_batch",
    keys=["sales_order_detail_id"],
    sequence_by=col("modified_date"),
    stored_as_scd_type=1,
)