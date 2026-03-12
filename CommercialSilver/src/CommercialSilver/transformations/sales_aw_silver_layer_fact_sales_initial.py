from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql.functions import col

spark.conf.set("pipelines.incompatibleViewCheck.enabled", "false")


@dp.temporary_view(
    name="fact_sales_snapshot",
    comment="One-time static batch join for initial bulk load — no watermarks"
)
def fact_sales_snapshot():
    salesOrderDetail = spark.readStream.table("adventureworksbronze.sales.salesorderdetail").alias("sod")
    salesOrderHeader = spark.read.table("adventureworksbronze.sales.salesorderheader").alias("soh")
    address = spark.read.table("adventureworksbronze.person.address").alias("a")

    return (
        salesOrderDetail
        .join(salesOrderHeader, col("sod.SalesOrderID") == col("soh.SalesOrderID"), "left")
        .join(address, col("soh.BillToAddressID") == col("a.AddressID"), "left")
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
    source="fact_sales_snapshot",
    keys=["sales_order_detail_id"],
    sequence_by=col("modified_date"),
    stored_as_scd_type=1,
)