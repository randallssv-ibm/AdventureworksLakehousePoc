from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


# ============================================================
# STEP 1: Staging views
# ============================================================

@dp.temporary_view(
    name="fact_sales_snapshot",
    comment="One-time static batch join for initial bulk load — no watermarks"
)
def fact_sales_snapshot():
    """
    Used only by the once=True flow below.
    Reads bronze tables as static batch to avoid watermark/state issues
    that prevent historical data from emitting during stream-stream joins.
    """
    salesOrderDetail = spark.read.table("adventureworksbronze.sales.salesorderdetail").alias("sod")
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


@dp.temporary_view(
    name="fact_sales_stream",
    comment="Incremental stream-stream join for ongoing CDC after initial load"
)
def fact_sales_stream():
    """
    Used by the ongoing CDC flow.
    Stream-stream join with watermarks for incremental processing.
    """
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


# ============================================================
# STEP 2: CDC target
# ============================================================

@dp.table(
    name="fact_sales",
    comment="Silver fact table — AdventureWorks sales transactions joined with order headers and billing addresses",
    table_properties={"quality": "silver"},
)
def fact_sales():
    # Schema-only stub — rows are written exclusively by create_auto_cdc_flow.
    # limit(0) registers the schema without inserting data from this function.
    return spark.read.table("fact_sales_batch").limit(0)

    
# ============================================================
# STEP 3a: One-time bulk load flow (runs once, then never again)
# ============================================================

dp.create_auto_cdc_flow(
    name="fact_sales_initial_load",
    target="fact_sales",
    source="fact_sales_snapshot",
    keys=["sales_order_detail_id"],
    sequence_by=col("modified_date"),
    stored_as_scd_type=1,
    once=True,  # runs exactly once for initial hydration
)


# ============================================================
# STEP 3b: Ongoing incremental CDC flow
# ============================================================

dp.create_auto_cdc_flow(
    name="fact_sales_incremental",
    target="fact_sales",
    source="fact_sales_stream",
    keys=["sales_order_detail_id"],
    sequence_by=col("modified_date"),
    stored_as_scd_type=1,
)