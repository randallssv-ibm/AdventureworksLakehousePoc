from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

@dp.table(
    name="fact_sales2",
    comment="Fact table with sales orders"
)
def fact_sales():
    # 1. Header Stream (Watermark 5 min)
    salesOrderHeader = (
        spark.read.table("adventureworksbronze.sales.salesorderheader")
        .alias("soh")
    )

    # 2. Detail Stream (Watermark 5 min)
    salesOrderDetail = (
        spark.read.table("adventureworksbronze.sales.salesorderdetail")
        .alias("sod")
    )

    # 3. Address Stream (Watermark 5 min)
    address = (
        spark.read.table("adventureworksbronze.person.address")
        .alias("a")
    )

    # Join con restricciones de tiempo para manejo de estado en memoria
    dfJoined = (
        salesOrderDetail.join(
            salesOrderHeader,
            expr("""
                sod.SalesOrderID = soh.SalesOrderID 
            """),
            "left"
        ).join(
            address,
            expr("""
                soh.BillToAddressID = a.AddressID 
            """),
            "left"
        )
    )

    # Proyección Final: Nombres de salida en snake_case
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
        col("soh.TotalDue").alias("total_due")
    )