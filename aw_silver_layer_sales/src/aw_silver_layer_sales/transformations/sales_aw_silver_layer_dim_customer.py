from pyspark import pipelines as dp
from pyspark.sql.functions import col, concat_ws, trim, regexp_replace, coalesce, greatest, when, lit

@dp.table(
    name="dim_customer",
    comment="Dimensional model for Individual Customers, joining Bronze Staging Sales and Person data."
)
def dim_individual_customer():
    # 1. Read from the bronze layer tables 
    customer_df = spark.readStream.option("readChangeFeed", "True").table("dev_bronze.stg_sales.stg_customer")
    person_df = spark.readStream.option("readChangeFeed", "True").table("dev_bronze.stg_person.stg_person")

    # 2. Join and Transform 
    return (
        customer_df.alias("c").filter(col("PersonID").isNotNull())
        .join(person_df.alias("p"), col("c.PersonID") == col("p.BusinessEntityID"), "inner")
        .select(
            col("c.CustomerID").alias("customer_key"),
            col("c.AccountNumber").alias("customer_account_number"),
            
            # String manipulation refactored for Spark SQL performance
            trim(regexp_replace(
                concat_ws(" ", 
                    coalesce(col("p.FirstName"), lit("")), 
                    coalesce(col("p.MiddleName"), lit("")), 
                    coalesce(col("p.LastName"), lit(""))
                ), "\\s+", " ")
            ).alias("customer_full_name"),
            
            col("p.FirstName").alias("customer_first_name"),
            col("p.LastName").alias("customer_last_name"),
            col("p.Suffix").alias("customer_suffix"),
            col("p.Title").alias("customer_title"),
            col("p.PersonType").alias("customer_type"),
            
            # Mapping Business Logic
            when(col("p.PersonType") == "SC", "Store Contact")
            .when(col("p.PersonType") == "IN", "Individual Retail Customer")
            .when(col("p.PersonType") == "SP", "Sales Person")
            .when(col("p.PersonType") == "EM", "Employee (Non-Sales)")
            .when(col("p.PersonType") == "VC", "Vendor Contact")
            .when(col("p.PersonType") == "GC", "General Contact")
            .otherwise("Unknown").alias("customer_type_description"),
            
            col("c.ModifiedDate").alias("customer_modified_date"),
            greatest(col("p.ModifiedDate"), col("c.ModifiedDate")).alias("customer_last_modified_date")
        )
    )