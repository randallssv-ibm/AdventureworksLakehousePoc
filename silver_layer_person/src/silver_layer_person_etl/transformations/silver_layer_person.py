from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name = "dim_person",
    comment="Dimensional model for Person data."
)
def sample_trips_silver_layer_person():
    # 1. Setup Streaming Reads
    address = (
        spark.readStream.table("dev_bronze.stg_person.stg_address")
        .withWatermark("ModifiedDate", "5 minutes")
        .alias("a")
    )
    state_province = (
        spark.readStream.table("dev_bronze.stg_person.stg_stateprovince")
        .withWatermark("ModifiedDate", "5 minutes")
        .alias("sp")
    )
    country_region = (
        spark.readStream.table("dev_bronze.stg_person.stg_countryregion")
        .withWatermark("ModifiedDate", "5 minutes")
        .alias("cr")
    )

    # 2. Join and Aggregate
    # In PySpark, we use .agg() for the max() logic and .alias() for renaming
    return (
        address
        .join(
            state_province,
            F.col("a.StateProvinceID") == F.col("sp.StateProvinceID"),
            "left"
        )
        .join(
            country_region,
            F.col("sp.CountryRegionCode") == F.col("cr.CountryRegionCode"),
            "left"
        )
        .groupBy(
            F.col("a.City"),
            F.col("sp.Name").alias("state_province"),
            F.col("sp.StateProvinceCode").alias("satate_province_code"),,
            F.col("cr.Name").alias("country_region"),
            F.col("a.PostalCode").alias("postal_code"),
        )
        .agg(
            F.max(
                F.concat(F.col("a.AddressID"), F.lit("_"), F.col("a.PostalCode"))
            ).alias("geography_key")
        )
    )