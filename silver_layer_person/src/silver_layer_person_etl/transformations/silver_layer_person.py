from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name = "dim_geo",
    comment= """provides a detailed view of geographical information related
    to individuals, including city, state, province, and country details. It is used to enrich personal data with geographical context, aiding in location-based analysis and reporting."""
)
def silver_layer_person():

    #readings from bronze
    # 1. ONLY Address is a stream (The driving table)
    address = (
        spark.readStream.table("dev_bronze.stg_person.stg_address")
        .withWatermark("ModifiedDate", "5 minutes")
        .alias("a")
    )
    state_province = dp.read("dev_bronze.stg_person.stg_stateprovince").alias("sp")
    country_region = dp.read("dev_bronze.stg_person.stg_countryregion").alias("cr")

    # 3. Join and Select
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
        .select(
            F.concat(F.col("a.AddressID"), F.lit("_"), F.col("a.PostalCode")).alias("geography_key"),
            F.col("a.City").alias("city"),
            F.col("sp.Name").alias("state_province"),
            F.col("sp.StateProvinceCode").alias("state_province_code"),
            F.col("cr.Name").alias("country_region"),
            F.col("a.PostalCode").alias("postal_code")
        )
    )