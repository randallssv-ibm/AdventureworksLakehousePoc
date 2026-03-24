from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql.functions import col



@dp.temporary_view(name="state_geography_mapping")
def state_geography_mapping():
    dim_geo = spark.read.table("personsilver.address.dim_geo")
    return (
        dim_geo.filter(F.col("country_region") == "United States")
        .groupBy(F.col("state_province_code"))
        .agg(F.max(F.col("geography_key")).alias("geography_key"))
        .withColumnRenamed("state_province_code", "state_geo_id")
    )



@dp.table(name="fact_weather_gold")
def fact_weather_gold():
    # Silver weather fact (state + date grain)
    fw  = spark.read.table("commonsilver.climate.fact_weather").alias("fw")

    # State-level geography lookup (temporary table defined above)
    sgm = spark.read.table("state_geography_mapping").alias("sgm")


    return (
        fw
        .join(sgm, fw.state_geo_id == sgm.state_geo_id, "left")
        .select(
            F.col("sgm.geography_key"),
            F.col("fw.state_geo_id"),           # retain for lineage / debugging
            F.col("fw.date").alias("weather_date"),
            F.col("fw.average_temperature"),
            F.col("fw.average_wind_speed"),
            F.col("fw.precipitation"),
            F.col("fw.snowfall"),
            # average_relative_humidity: not available until silver includes AWRH variable
        )
    )
