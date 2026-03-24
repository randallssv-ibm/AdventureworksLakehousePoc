from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql.functions import col


# ============================================================
# CONCEPTUAL REQUIREMENT (from demoSNOW/marts/weather/fact_weather.sql)
#
# Snowflake mart joined int_common__fact_weather (aggregated at
# zip_name + city grain) with dim_geography on postal_code + city
# to resolve geography_key.
#
# GRAIN DIFFERENCE: The silver baseline (commonsilver.climate.fact_weather
# from silver_layer_common/Geo_silver_layer_common.py) aggregates at
# state level (state_geo_id), NOT at postal_code + city level.
#
# To bridge this, we use a state_geography_mapping temporary table
# that derives a representative geography_key per US state from
# personsilver.address.dim_geo. This follows the same pattern as
# geography_mapping in gold_layer_commercial.
#
# NOTE: average_relative_humidity is present in the Snowflake model
# (variable: 'average_relative_humidity') but is NOT produced by the
# current silver baseline (only TAVG, AWND, PRCP, SNOW are aggregated).
# Add humidity when silver is enhanced to include AWRH.
# ============================================================


@dp.temporary_view(name="state_geography_mapping")
def state_geography_mapping():
    # dim_geo already has state_province_code (2-letter, e.g. "WA", "OR")
    # which matches state_geo_id in commonsilver.climate.fact_weather directly.
    # Aggregate to state level: one representative geography_key per state code.
    return (
        spark.read.table("personsilver.address.dim_geo")
        .filter(F.col("country_region") == "United States")
        .groupBy(F.col("state_province_code"))
        .agg(F.max(F.col("geography_key")).alias("geography_key"))
        .withColumnRenamed("state_province_code", "state_geo_id")
    )


@dp.table(name="fact_weather_gold")
def fact_weather_gold():
    fw  = spark.read.table("commonsilver.climate.fact_weather").alias("fw")
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
