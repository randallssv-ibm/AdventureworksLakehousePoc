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


@dp.temporary_table(
    name="state_geography_mapping",
    quality="silver",
    description=(
        "Maps US state abbreviation to a representative geography_key from dim_geo. "
        "Bridges the state-level NOAA grain in silver to the address-level geography dimension. "
        "Uses MAX geography_key per state to pick a deterministic representative key."
    ),
)
def state_geography_mapping():
    # dim_geo has city/postal_code level geography; we aggregate to state level
    # state_province in dim_geo holds the 2-letter abbreviation (e.g. 'WA', 'CA')
    dg = spark.read.table("personsilver.address.dim_geo")

    return (
        dg
        .filter(F.col("country_region") == "United States")
        .groupBy(F.col("state_province"))
        .agg(F.max(F.col("geography_key")).alias("geography_key"))
        .withColumnRenamed("state_province", "state_abbr")
    )


@dp.table(
    name="fact_weather_gold",
    quality="gold",
    description=(
        "Gold weather fact table — NOAA climate observations enriched with geography dimension key. "
        "Grain: one row per US state per date. "
        "Source: commonsilver.climate.fact_weather joined with state_geography_mapping."
    ),
)
def fact_weather_gold():
    # Silver weather fact (state + date grain)
    fw  = spark.read.table("commonsilver.climate.fact_weather").alias("fw")

    # State-level geography lookup (temporary table defined above)
    sgm = spark.read.table("state_geography_mapping").alias("sgm")

    # state_geo_id in the NOAA data has format "state/US-WA"
    # Extract the 2-letter state abbreviation from the trailing segment
    fw_with_state = fw.withColumn(
        "state_abbr",
        F.regexp_extract(F.col("fw.state_geo_id"), r"([A-Z]{2})$", 1)
    )

    return (
        fw_with_state
        .join(sgm, F.col("state_abbr") == F.col("sgm.state_abbr"), "left")
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
