# weather_bronze_ingestion.py
from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql.types import *

# ============================================================
# TABLE 1: NOAA Weather Station Index
# ============================================================
@dp.table(
    name="raw_noaa_weather_station_index",
    comment="NOAA weather station index - matches Snowflake structure"
)
def noaa_weather_station_index():
    """
    Read NOAA GHCN-D station metadata from AWS Open Data
    Maps to Snowflake's noaa_weather_station_index
    
    Format from ghcnd-stations.txt:
    ID:          1-11   (Character)
    LATITUDE:   13-20   (Real)
    LONGITUDE:  22-30   (Real)
    ELEVATION:  32-37   (Real)
    STATE:      39-40   (Character)
    NAME:       42-71   (Character)
    GSN FLAG:   73-75   (Character)
    HCN/CRN:    77-79   (Character)
    WMO ID:     81-85   (Character)
    """
    
    # Read stations file (fixed-width format)
    df_stations_raw = spark.read.text("s3://noaa-ghcn-pds/ghcnd-stations.txt")
    
    # Parse fixed-width format according to official documentation
    df_stations = df_stations_raw.select(
        F.trim(F.substring("value", 1, 11)).alias("station_id"),
        F.substring("value", 13, 8).cast("double").alias("latitude"),
        F.substring("value", 22, 9).cast("double").alias("longitude"),
        F.substring("value", 32, 6).cast("double").alias("elevation"),
        F.trim(F.substring("value", 39, 2)).alias("state"),
        F.trim(F.substring("value", 42, 30)).alias("name"),
        F.trim(F.substring("value", 73, 3)).alias("gsn_flag"),
        F.trim(F.substring("value", 77, 3)).alias("hcn_crn_flag"),
        F.trim(F.substring("value", 81, 5)).alias("wmo_id")
    )
    
    # Transform to match Snowflake schema
    return df_stations.select(
        F.col("station_id").alias("noaa_weather_station_id"),
        F.col("name").alias("noaa_weather_station_name"),
        F.substring(F.col("station_id"), 1, 2).alias("country_geo_id"),
        F.when(F.substring(F.col("station_id"), 1, 2) == "US", F.lit("United States"))
         .otherwise(F.lit("Other")).alias("country_name"),
        F.col("state").alias("state_geo_id"),
        F.col("state").alias("state_name"),
        F.lit(None).cast("string").alias("zip_geo_id"),
        F.lit(None).cast("string").alias("zip_name"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("elevation"),
        F.col("gsn_flag").alias("weather_station_network"),
        F.concat_ws(",", 
            F.when(F.col("gsn_flag").isNotNull(), F.col("gsn_flag")).otherwise(F.lit("")),
            F.when(F.col("hcn_crn_flag").isNotNull(), F.col("hcn_crn_flag")).otherwise(F.lit(""))
        ).alias("associated_networks"),
        F.col("wmo_id").alias("world_meteorological_organization_id"),
        F.lit("NOAA GHCN-D").alias("source_data")
    )


# ============================================================
# TABLE 2: NOAA Weather Station Inventory
# ============================================================
@dp.table(
    name="raw_noaa_weather_inventory",
    comment="NOAA weather station data availability inventory"
)
def noaa_weather_inventory():
    """
    Read NOAA GHCN-D inventory from AWS Open Data
    Shows which stations have which weather variables and date ranges
    
    Format from ghcnd-inventory.txt:
    ID:        1-11   (Character)
    LATITUDE: 13-20   (Real)
    LONGITUDE:22-30   (Real)
    ELEMENT:  32-35   (Character)
    FIRSTYEAR:37-40   (Integer)
    LASTYEAR: 42-45   (Integer)
    """
    
    # Read inventory file (fixed-width format)
    df_inventory_raw = spark.read.text("s3://noaa-ghcn-pds/ghcnd-inventory.txt")
    
    # Parse fixed-width format according to official documentation
    return df_inventory_raw.select(
        F.trim(F.substring("value", 1, 11)).alias("station_id"),
        F.substring("value", 13, 8).cast("double").alias("latitude"),
        F.substring("value", 22, 9).cast("double").alias("longitude"),
        F.trim(F.substring("value", 32, 4)).alias("element"),
        F.substring("value", 37, 4).cast("int").alias("first_year"),
        F.substring("value", 42, 4).cast("int").alias("last_year")
    )


# ============================================================
# TABLE 3: NOAA Weather Metrics Timeseries
# ============================================================
@dp.table(
    name="raw_noaa_weather_metrics_timeseries",
    comment="NOAA daily weather observations - matches Snowflake structure"
)
def noaa_weather_metrics_timeseries():
    """
    Read NOAA GHCN-D daily weather data from AWS Open Data
    Maps to Snowflake's noaa_weather_metrics_timeseries
    
    Note: Using by_year CSV format for easier parsing.
    Full dataset is 30GB+. For POC, reading 2023 data only.
    """
    
    # Read from by_year directory (CSV format)
    df_weather_raw = spark.read.csv(
        "s3://noaa-ghcn-pds/csv/by_year/2023.csv",
        header=False,
        schema=StructType([
            StructField("station_id", StringType(), False),
            StructField("date", StringType(), False),
            StructField("element", StringType(), False),
            StructField("value", IntegerType(), True),
            StructField("mflag", StringType(), True),
            StructField("qflag", StringType(), True),
            StructField("sflag", StringType(), True),
            StructField("obs_time", StringType(), True)
        ])
    )
    
    # Transform to match Snowflake schema
    return df_weather_raw.select(
        F.col("station_id").alias("noaa_weather_station_id"),
        F.col("element").alias("variable"),
        # Variable name mapping using when/otherwise chain
        F.when(F.col("element") == "TMAX", F.lit("Maximum Temperature"))
         .when(F.col("element") == "TMIN", F.lit("Minimum Temperature"))
         .when(F.col("element") == "PRCP", F.lit("Precipitation"))
         .when(F.col("element") == "SNOW", F.lit("Snowfall"))
         .when(F.col("element") == "SNWD", F.lit("Snow Depth"))
         .when(F.col("element") == "AWND", F.lit("Average Wind Speed"))
         .when(F.col("element") == "TAVG", F.lit("Average Temperature"))
         .when(F.col("element") == "WSFG", F.lit("Peak Gust Wind Speed"))
         .when(F.col("element") == "ACMC", F.lit("Average Cloudiness"))
         .when(F.col("element") == "AWDR", F.lit("Average Wind Direction"))
         .otherwise(F.col("element")).alias("variable_name"),
        F.to_date(F.col("date"), "yyyyMMdd").alias("date"),
        F.to_timestamp(F.col("date"), "yyyyMMdd").alias("datetime"),
        # Convert values according to GHCN-D documentation:
        # Temperature elements use tenths of degrees C
        # Precipitation uses tenths of mm
        F.when(F.col("element").isin(["TMAX", "TMIN", "TAVG", "TOBS", "AWBT", "ADPT"]), 
               F.col("value") / 10.0)
         .when(F.col("element").isin(["PRCP", "EVAP", "MDPR", "MDEV", "WESF", "WESD"]), 
               F.col("value") / 10.0)
         .when(F.col("element").isin(["AWND", "WSF1", "WSF2", "WSF5", "WSFG", "WSFI", "WSFM"]),
               F.col("value") / 10.0)
         .otherwise(F.col("value").cast("double")).alias("value"),
        # Units mapping
        F.when(F.col("element").isin(["TMAX", "TMIN", "TAVG", "TOBS", "AWBT", "ADPT"]), 
               F.lit("Celsius"))
         .when(F.col("element").isin(["PRCP", "EVAP", "MDPR", "MDEV", "WESF", "WESD"]), 
               F.lit("mm"))
         .when(F.col("element").isin(["SNOW", "SNWD"]), 
               F.lit("mm"))
         .when(F.col("element").isin(["AWND", "WSF1", "WSF2", "WSF5", "WSFG", "WSFI", "WSFM"]),
               F.lit("m/s"))
         .when(F.col("element").isin(["ACMC", "ACMH", "ACSC", "ACSH", "PSUN"]),
               F.lit("percent"))
         .otherwise(F.lit("unknown")).alias("unit")
    )


# ============================================================
# OPTIONAL: Filter for US States Only
# ============================================================
@dp.table(
    name="raw_noaa_weather_us_stations",
    comment="US-only weather stations for faster POC"
)
def noaa_weather_us_stations():
    """
    Filter stations to US-only for AdventureWorks correlation
    """
    df_stations = spark.read.table("raw_noaa_weather_station_index")
    
    # Filter US stations (country_geo_id = 'US' and state is not null)
    return df_stations.filter(
        (F.col("country_geo_id") == "US") & 
        (F.col("state_geo_id").isNotNull())
    )