# weather_bronze_ingestion.py
from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql.types import *

# ============================================================
# SCHEMAS - Matching GHCN-D Fixed-Width Format
# ============================================================

# Station metadata schema (ghcnd-stations.txt)
# Format: ID, LATITUDE, LONGITUDE, ELEVATION, STATE, NAME, GSN FLAG, HCN/CRN FLAG, WMO ID
station_schema = StructType([
    StructField("station_id", StringType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("elevation", DoubleType(), True),
    StructField("state", StringType(), True),
    StructField("name", StringType(), True),
    StructField("gsn_flag", StringType(), True),
    StructField("hcn_crn_flag", StringType(), True),
    StructField("wmo_id", StringType(), True)
])

# Inventory schema (ghcnd-inventory.txt)
# Format: ID, LATITUDE, LONGITUDE, ELEMENT, FIRSTYEAR, LASTYEAR
inventory_schema = StructType([
    StructField("station_id", StringType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("element", StringType(), True),
    StructField("first_year", IntegerType(), True),
    StructField("last_year", IntegerType(), True)
])

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
    """
    
    # Read stations file (fixed-width format)
    df_stations_raw = spark.read.text("s3://noaa-ghcn-pds/ghcnd-stations.txt")
    
    # Parse fixed-width format
    # Positions: 1-11 (ID), 13-20 (LAT), 22-30 (LON), 32-37 (ELEV), 39-40 (STATE), 42-71 (NAME)
    df_stations = df_stations_raw.select(
        F.substring("value", 1, 11).alias("station_id"),
        F.substring("value", 13, 8).cast("double").alias("latitude"),
        F.substring("value", 22, 9).cast("double").alias("longitude"),
        F.substring("value", 32, 6).cast("double").alias("elevation"),
        F.substring("value", 39, 2).alias("state"),
        F.trim(F.substring("value", 42, 30)).alias("name"),
        F.substring("value", 73, 3).alias("gsn_flag"),
        F.substring("value", 77, 3).alias("hcn_crn_flag"),
        F.substring("value", 81, 5).alias("wmo_id")
    )
    
    # Transform to match Snowflake schema
    return df_stations.select(
        F.col("station_id").alias("noaa_weather_station_id"),
        F.col("name").alias("noaa_weather_station_name"),
        F.lit("US").alias("country_geo_id"),
        F.lit("United States").alias("country_name"),
        F.col("state").alias("state_geo_id"),
        F.col("state").alias("state_name"),  # Would need lookup table for full name
        F.lit(None).cast("string").alias("zip_geo_id"),
        F.lit(None).cast("string").alias("zip_name"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("elevation"),
        F.col("gsn_flag").alias("weather_station_network"),
        F.concat_ws(",", F.col("gsn_flag"), F.col("hcn_crn_flag")).alias("associated_networks"),
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
    """
    
    # Read inventory file (fixed-width format)
    df_inventory_raw = spark.read.text("s3://noaa-ghcn-pds/ghcnd-inventory.txt")
    
    # Parse fixed-width format
    # Positions: 1-11 (ID), 13-20 (LAT), 22-30 (LON), 32-35 (ELEMENT), 37-40 (FIRSTYEAR), 42-45 (LASTYEAR)
    return df_inventory_raw.select(
        F.substring("value", 1, 11).alias("station_id"),
        F.substring("value", 13, 8).cast("double").alias("latitude"),
        F.substring("value", 22, 9).cast("double").alias("longitude"),
        F.substring("value", 32, 4).alias("element"),
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
    
    Note: Full dataset is 30GB+. For POC, filter to specific year(s) or states.
    """
    
    # For POC: Read specific year (e.g., 2023)
    # Full path pattern: s3://noaa-ghcn-pds/csv/by_year/YYYY.csv
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
        # Fixed: Use when/otherwise chain instead of dictionary lookup
        F.when(F.col("element") == "TMAX", F.lit("Maximum Temperature"))
         .when(F.col("element") == "TMIN", F.lit("Minimum Temperature"))
         .when(F.col("element") == "PRCP", F.lit("Precipitation"))
         .when(F.col("element") == "SNOW", F.lit("Snowfall"))
         .when(F.col("element") == "SNWD", F.lit("Snow Depth"))
         .when(F.col("element") == "AWND", F.lit("Average Wind Speed"))
         .when(F.col("element") == "TAVG", F.lit("Average Temperature"))
         .when(F.col("element") == "WSFG", F.lit("Peak Gust Wind Speed"))
         .otherwise(F.col("element")).alias("variable_name"),
        F.to_date(F.col("date"), "yyyyMMdd").alias("date"),
        F.to_timestamp(F.col("date"), "yyyyMMdd").alias("datetime"),
        # Convert values (GHCN-D uses tenths of degrees/mm)
        F.when(F.col("element").isin(["TMAX", "TMIN", "TAVG"]), 
               F.col("value") / 10.0)
         .when(F.col("element") == "PRCP", 
               F.col("value") / 10.0)
         .otherwise(F.col("value").cast("double")).alias("value"),
        # Units
        F.when(F.col("element").isin(["TMAX", "TMIN", "TAVG"]), F.lit("Celsius"))
         .when(F.col("element") == "PRCP", F.lit("mm"))
         .when(F.col("element").isin(["SNOW", "SNWD"]), F.lit("mm"))
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
    
    # Filter US stations (state_geo_id is not null)
    return df_stations.filter(
        F.col("state_geo_id").isNotNull()
    )