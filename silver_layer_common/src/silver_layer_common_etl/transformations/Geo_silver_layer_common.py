from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.table(
    name="fact_weather",
    comment="Silver layer weather fact table aggregated by state and date"
)
def fact_weather():
    #aggregate by state_geo_id and date
    ts = spark.read.table("dev_bronze.stg_noaa.raw_noaa_weather_metrics_timeseries").alias("ts")
    idx = spark.read.table("dev_bronze.stg_noaa.raw_noaa_weather_us_stations").alias("idx")
    
    weather = ts.join(
        idx,
        F.col("ts.noaa_weather_station_id") == F.col("idx.noaa_weather_station_id"),
        "inner"
    ).filter((F.col("ts.variable").isin(["TAVG", "AWND", "PRCP", "SNOW"]))
    ).groupBy(
        F.col("idx.state_geo_id"),
        F.col("ts.date"),
        F.upper(F.col("ts.variable")).alias("variable")
    ).agg(
        F.avg(F.col("ts.value")).alias("avg_value")
    )
    
    # Pivot
    pivot = weather.groupBy("state_geo_id", "date").pivot(
        "variable",
        ["TAVG", "AWND", "PRCP", "SNOW"]
    ).agg(
        F.avg("avg_value")
    )
    
    return pivot.select(
        F.col("state_geo_id"),
        F.col("date"),
        F.col("TAVG").alias("average_temperature"),
        F.col("AWND").alias("average_wind_speed"),
        F.col("PRCP").alias("precipitation"),
        F.col("SNOW").alias("snowfall")
    )