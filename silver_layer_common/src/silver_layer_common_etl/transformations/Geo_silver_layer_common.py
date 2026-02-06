from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.table(
    name="Fact_weather",
    comment="Tabla de hechos climática. Geografía vía streaming y métricas NOAA sin filtros temporales."
)

def Fact_weather():
    # 1. Lectura de Geografía en Streaming (stg_person)
    # Se leen como stream para capturar cambios en direcciones o divisiones políticas
    df_address_stream = spark.read.table("dev_bronze.stg_person.stg_address").alias("a")
    df_state_stream = spark.read.table("dev_bronze.stg_person.stg_stateprovince").alias("s")
    df_country_stream = spark.read.table("dev_bronze.stg_person.stg_countryregion").alias("c")

    # Reconstrucción de la dimensión geográfica usando los alias
    df_geo_stream = df_address_stream \
        .join(df_state_stream, F.col("a.StateProvinceID") == F.col("s.StateProvinceID")) \
        .join(df_country_stream, F.col("s.CountryRegionCode") == F.col("c.CountryRegionCode")) \
        .select(
            F.col("a.City").alias("city"),
            F.col("a.PostalCode").alias("postal_code"),
            F.col("c.Name").alias("country_region") # El nombre del país viene de la tabla 'c'
        )

    # 2. Lectura de Clima (Estático - stg_noaa)
    df_timeseries = spark.read.table("dev_bronze.stg_noaa.raw_noaa_weather_metrics_timeseries")
    df_stations = spark.read.table("dev_bronze.stg_noaa.raw_noaa_weather_us_stations")

    # 3. Lógica de Negocio: Refinado de Clima (weather)
    # SIN filtro de años. Se procesa toda la data histórica disponible.
    weather_refined = df_timeseries.join(
            df_stations, 
            "noaa_weather_station_id"
        ) \
        .filter(
            F.col("variable").isin(
                'average_temperature', 'average_wind_speed', 
                'average_relative_humidity', 'precipitation', 'snowfall'
            )
        ) \
        .withColumn("variable", F.upper(F.col("variable"))) \
        .groupBy("state_name", "country_name", "zip_name", "date", "variable") \
        .agg(F.avg("value").alias("avg_value"))

    # 4. Join Stream-Static y Pivotado
    # El resultado final fluye como un stream hacia la tabla Delta
    fact_weather_stream = df_geo_stream.join(
            weather_refined,
            (df_geo_stream.country_region == weather_refined.country_name) &
            (df_geo_stream.postal_code == weather_refined.zip_name),
            "inner"
        ) \
        .groupBy("city", "postal_code", "date") \
        .pivot("variable", [
            "AVERAGE_TEMPERATURE", 
            "AVERAGE_WIND_SPEED", 
            "AVERAGE_RELATIVE_HUMIDITY", 
            "PRECIPITATION", 
            "SNOWFALL"
        ]) \
        .avg("avg_value")

    return fact_weather_stream