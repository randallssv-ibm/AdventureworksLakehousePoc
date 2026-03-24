from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.table(name="dim_geography")
def dim_geography():
    # Silver dim_geo is at address level (one row per address_id).
    # Gold deduplicates to one row per city + postal_code combination,
    # taking MAX geography_key as the canonical key — consistent with
    # the Snowflake intermediate int_person__dim_geography.sql pattern.
    dim_geo = spark.read.table("personsilver.address.dim_geo")
    return (
        dim_geo
        .groupBy("city", "state_province", "state_province_code", "country_region", "postal_code")
        .agg(F.max(col("geography_key")).alias("geography_key"))
        .select(
            col("geography_key"),
            col("city"),
            col("state_province"),
            col("state_province_code"),
            col("country_region"),
            col("postal_code"),
        )
    )
