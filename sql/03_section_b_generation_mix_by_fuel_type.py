from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


spark = (
    SparkSession.builder
    .appName("NEM Assessment Ingestion")
    .getOrCreate()
)


# =========================
# BRONZE LAYER
# =========================

unit_dispatch_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("nem_data/raw_unit_dispatch.csv")
    .withColumn("source_file_name", F.input_file_name())
    .withColumn("ingested_at", F.current_timestamp())
)


# =========================
# SILVER LAYER
# =========================

unit_dispatch_interval = (
    unit_dispatch_raw
    .withColumn("interval_datetime", F.to_timestamp("interval_datetime"))
    .withColumn("dispatch_date", F.to_date("interval_datetime"))
    .select(
        "interval_datetime",
        "dispatch_date",
        "availability_mw",
        "fuel_type",
        "region_id",
        "duid",
        F.col("dispatch_mw").cast("double").alias("dispatch_mw"),
        "source_file_name",
        "ingested_at"
    ) 
)


# =========================
# GOLD LAYER
# =========================

generation_mix_by_fuel_type_base = (
    unit_dispatch_interval
    .withColumn(
        "fuel_type_category",
        F.when(
            F.col("fuel_type").isin("WIND", "SOLAR_UTILITY", "HYDRO"),
            "RENEWABLES"
        ).otherwise(F.col("fuel_type"))
    )
    .groupBy("region_id", "fuel_type_category")
    .agg(
        F.sum("dispatch_mw").alias("total_dispatch_mw")
    )
)

region_window = Window.partitionBy("region_id")

generation_mix_by_fuel_type_v = (
    generation_mix_by_fuel_type_base
    .withColumn(
        "region_total_dispatch_mw",
        F.sum("total_dispatch_mw").over(region_window)
    )
    .withColumn(
        "percentage_of_total_regional_dispatch",
        F.round(
            (F.col("total_dispatch_mw") / F.col("region_total_dispatch_mw")) * 100,
            2
        )
    )
    .select(
        F.col("region_id"),
        F.col("fuel_type_category"),          
        F.col("percentage_of_total_regional_dispatch"),                
    )
    .orderBy("region_id", F.desc("percentage_of_total_regional_dispatch"))
)

generation_mix_by_fuel_type_v.show()


# For aggregation using the new category 'RENEWABLES', a new column fuel_type_category was created. 
# To get total generation for each fuel type in each region, total_dispatch_mw was calculated by summing dispatch_mw 
# grouped by region and fuel_type_category. In the next step, a window function was used to get total dispatch of each region,
# and then total_dispatch_mw attained in the previous step was divided by this total to get the percentage by fuel type for each region.

spark.stop()