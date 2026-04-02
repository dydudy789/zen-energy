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

region_demand_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("nem_data/raw_dispatch_intervals.csv")
    .withColumn("source_file_name", F.input_file_name())
    .withColumn("ingested_at", F.current_timestamp())
)



# =========================
# SILVER LAYER
# =========================

region_demand_interval = (
    region_demand_raw
    .withColumn("interval_datetime", F.to_timestamp("interval_datetime"))
    .withColumn("dispatch_date", F.to_date("interval_datetime"))
    .select(
        "interval_datetime",
        "dispatch_date",
        "region_id",
        F.col("rrp").cast("double").alias("rrp"),
        F.col("total_demand_mw").cast("double").alias("total_demand_mw"),
        F.col("scheduled_generation_mw").cast("double").alias("scheduled_generation_mw"),
        "source_file_name",
        "ingested_at"
    )
    .filter(
        F.col("interval_datetime").isNotNull() &
        F.col("region_id").isNotNull()
    )
)


# =========================
# GOLD LAYER
# =========================

PRICE_CAP = 17500.0

gold_regional_price_summary_v = (
    region_demand_interval
    .groupBy("region_id")
    .agg(
        F.round(F.avg("rrp"), 2).alias("avg_rrp"),
        F.min("rrp").alias("min_rrp"),
        F.max("rrp").alias("max_rrp"),
        F.sum(F.when(F.col("rrp") >= PRICE_CAP, 1).otherwise(0)).alias("count_intervals_at_price_cap"),
        F.sum(F.when(F.col("rrp") < 0, 1).otherwise(0)).alias("count_intervals_negative_price")
    )
    .orderBy("region_id")
)

gold_regional_price_summary_v.show(truncate=False)

spark.stop()