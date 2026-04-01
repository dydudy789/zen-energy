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
    .withColumn("ingestion_date", F.current_date())
)

unit_dispatch_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("nem_data/raw_unit_dispatch.csv")
    .withColumn("ingestion_date", F.current_date())
)

reference_generators_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("nem_data/reference_generators.csv")
    .withColumn("ingestion_date", F.current_date())
)

print("BRONZE: region_demand_raw")
region_demand_raw.show(10, truncate=False)
region_demand_raw.printSchema()

print("BRONZE: unit_dispatch_raw ")
unit_dispatch_raw.show(10, truncate=False)
unit_dispatch_raw.printSchema()

print("BRONZE: reference_generators_raw")
reference_generators_raw.show(10, truncate=False)
reference_generators_raw.printSchema()


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
        F.col("scheduled_generation_mw").cast("double").alias("scheduled_generation_mw")
    )
    .filter(
        F.col("interval_datetime").isNotNull() &
        F.col("region_id").isNotNull()
    )
)



reference_generators_scd2 = (
    reference_generators_raw
    .withColumn("registered_capacity_mw", F.col("registered_capacity_mw").cast("double"))
    .withColumn("effective_from",  F.to_timestamp(F.lit("2024-01-01 00:00:00")))
    .withColumn("effective_to", F.to_timestamp(F.lit("9999-12-31 00:00:00")))

    .select(
        "duid",
        "station_name",
        "region_id",
        "fuel_type",
        "owner",
        "registered_capacity_mw",
        "effective_from",
        "effective_to"
    )
)


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
        F.col("dispatch_mw").cast("double").alias("dispatch_mw")
    ) )

  
unit_dispatch_enriched_mv = (
    unit_dispatch_interval.alias("u")
    .join(
        reference_generators_scd2.alias("r"),
        on=(
            (F.col("u.duid") == F.col("r.duid")) &
            (F.col("u.interval_datetime") >= F.col("r.effective_from")) &
            (F.col("u.interval_datetime") < F.col("r.effective_to"))
        ),
        how="left"
    )
    .select(
        F.col("u.interval_datetime"),
        F.col("u.dispatch_date"),
        F.col("u.region_id"),
        F.col("u.duid"),
        F.col("u.dispatch_mw"),
        F.col("u.availability_mw"),
        F.col("r.station_name"),
        F.col("r.fuel_type"),
        F.col("r.owner"),
        F.col("r.registered_capacity_mw")
    )
)   

print("=== SILVER: region demand interval ===")
region_demand_interval.show(15, truncate=True)
region_demand_interval.printSchema()


print("=== SILVER: reference_generators_scd2 ===")
reference_generators_scd2.show(15, truncate=True)
reference_generators_scd2.printSchema()

print("=== SILVER: unit_dispatch_interval ===")
unit_dispatch_interval.show(15, truncate=True)
unit_dispatch_interval.printSchema()

print("=== SILVER: unit_dispatch_enriched_mv ===")
unit_dispatch_enriched_mv.show(15, truncate=True)
unit_dispatch_enriched_mv.printSchema()


spark.stop()