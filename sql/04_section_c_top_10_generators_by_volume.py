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

reference_generators_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("nem_data/reference_generators.csv")
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
        "effective_to",
        "source_file_name"
    )
)


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


# =========================
# GOLD LAYER
# =========================

INTERVAL_HOURS = 5 / 60

top_10_generators_base = (
    unit_dispatch_enriched_mv
    .groupBy(
        "duid",
        "station_name",
        "owner",
        "fuel_type",
        "region_id",
        "registered_capacity_mw"
    )
    .agg(
        F.sum("dispatch_mw").alias("total_dispatch_mwh_raw"),
        F.count("interval_datetime").alias("interval_count")
    )
    .withColumn(
        "total_dispatch_mwh",
        F.round(F.col("total_dispatch_mwh_raw") * F.lit(INTERVAL_HOURS), 2)
    )
    .withColumn(
        "max_possible_mwh",
        F.col("registered_capacity_mw") * F.col("interval_count") * F.lit(INTERVAL_HOURS)
    )
    .withColumn(
        "capacity_factor_pct",
        F.round(
            F.when(F.col("max_possible_mwh") > 0,
                   (F.col("total_dispatch_mwh") / F.col("max_possible_mwh")) * 100
            ).otherwise(None),
            2
        )
    )
)

top_10_generators_by_dispatch_volume = (
    top_10_generators_base
    .select(
        F.col("station_name").alias("station"),
        "owner",
        F.col("fuel_type"),
        F.col("region_id").alias("region"),
        "total_dispatch_mwh",
        "capacity_factor_pct"
    )
    .orderBy(F.col("total_dispatch_mwh").desc())
    .limit(10)
)

top_10_generators_by_dispatch_volume.show(truncate=False)

spark.stop()