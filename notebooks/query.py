from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

"""


dispatch_intervals_df:
Shows at 5 minute intervals how much electricity is generated and the consumed in each region. Also shows market price.
Difference between scheduled_generation and demand is probably from unscheduled sources e.g. rooftop solar, batteries, or from other imports
Large difference can possibly indicate grid stress?

+-------------------+---------+-----+---------------+-----------------------+
|interval_datetime  |region_id|rrp  |total_demand_mw|scheduled_generation_mw|
+-------------------+---------+-----+---------------+-----------------------+
|2024-08-01 00:05:00|NSW1     |79.17|8272.0         |7907.4                 |
|2024-08-01 00:10:00|NSW1     |77.2 |8149.8         |7921.4                 |



unit_dispatch_df:
At 5 minute intervals, shows which generators are running and how much electricity in produced, and the fuel type.
duid is likely unique identifier of generator
dispatch_mw is likely the current power output
availability_mw is likely the max production capacity. Generators are probably not running at max all the time.
fuel types can include black_coal, gas, hydro, wind, solar, etc

dispatch_mw can be negative which may mean battery is charging instead of outputing electricity?

Question:
Is scheduled_generation_mw in dispatch_intervals_df the sum of dispatch_mw from all generators for that interval?
Sum of dispatch_mw seems to be around 60% of scheduled_generation_mw, which may mean that some duids are missing from this dataset.

+-------------------+------+---------+-----------+---------------+----------+
|interval_datetime  |duid  |region_id|dispatch_mw|availability_mw|fuel_type |
+-------------------+------+---------+-----------+---------------+----------+
|2024-08-01 00:05:00|ERGT01|NSW1     |1990.65    |2646.37        |BLACK_COAL|
|2024-08-01 00:10:00|ERGT01|NSW1     |2072.98    |2855.49        |BLACK_COAL|



reference_generators_df:
Shows info about each generator unit (duid).
Plant name, fuel type, region, owner.
Registered_capacity_mw is likely rated maximum size and different to availability_mw in unit_dispatch_df. 

+-------+-------------------------+---------------+---------+----------------------+---------------+
|duid   |station_name             |fuel_type      |region_id|registered_capacity_mw|owner          |
+-------+-------------------------+---------------+---------+----------------------+---------------+
|ERGT01 |Eraring Power Station    |BLACK_COAL     |NSW1     |2880.0                |Origin Energy  |
|BAYSW  |Bayswater Power Station  |BLACK_COAL     |NSW1     |2640.0                |AGL Energy     |
|LIDDL1 |Liddell Power Station    |BLACK_COAL     |NSW1     |500.0                 |AGL Energy     |




Analytics:

Capacity Factor: How much a generator actually produced vs how much it could have produced
* actual energy produced / maximum possible energy


Intervals with NEGATIVE price: Generators are paying to generate electricity as there may be too much supply (low demand, grid oversupplied)
But possible better to pay then turn off
Retailers may make money if they earn money to take power into grid from plants and also get paid from
* rrp < 0

Count of intervals at PRICE CAP: When the market has hit maximum allowed price due to supply shortage, peak demand, generator outage, etc.
Plants can make large profits in these spikes, but retailers may lose money if costs increase but selling to customers at a lower, fixed price.
* rrp >= ~15000 $/MWh


"""

spark = (
    SparkSession.builder
    .appName("NEM Assessment Ingestion")
    .getOrCreate()
)


# =========================
# BRONZE DATAFRAMES
# =========================

region_demand_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("nem_data/raw_dispatch_intervals.csv")
    .withColumn("source_file_name", F.input_file_name())
    .withColumn("ingested_at", F.current_timestamp())
)

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
# SILVER DATAFRAMES
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



# =========================
# GOLD DATAFRAMES
# =========================

PRICE_CAP = 15000.0


gold_regional_price_summary = (
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

gold_regional_price_summary.show(truncate=False)


generation_mix_by_fuel_type_base = (
    unit_dispatch_interval
    .withColumn(
        "fuel_type_category",
        F.when(
            F.col("fuel_type").isin("WIND", "SOLAR_UTILITY", "HYDRO"),
            "Renewables"
        ).otherwise(F.col("fuel_type"))
    )
    .groupBy("region_id", "fuel_type_category")
    .agg(
        F.sum("dispatch_mw").alias("total_dispatch_mw")
    )
)

region_window = Window.partitionBy("region_id")

generation_mix_by_fuel_type = (
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


generation_mix_by_fuel_type.show()



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
        F.col("fuel_type").alias("fuel_type"),
        F.col("region_id").alias("region"),
        "total_dispatch_mwh",
        "capacity_factor_pct"
    )
    .orderBy(F.col("total_dispatch_mwh").desc())
    .limit(10)
)

top_10_generators_by_dispatch_volume.show(truncate=False)


spark.stop()