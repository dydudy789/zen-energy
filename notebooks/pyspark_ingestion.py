from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, lit, count, when, sum as spark_sum, round as spark_round


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
"""

spark = (
    SparkSession.builder
    .appName("NEM Assessment Ingestion")
    .getOrCreate()
)


# Read CSVs
dispatch_intervals_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("nem_data/raw_dispatch_intervals.csv")
)

unit_dispatch_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("nem_data/raw_unit_dispatch.csv")
)

reference_generators_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("nem_data/reference_generators.csv")
)

print("Raw Dispatch Intervals Data")
dispatch_intervals_df.show(10, truncate=False)

print("Raw Unit Dispatch Data")
unit_dispatch_df.show(10, truncate=False)

print("Reference Generator data")
reference_generators_df.show(10, truncate=False)


# Dispatch by fuel type
unit_dispatch_df.groupBy("fuel_type") \
    .agg(spark_round(spark_sum("dispatch_mw"), 2).alias("total_dispatch_mw")) \
    .orderBy(col("total_dispatch_mw").desc()) \
    .show(truncate=False)

# Generator unitilization rate
# For coal, utilization rate seems to be around 70-80%, output steady around 2000 mwgit 
unit_dispatch_with_util_df = unit_dispatch_df.withColumn(
    "utilisation_pct",
    spark_round((col("dispatch_mw") / col("availability_mw")) * 100, 2)
)

unit_dispatch_with_util_df.show(10, truncate=False)


# Comparison: scheduled_generation_mw vs sum(dispatch_mw) by interval + region
# The sum(dispatch_mw) seems to be around 60% of scheduled_generation_mw
scheduled_check_df = unit_dispatch_df.groupBy("interval_datetime", "region_id") \
    .agg(
        spark_round(spark_sum("dispatch_mw"), 2).alias("sum_dispatch_mw")
    )

comparison_df = dispatch_intervals_df.join(
    scheduled_check_df,
    on=["interval_datetime", "region_id"],
    how="left"
).withColumn(
    "difference_mw",
    spark_round(col("scheduled_generation_mw") - col("sum_dispatch_mw"), 2)
)

print("scheduled_generation_mw vs summed dispatch_mw")
comparison_df.select(
    "interval_datetime",
    "region_id",
    "scheduled_generation_mw",
    "sum_dispatch_mw",
    "difference_mw"
).show(20, truncate=False)


spark.stop()