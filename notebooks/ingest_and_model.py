from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, lit, count, when



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

spark.stop()