from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, lit, count, when



spark = (
    SparkSession.builder
    .appName("NEM Assessment Ingestion")
    .getOrCreate()
)



spark.stop()