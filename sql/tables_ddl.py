spark.sql("""
          
# =========================
# BRONZE TABLES
# =========================
          
CREATE TABLE IF NOT EXISTS bronze.region_demand_raw (
    interval_datetime STRING,
    region_id STRING,
    rrp DOUBLE,
    total_demand_mw DOUBLE,
    scheduled_generation_mw DOUBLE,
    source_file_name STRING,
    ingested_at TIMESTAMP
)
USING DELTA;


CREATE TABLE IF NOT EXISTS bronze.unit_dispatch_raw (
    duid STRING,
    interval_datetime STRING,
    dispatch_mw DOUBLE,
    availability_mw DOUBLE,
    fuel_type STRING,
    source_file_name STRING,
    ingested_at TIMESTAMP
)
USING DELTA;
          

CREATE TABLE IF NOT EXISTS bronze.reference_generators_raw (
    duid STRING,
    station_name STRING,
    fuel_type STRING,
    region_id STRING,
    registered_capacity_mw DOUBLE,
    owner STRING,
    ingested_at TIMESTAMP
)
USING DELTA;
          

# =========================
# SILVER TABLES
# =========================
          
CREATE TABLE IF NOT EXISTS silver.reference_generators_scd2 ( 
    duid STRING,
    station_name STRING,
    region_id STRING,
    fuel_type STRING,
    owner STRING,
    registered_capacity_mw DOUBLE,
    effective_from TIMESTAMP, 
    effective_to TIMESTAMP) 
USING DELTA
          

CREATE TABLE IF NOT EXISTS silver.region_demand_interval (
    interval_datetime TIMESTAMP,
    dispatch_date DATE,
    region_id STRING,
    rrp DOUBLE,
    total_demand_mw DOUBLE,
    scheduled_generation_mw DOUBLE,
    source_file_name STRING,
    ingested_at TIMESTAMP)
USING DELTA
PARTITIONED BY (dispatch_date)
          

CREATE TABLE IF NOT EXISTS silver.unit_dispatch_interval ( 
    interval_datetime TIMESTAMP, 
    dispatch_date DATE,
    duid STRING, dispatch_mw DOUBLE, 
    availability_mw DOUBLE,
    fuel_type STRING,
    source_file_name STRING, 
    ingested_at TIMESTAMP ) 
    USING DELTA PARTITIONED BY (dispatch_date) 



# =========================
# GOLD TABLES
# =========================

CREATE TABLE IF NOT EXISTS gold.region_price_summary_v ( 

    USING DELTA
          

          

CREATE TABLE IF NOT EXISTS gold.generation_by_fuel_type_v ( 
    interval_datetime TIMESTAMP, 
    dispatch_date DATE, 
    duid STRING, dispatch_mw DOUBLE, 
    source_file_name STRING, 
    ingested_at TIMESTAMP ) 
    USING DELTA PARTITIONED BY (dispatch_date) 
          

CREATE TABLE IF NOT EXISTS top_10_generators_by_dispatch_volume_v ( 
    interval_datetime TIMESTAMP, 
    dispatch_date DATE, 
    duid STRING, dispatch_mw DOUBLE, 
    source_file_name STRING, 
    ingested_at TIMESTAMP ) 
    USING DELTA PARTITIONED BY (dispatch_date) 

   
""")