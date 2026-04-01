from pyspark.sql import SparkSession

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
    ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date);


CREATE TABLE IF NOT EXISTS bronze.unit_dispatch_raw (
    interval_datetime STRING,
    duid STRING,       
    region_id STRING,  
    dispatch_mw DOUBLE,
    availability_mw DOUBLE,
    fuel_type STRING,
    ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date);
          

CREATE TABLE IF NOT EXISTS bronze.reference_generators_raw (
    duid STRING,
    station_name STRING,
    fuel_type STRING,
    region_id STRING,
    registered_capacity_mw DOUBLE,
    owner STRING
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
    scheduled_generation_mw DOUBLE)
USING DELTA
PARTITIONED BY (dispatch_date)
          

CREATE TABLE IF NOT EXISTS silver.unit_dispatch_interval ( 
    interval_datetime TIMESTAMP, 
    dispatch_date DATE,
    duid STRING, dispatch_mw DOUBLE, 
    availability_mw DOUBLE,
    fuel_type STRING) 
    USING DELTA PARTITIONED BY (dispatch_date) 


CREATE OR REPLACE MATERIALIZED VIEW silver.unit_dispatch_enriched_mv
AS
SELECT
    u.interval_datetime,
    u.dispatch_date,
    u.region_id,
    u.duid,
    u.dispatch_mw,
    u.availability_mw,
    r.station_name,
    r.fuel_type,
    r.owner,
    r.registered_capacity_mw
FROM silver.unit_dispatch_interval u
LEFT JOIN silver.reference_generators_scd2 r
    ON  u.duid = r.duid
    AND u.interval_datetime >= r.effective_from
    AND u.interval_datetime <  r.effective_to;
          
# =========================
# GOLD TABLES
# =========================

CREATE OR REPLACE VIEW gold.region_price_summary_v AS
SELECT
    region_id,
    ROUND(AVG(rrp), 2)                                    AS avg_rrp,
    MIN(rrp)                                              AS min_rrp,
    MAX(rrp)                                              AS max_rrp,
    SUM(CASE WHEN rrp >= 15000.0 THEN 1 ELSE 0 END)   AS count_intervals_at_price_cap,
    SUM(CASE WHEN rrp < 0 THEN 1 ELSE 0 END              AS count_intervals_negative_price
FROM region_demand_interval
GROUP BY region_id
ORDER BY region_id;
          

          
CREATE OR REPLACE VIEW gold.generation_mix_by_fuel_type_v AS
WITH generation_mix_by_fuel_type_base AS (
    SELECT
        region_id,
        CASE
            WHEN fuel_type IN ('WIND', 'SOLAR_UTILITY', 'HYDRO') THEN 'RENEWABLES'
            ELSE fuel_type
        END AS fuel_type_category,
        SUM(dispatch_mw) AS total_dispatch_mw
    FROM unit_dispatch_interval
    GROUP BY
        region_id,
        CASE
            WHEN fuel_type IN ('WIND', 'SOLAR_UTILITY', 'HYDRO') THEN 'RENEWABLES'
            ELSE fuel_type
        END
)
SELECT
    region_id,
    fuel_type_category,
    ROUND(
        (total_dispatch_mw / SUM(total_dispatch_mw) OVER (PARTITION BY region_id)) * 100,
        2
    ) AS percentage_of_total_regional_dispatch
FROM generation_mix_by_fuel_type_base
ORDER BY region_id, percentage_of_total_regional_dispatch DESC
          


          
CREATE OR REPLACE VIEW gold.top_10_generators_by_dispatch_volume_v AS
WITH base AS (
    SELECT
        duid,
        station_name,
        owner,
        fuel_type,
        region_id,
        registered_capacity_mw,
        SUM(dispatch_mw) AS total_dispatch_mwh_raw,
        COUNT(interval_datetime) AS interval_count
    FROM silver.unit_dispatch_enriched_mv
    GROUP BY
        duid,
        station_name,
        owner,
        fuel_type,
        region_id,
        registered_capacity_mw
),
calc AS (
    SELECT
        *,
        ROUND(total_dispatch_mwh_raw * (5.0 / 60), 2) AS total_dispatch_mwh,
        registered_capacity_mw * interval_count * (5.0 / 60) AS max_possible_mwh
    FROM base
)
SELECT
    station_name AS station,
    owner,
    fuel_type,
    region_id AS region,
    total_dispatch_mwh,
    ROUND(
        CASE
            WHEN max_possible_mwh > 0 THEN (total_dispatch_mwh / max_possible_mwh) * 100
            ELSE NULL
        END,
        2
    ) AS capacity_factor_pct
FROM calc
ORDER BY total_dispatch_mwh DESC
LIMIT 10
          
   
""")