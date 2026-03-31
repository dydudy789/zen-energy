# Data Pipeline Solution – NEM Data Ingestion & Processing

## Overview
Data model and data pipeline for ingesting, transforming, and analysing Australian energy market dispatch data.
This solution leverages a modern data engineering stack consisting of:

- **Databricks**
- **Python / PySpark**
- **Azure Data Lake Storage (ADLS)**
- **Delta Lake**

The design aligns with typical Azure-based data platforms and assumes:
- Databricks is configured with a **registered Service Principal in Azure**
- Email ingestion is performed via **Microsoft Outlook**, using the **Microsoft Graph API**

---

## Architecture Summary

The pipeline ingests, processes, and models three key datasets:

1. **Daily email-delivered data (raw_unit_dispatch.csv)**
2. **Dispatch intervals (time-series data)**
3. **Reference generator data (dimension data)**

Data flows through a standard **Bronze → Silver → Gold** architecture:
- **Bronze**: Raw ingestion (minimal transformation)
- **Silver**: Cleaned, validated, structured data
- **Gold**: Business-ready datasets for reporting and BI

---

## Email Ingestion (raw_unit_dispatch)

To ingest daily files received via email:

- A **scheduled Databricks Workflow job** performs a `GET` request to the **Microsoft Graph API**
- The job:
  1. Locates the email using sender + date filters
  2. Retrieves the attachment using the expected file name
- Attachments are:
  - Retrieved in **Base64 format**
  - Decoded and uploaded to ADLS using Azure SDK methods:
    - `append_data`
    - `flush_data`

### Storage Pattern
Files are stored in ADLS using a **date-partitioned structure**: raw/aemo/unit_dispatch/YYYY/MM/DD/raw_unit_dispatch.csv


This supports:
- Efficient ingestion
- Incremental processing
- Partition pruning

---

## Dispatch Intervals Ingestion

The `dispatch_intervals` dataset is ingested via a **daily batch job**:

### Raw Layer (Bronze)

- Data is landed into a raw zone: raw/aemo/dispatch_intervals/YYYY/MM/DD/raw_dispatch_intervals.csv


- Scheduled after expected file availability
- Treated as **append-only time-series data**

### Silver Layer

- Data is transformed into a **Delta table**:
  - Enforced schema
  - Deduplication
  - Metadata columns (e.g., `ingested_at`, `source_file_name`)

- Example target table: silver.fct_region_price_interval


### Notes

- Batch processing is sufficient for this use case
- If near real-time ingestion is required, **Databricks Auto Loader** can be used instead

---

## Reference Data Ingestion (reference_generators)

Reference data is ingested from an internal system via:
- Database extract OR
- API call

### Raw Layer

Stored in ADLS using load-date partitioning: raw/reference/reference_generators/load_date=YYYY-MM-DD/reference_generators.csv


---

## Slowly Changing Dimension (SCD Type 2)

Reference data is modeled as an **SCD Type 2 dimension** in the Silver layer:



### Additional Columns

- `asset_status` → ("ACTIVE" / "DECOMMISSIONED")
- `effective_from`
- `effective_to`

### Update Logic

- Records are tracked using `duid` as the business key
- On change:
  - Existing record:
    - `effective_to = current_date - 1`
  - New record:
    - `effective_from = current_date`
    - `effective_to = '9999-12-31'`

This ensures:
- Full history tracking
- Point-in-time analysis capability

---

## Authentication (Microsoft Graph API)

To enable secure API access from Databricks:

### Steps

1. **Register an application in Azure**
2. Add **Microsoft Graph API permissions**:
   - `Mail.Read`
3. Create a **Client Secret**
4. Store credentials securely in **Databricks Secret Scope**:
   - `client_id`
   - `tenant_id`
   - `client_secret`

### Usage

- Secrets are retrieved at runtime within Databricks workflows
- Avoids hardcoding credentials in code

---

## Key Design Considerations

- **Partitioning by date** for scalability and performance
- **Delta Lake** for ACID compliance and efficient upserts
- **SCD Type 2** for historical tracking of reference data
- **Separation of raw and processed layers** for data lineage and reprocessing
- **Secure credential management** via secret scopes

---

## Future Enhancements

- Implement **Auto Loader** for streaming ingestion
- Add **data quality checks** (e.g., schema validation, anomaly detection)
- Introduce **Gold layer marts** for reporting
- Automate **CI/CD deployment** using Databricks Asset Bundles or Azure DevOps

---

