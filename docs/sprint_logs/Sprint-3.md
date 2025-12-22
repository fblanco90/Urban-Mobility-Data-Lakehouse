# Engineering Log: Sprint 3 - Schema reDesign and AirFlow

**Authors:**
- María López Hernández
- Fernando Blanco Membrives
- Joan Sánchez Verdú

**Date:** 22/12/2025

**Status:** Done

## 1. Sprint Overview & Objectives


**Key Goals for Sprint 3:**
1. Apply improvements from Sprint 2
    - Table for metrics
    - `data` and `hour` column from `fact_mobility` into one TIMESTAMP column.
    - Delete geo table and add `GEOMETRY` column on `dim_zones` table.
    - Don't download data (csvs) but read it directly to DB.
    - Migrate from local to Neon + AWS(s3) storage.
2. Airflow
    - Configure Airflow
    - Design DAG for ingestion

---

## 2. Data Sourcing & Exploration
* **CNIG (National Center for Geographic Information)**:

    * Read from `pyspainmobility` library
---

## 3. Proof of Concept Implementation

This section details the schemas and improvements per layer.

### 3.1. Bronze Layer Ingestion `(Done)`
New coordinates from `pyspainmobility` library.


**Schema:**
- ![alt text](../diagrams/Sprint3/Bronze_Schema_diagramS3.png)
---

### 3.2. Silver Layer Transformation `(Done)`

`fact_mobility`:
- `date` and `period` (hour) columns removed. Now there is one only `TIMESTAMP WITH ZONE` column (`period_time`). `partition_date` = `date` (only for the storage partition).

`dim_zones`:
- `polygon` column (Geometry) added to store the `POLYGONS`and `MULTIPOLYGONS` for each zone.

`data_quality_logs`: Table to store data quality checks
- `check_timestamp`: Time when the log was generated
- `table_name`: Name of the table of the metric
- `metric_name`: Name of the metric stored (i.e. avg_income_per_capita)
- `metric_vale`: Numeric value of the metric stored
- `notes`: Extra information needed for the metric


**Schema:**
- ![alt text](../diagrams/Sprint3/Silver_Schema_diagramS3.png)

---

### 3.3. Gold Layer Analytics `(Done)`

`gold_infrastructure_gaps`
- Calculation of distance from `st_point(origin.longitude, origin.latitude)` to `ST_Centroid(geometry_column)`.


**Schema:**
- ![alt text](../diagrams/Sprint3/Gold_Schema_diagramS3.png)

---


## Airflow
Unique DAG for ingestion:
![alt text](../diagrams/AirflowMainDAGExecuted.PNG)

The specific responsibilities of the key tasks depicted in the diagram are as follows:
- create_schemas: Initializes the DuckLake schemas (bronze, silver, gold) and the persistence layer for data quality logs.
- ingest_[geo|static]: A parallel group of tasks that extract reference data (Shapefiles, INE CSVs) from external web sources.
- build_silver_dimensions: Performs SQL transformations to clean reference data and generate surrogate keys (e.g., dim_zones).
- ensure_fact_tables_exist: A singleton task that prepares the destination tables for the mobility data, preventing race conditions during parallel writes.
- process_single_day: The dynamic mapped task. Each instance handles the full ELT cycle (Download → Bronze → Silver) for a specific date within a transaction block.
- audit_[dims|batch]: Quality control gates that calculate metrics (e.g., null rates) and log them to the metadata registry.
- create_gold_[cluster|gaps]: The final analytical steps that aggregate the fully processed Silver data into business-ready insights.



## Improvements for next Sprint
- rss xml geo data (no library)
- Clean INE ingestion (checksum check)
- 1 task = 1 execute (atomicity)
- Multiple DAGs (Ingest bronze, Silver transform, gold creation, gold query) 