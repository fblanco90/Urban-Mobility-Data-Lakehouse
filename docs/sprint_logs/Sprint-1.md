# Engineering Log: Sprint 1 - Schema Design and Prototyping

**Authors:**
- María López Hernández
- Fernando Blanco Membrives
- Joan Sánchez Verdú

**Date:** 14/11/2025

**Status:** In Progress

## 1. Sprint Overview & Objectives

The primary objective of Sprint 1 is to design the foundational schemas for the 3-tier data lakehouse and build a working Proof of Concept (PoC) for the data ingestion and transformation pipeline. This log documents the process, decisions, and outcomes for the entire sprint.

**Key Goals for Sprint 1:**
1.  Source and explore a one-week sample of data from MITMA and INE.
2.  Define the schemas for the Bronze, Silver, and Gold layers.
3.  Implement the ingestion process for the Bronze layer.
4.  Implement the transformation logic for the Silver layer.
5.  Implement the aggregation logic for the Gold layer to support the key business questions.

---

## 2. Data Sourcing & Exploration

This phase involved identifying, downloading, and performing a preliminary inspection of all required datasets for the one-week prototype (May 8-14, 2023).

-   **MITMA Data:** Sourced from the official Open Data portal. This included 7 daily mobility files (`*_Viajes_distritos.csv.gz`) and several supporting metadata files (`nombres_distritos.csv`, `poblacion_distritos.csv`, `relacion_ine_zonificacionMitma.csv`).
-   **INE Data:** Sourced from the INEbase portal. This consisted of a single CSV file containing provincial GDP data from the "Contabilidad Regional de España" for the latest complete series (2000-2022).
    - Access to the INE file: https://www.ine.es/dynt3/inebase/es/index.htm?padre=12155&capsel=12157

All source files are stored locally in the `data/raw/` directory, **which is excluded from version control**.

---

## 3. Proof of Concept Implementation

This section details the step-by-step implementation of the 3-tier lakehouse PoC.

### 3.1. Bronze Layer Ingestion `(Completed)`

The Bronze layer creates a raw, immutable, and performant copy of the source data.

-   **Design Philosophy:**
    -   **Fidelity:** Data is ingested with a 1-to-1 correspondence to the source files. No data content is altered, filtered, or cleaned.
    -   **Robustness:** To prevent ingestion failures, all columns from source CSVs are explicitly ingested as `VARCHAR` using the `all_varchar=true` option in DuckDB. This decision was made after an initial attempt failed due to a type conversion error on the `estudio_destino_posible` column (could not convert `"si"` to `BOOLEAN`). This approach makes the pipeline resilient to source data quality issues.
    -   **Optimization:** Source CSV files are converted to the columnar Parquet format for significantly improved query performance.
    -   **Auditability:** Key metadata is added during ingestion, including a precise `ingestion_timestamp` (`TIMESTAMP WITH TIME ZONE`) and the source `filename`.

-   **Implementation Outcome:**
    The ingestion was performed using DuckDB's `COPY` command. The following Parquet files were successfully created in the `data/lakehouse/bronze/` directory:
    -   `mobility_sample_week.parquet`
    -   `zoning_districts.parquet`
    -   `population_districts.parquet`
    -   `mapping_ine_mitma.parquet`
    -   `gdp_provinces.parquet`

-   **Status:** The Bronze layer is complete and validated for the Sprint 1 sample data.

- **Schema:**
    - ![alt text](../diagrams/Bronze_Schema_diagram.png)

---

# 3.2. Silver Layer Transformation `(Completed)`

The Silver layer serves as the "single source of truth" within the data lakehouse. Its purpose is to transform the raw data from the Bronze layer into cleaned, integrated, and well-structured datasets ready for analysis.

## Implementation Summary

### Views Created in Silver Layer

1. **`silver.cleaned_mobility`** - Cleaned mobility trip data
2. **`silver.cleaned_population`** - Cleaned demographic data  
3. **`silver.silver_integrated_od`** - Core integrated table (OD matrices with demographics)

### Data Transformation Process

#### 1. **Mobility Data Cleaning** (`silver.cleaned_mobility`)
- **Date Conversion**: Transformed `fecha` from `YYYYMMDD` string format (`"20230508"`) to proper `DATE` type (`2023-05-08`)
- **Distance Standardization**: Handled range values by converting:
  - `"0.5-2"` → `1.25` (average)
  - `"2-10"` → `6.0` (average)
  - `"10-50"` → `30.0` (average) 
  - `">50"` → `75.0` (estimated)
- **Type Casting**: Converted `viajes` (trips) from `VARCHAR` to `INTEGER`
- **Column Renaming**: Standardized Spanish names to English:
  - `origen` → `origin_zone_id`
  - `destino` → `destination_zone_id`
  - `viajes` → `trips_count`

#### 2. **Population Data Cleaning** (`silver.cleaned_population`)
- **Missing Value Handling**: Converted `"NA"` values to `NULL` while preserving all zone records
- **Type Casting**: Converted population counts from `VARCHAR` to `INTEGER`
- **Column Renaming**:
  - `column0` → `zone_id`
  - `column1` → `population_count`

#### 3. **Data Integration** (`silver.silver_integrated_od`)
- **Join Logic**: Successfully joined mobility data with population data using zone IDs
- **Demographic Enrichment**: Added both origin and destination population counts to each trip record
- **Join Success Rate**: 99.76% of trips successfully linked with demographic data (134.4M out of 134.7M records)

## Key Technical Decisions

### Data Quality Handling
- **Explicit NULLs**: Used `NULL` for missing values rather than filtering out records, preserving data completeness
- **Range Value Strategy**: Applied averaging for distance ranges to maintain analytical utility
- **Quality Thresholds**: Applied filters for essential fields (non-null dates, zone IDs, and trip counts)

### Schema Design
- **Consistent Naming**: Used English snake_case convention across all tables
- **Audit Trail**: Preserved original filenames and ingestion timestamps for traceability
- **Data Typing**: Ensured proper data types for efficient storage and query performance
- **Schema:**
    - ![alt text](../diagrams/Silver_Schema_diagram.png)

---

### 3.3. Gold Layer Aggregation `(To Be Implemented)`

The Gold layer consists of business-ready, aggregated data products specifically designed to answer the project's key business questions.

-   **Planned Steps:**
    1.  **Read Data:** Source all data from the Silver layer tables.
    2.  **Business Question 1 (Typical Day):** Create the `gold_hourly_mobility_patterns` table by aggregating trip data by hour and day type (e.g., weekday/weekend).
    3.  **Business Question 2 (Infrastructure Gaps):** Create the `gold_gravity_model_inputs` table by joining trip data with population and economic indicators, preparing the necessary inputs for the gravity model formula.
    4.  **Persist:** Save the final aggregated tables as new Parquet files in the `data/lakehouse/gold/` directory.

-   **Implementation Details:**
    `[TODO: Document the final Gold layer aggregation queries and the resulting table schemas once this step is completed.]`

---

*This document will be updated as progress is made on the Silver and Gold layers.*