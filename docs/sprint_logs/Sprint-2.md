# Engineering Log: Sprint 2 - Schema reDesign and Prototyping

**Authors:**
- María López Hernández
- Fernando Blanco Membrives
- Joan Sánchez Verdú

**Date:** 24/11/2025

**Status:** Done

## 1. Sprint Overview & Objectives
In this sprint, the team focused on transitioning the project from a basic file-based Proof of Concept (PoC) to a managed Data Lakehouse architecture. The primary goal was to establish the foundational infrastructure using DuckLake, enabling ACID-compliant transactions, metadata management, and scalable storage for the future high-volume mobility data.

**Key Goals for Sprint 2:**
1.  **Architecture Setup**:  Initialize the DuckLake catalog (`metadata.duckdb`) and define the logical schema layers (Bronze, Silver, Gold).
2. **Scope Refinement**: Shift the granularity of analysis from Census Districts to Municipalities to align mobility data with available economic and demographic indicators.
3. **Bronze Layer Implementation**: Build robust ingestion pipelines for heterogeneous raw data sources (CSV/Parquet), ensuring full data lineage and auditability.
4. **Big Data Strategy**: Implement physical partitioning for the massive mobility matrices to optimize future query performance.
5. **Silver Layer Normalization**: Construct a high-quality Silver layer by eliminating redundant columns, resolving many-to-one mapping conflicts, and establishing a single, unique identifier (`zone_id`) for municipalities to ensure strict referential integrity.

---

## 2. Data Sourcing & Exploration
Data acquisition strategies were updated to target Municipal-level datasets. We sourced data exclusively from public government repositories, ensuring open access compliance.
* **MITMA (Ministry of Transport)**:
    * Mobility Matrices: Daily O-D trips (switched to `Viajes_municipios` files).

    * Zoning: Municipal names and population registries.

    * Mapping: Cross-reference tables for MITMA-to-INE coding.

* **INE (National Statistics Institute)**:

    * Economics: Net income per person/household.

* **CNIG (National Center for Geographic Information)**:

    * Geography: Coordinates (centroids) for municipalities.

* **Open Data**:

    * Calendars: National working/holiday calendars.
---

## 3. Proof of Concept Implementation

This section details the step-by-step implementation of the 3-tier lakehouse PoC.

### 3.1. Bronze Layer Ingestion `(Finished)`

The Bronze layer serves as the raw data reservoir for the Lakehouse, adhering to the **ELT** paradigm. The primary objective at this stage is to ingest data from public sources (MITMA, INE, CNIG, Open Data) with minimal transformation, ensuring fidelity to the original source while preparing the storage structure for scalability.

**1. Design Principles & Metadata**
To ensure auditability and robustness, we implemented a standardized ingestion pattern for all tables:
*   **Data Fidelity:** All columns are ingested as `VARCHAR` (using DuckDB's `all_varchar=true`). This prevents ingestion failures due to data type mismatches (e.g., Spanish decimal formatting with commas/dots) and preserves the raw state of the data for auditing. Type casting and cleaning are strictly deferred to the Silver layer.
*   **Audit Columns:** Two metadata columns were appended to every table during ingestion:
    *   `ingestion_timestamp`: records exactly when the data entered the lakehouse.
    *   `source_url`: provides lineage traceability back to the specific public domain origin.

**2. Mobility Data Strategy**
The core mobility dataset (`mobility_sample_week`) presents a Big Data challenge, with potential volumes reaching billions of records. To handle this:
*   **Partitioning Strategy:** We implemented physical partitioning by **Date** (`fecha`).
*   **Implementation:** A "Define-Configure-Insert" pattern was used. We first defined the schema, explicitly configured the partition key via `SET PARTITIONED BY (fecha)`, and then inserted the data. This forces DuckLake to write data into physically separated folders (e.g., `/fecha=20230508/...`). This architecture optimizes downstream performance by enabling **Partition Pruning**, allowing the query engine to read only the relevant daily folders instead of scanning the entire dataset.

**3. Auxiliary Data (Dimensions) Handling**
Contextual datasets (Zoning, Population, Economics, Calendars) were ingested as unpartitioned tables due to their small size. A flexible Python ingestion function was developed to handle the heterogeneous formats of Spanish public administration files:
*   **Separator Handling:** Dynamically switching between Semicolon (`;`) for INE data and Pipe (`|`) for MITMA data.
*   **Header Repair:** MITMA dictionary files (e.g., `zoning_municipalities`) often lack standard headers or contain malformed first rows (e.g., `|ID|name`). We utilized `header=True` logic to correctly interpret these raw structures without manual intervention.

**4. Bronze Artifacts**
The following tables have been successfully established in the `lakehouse.bronze` schema:

| Table Name | Type | Source | Description |
| :--- | :--- | :--- | :--- |
| **`mobility_sample_week`** | Fact | MITMA | Raw O-D trip matrices. Partitioned by `fecha`. |
| **`zoning_municipalities`** | Dim | MITMA | Mapping of MITMA IDs to Municipality Names. |
| **`population_municipalities`** | Metric | MITMA | Resident population counts (raw strings with thousands separators). |
| **`mapping_ine_mitma`** | Map | MITMA | Cross-reference table linking MITMA zoning codes to INE administrative codes. |
| **`ine_rent_municipalities`** | Metric | INE | Average net income per person by municipality. |
| **`municipal_coordinates`** | Dim | CNIG | Geospatial centroids (Lat/Lon) for distance calculations. |
| **`work_calendars`** | Dim | Open Data | Daily calendar identifying working days, weekends, and national holidays. |

**5. Schema:**
- ![alt text](../images/Sprint2/Bronze_Schema_diagramS2.png)
---

### 3.2. Silver Layer Transformation `(Finished)`

The Silver layer represents the **"Trusted"** zone of the Lakehouse. In this stage, we transitioned from raw strings to strongly typed data, applied business rules for data integration, and established a robust **Star Schema** architecture utilizing **Surrogate Keys**. The primary transformation engine used was DuckDB SQL.

**1. Data Cleaning & Type Casting Strategy**
A major challenge was handling the heterogeneity of Spanish numeric and date formats in the Bronze files. We implemented a robust casting strategy:
*   **Spanish Number Handling:** Raw inputs often used dots for thousands and commas for decimals (e.g., `"1.200,50"`). We applied a standardization functions chain: `TRY_CAST(REPLACE(REPLACE(col, '.', ''), ',', '.') AS DOUBLE)`.
*   **The "Zero" Trap:** For integer metrics (Population), we strictly cast to `DOUBLE` before `BIGINT`. This prevented truncating values like `"50.0"` into `"5"`, a common error when using string manipulation on floating-point strings.
*   **Calendar Standardization:** Dates in Spanish format (`DD/MM/YYYY`) were parsed into standard SQL `DATE` objects. We also normalized free-text holiday descriptions, mapping variations (e.g., "fiesta nacional", "festivo nacional") to canonical categories (e.g., `'NationalFestive'`).

**2. Dimensional Modeling (Surrogate Key Architecture)**
We constructed a Star Schema centered around a master **`dim_zones`** table. Crucially, we decoupled the Lakehouse from external data changes by moving from Natural Keys (Raw Text Codes) to **Surrogate Keys** (Auto-generated Sequential Integers).
*   **Abstraction Layer:** The `zone_id` is now a generated `BIGINT`. All satellite tables were updated to lookup this internal ID via the Dimension table, ensuring referential integrity across the system.
*   **Zone Deduplication:** We detected "Aggregated Zones" (`_AM` suffix) in the MITMA dataset. To preserve **1:1 cardinality** for the ID generation, we grouped these duplicates, selecting a representative INE code for linkage.
*   **Geospatial Integration:** Linking coordinates required harmonizing INE Nomenclator codes (11 digits) with Municipality codes (5 digits). We implemented a `LEFT(col, 5)` logic with safe integer comparison.
*   **Holiday Bridge Strategy:** To model National Holidays (which apply universally to all territories), we implemented a **Bridge Table** (`bridge_zones_festives`). We utilized a `CROSS JOIN` strategy to propagate distinct National Holiday events to every `zone_id`, creating a dense lookup table that links specific dates and zones to festive types.

**3. Fact Table Optimization**
The `fact_mobility` table was transformed to enforce a strict Schema:
*   **Integer-Based Foreign Keys:** During insertion, we performed `LEFT JOIN` lookups against `dim_zones` to resolve raw text codes (e.g., `'28079'`) into system Integer IDs. This optimizes storage and join performance in the Gold layer.
*   **External Zone Handling:** International codes (e.g., `'PT170'`) which do not exist in the Spanish Master Dimension are automatically set to `NULL`. This prioritizes schema strictness over international granularity.
*   **Partition Maintenance:** The `fecha` partition strategy from Bronze was preserved.

**4. Silver Artifacts**
The following tables constitute the Core Lakehouse layer:

| Table Name | Type | Key Transformations |
| :--- | :--- | :--- |
| **`fact_mobility`** | Fact | Partitioned by Date. IDs resolved to `BIGINT` from `dim_zone`. Mobilities from any zone not in `dim_zone` **discarded**. Date formated (with TRY because it can have wrong values i.e. `20231035`). Posibility of using batches. `periodo` casted to INTEGER. `Viajes` casted to DOUBLE (With REPLACE for decimals). |
| **`dim_zones`** | Dim | Our own zone_id for each pair mitma-ine codes (BIGINT). Deduplicated mitma_codes with different ine_codes as MIN(ine_code) (Agregación de municipios). Cleaned `NA` or `NULL` in any mitma or ine code from `bronze.mapping_ine_mitma`. |
| **`dim_coordinates`** | Dim | Joined to our `zone_id`. Centroids calculated via `latitude` and `longitude` (DOUBLE) (REPLACE `,` by `.`). |
| **`dim_zone_holidays`** | Dim | Links Zones to Dates and National Holidays. Uses Cross Join to apply national events to all zones. `festive_date` casted to DATE. National Holidays have 3 different ways to say `National Festive` (`festivo nacional`, `Festivo Nacional` (ILIKE), `fiesta nacional`). |
| **`metric_population`** | Metric | Linked to our `zone_id`. Safe Double-to-Int casting for population. Asigned all for 2023. |
| **`metric_ine_rent`** | Metric | Linked to our `zone_id`. `income_per_capita` as DOUBLE with REPLACE(`,` by ` `). `year` as INTEGER. Only from the rows where `Distritos` and  `Secciones` are `NULL`, the income is not null, and the `zone_id`exists in `dim_zone`. |


**5. Schema:**
- ![alt text](../images/Sprint2/Silver_Schema_diagramS2.png)

---

### 3.3. Gold Layer Analytics `(Finished)`

The Gold layer constitutes the **Analytical/Mart** zone of the Lakehouse. Unlike the Silver layer, which focuses on data integrity and normalization, the Gold layer is purpose-built to answer specific business questions. It consists of highly aggregated, business-ready tables derived from complex joins between the Fact and Dimension tables.

**1. Business Question 1: Mobility Pattern Characterization**
To identify "Typical Day" patterns, we moved beyond simple heuristic assumptions and implemented an unsupervised machine learning approach within the Gold Layer pipeline:
*   **Profile Normalization**: We transformed raw trip counts into normalized daily profiles (probability distributions). This allows the model to compare the shape of the hourly demand curve (e.g., morning peaks vs. lunch plateaus) regardless of the absolute volume of trips.
*   **Unsupervised Clustering (K-Means)**: We applied K-Means clustering ($k=3$) to mathematically group days with similar temporal behaviors. This creates data-driven categories rather than relying solely on the calendar.
* **Gold Table Materialization:** The resulting centroids are stored in lakehouse.gold.typical_day_by_cluster. This table represents the "Pulse of the City" for distinct behaviors (e.g., Standard Workday, Leisure/Saturday, Quiet/Sunday).
* **Semantic Validation:** We cross-referenced the mathematical clusters with the bridge_zones_festives Silver table to validate that the clusters correlate with real-world concepts (Weekdays vs. Holidays).

**2. Business Question 2: Infrastructure Gap Identification (Gravity Model)**
To identify underserved areas, we implemented a classic Gravity Model ($T_{ij} \propto \frac{P_i E_j}{d_{ij}^2}$) using DuckDB's advanced analytical capabilities:
*   **Spatial Computation:** We utilized the DuckDB `spatial` extension (`st_distance_spheroid`) to calculate the precise geodesic distance ($d_{ij}$) between the centroids of every Origin-Destination pair stored in `dim_coordinates`. A safety clamp (`GREATEST(0.5, ...)`) was applied to prevent division-by-zero errors for intra-zonal trips.
*   **Model Enrichment:** The model integrates three Silver sources:
    *   **Mass ($P_i$):** Derived from `metric_population` linked to the Origin.
    *   **Attraction ($E_j$):** Derived from `metric_ine_rent` (Income per capita) linked to the Destination.
    *   **Friction ($d_{ij}^2$):** The squared distance acting as the impedance factor.
*   **Gap Scoring:** We calculated a `mismatch_ratio` (Actual Trips / Estimated Potential). A low ratio suggests high theoretical potential but low actual flow, indicating a likely infrastructure gap (e.g., lack of direct public transport).

**3. Gold Artifacts**
The following tables represent the final analytical deliverables:

| Table Name | Purpose | Key Metrics |
| :--- | :--- | :--- |
| **`typical_day_by_cluster`** | **Typical Day Analysis**. To provide a reference hourly mobility profile ("pulse of the city") for each behavioral cluster identified by the machine learning model. | `cluster_id`, `days_in_cluster`, `typical_day` |
| **`gold_infrastructure_gaps`** | **Gravity Model**. Comparing theoretical demand vs. actual flow to find gaps. | `estimated_potential_trips`, `mismatch_ratio`, `geographic_distance_km` |


**5. Schema:**
- ![alt text](../images/Sprint2/Gold_Schema_diagramS2.png)

---