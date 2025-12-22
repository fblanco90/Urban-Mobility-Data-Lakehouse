# Engineering Log: Sprint 4 - AirFlow reDesign

**Authors:**
- María López Hernández
- Fernando Blanco Membrives
- Joan Sánchez Verdú

**Date:** 23/12/2025

**Status:** In process

## 1. Sprint Overview & Objectives


**Key Goals for Sprint 3:**
1. Apply improvements from Sprint 3
    - Redisgn DAGs.
        - Multiple DAGs (Ingest bronze, Silver transform, gold creation, gold query) 
        - 1 task = 1 execute (atomicity)
    - Don't use library for Geo Data
    - Clean INE ingestion
2. Gold layer
    - Finally design and implement gold layer
    - Implement use cases with outputs

---

## 2. Data Sourcing & Exploration
* **CNIG (National Center for Geographic Information)**:

    * Read from ...
---

## 3. Data Layers

The layers have not been updated from last sprint.

### 3.1. Bronze Layer Ingestion `(Done)`
**Schema:**
- ![alt text](../diagrams/Sprint3/Bronze_Schema_diagramS3.png)
---

### 3.2. Silver Layer Transformation `(Done)`
**Schema:**
- ![alt text](../diagrams/Sprint3/Silver_Schema_diagramS3.png)

---

### 3.3. Gold Layer Analytics `(Done)`
**Schema:**
- ![alt text](../diagrams/Sprint3/Gold_Schema_diagramS3.png)

---


## Airflow
NOT Unique DAG for ingestion. DAGs redesign:
![alt text](../diagrams/AirflowMainDAGExecuted.PNG)

- DAG 1: Ingest Bronze and Silver from csvs (All but mobility data)
    - Only executed once.
- DAG 2: Ingest Bronze and Silver from a Day (Only mobility data) given a date range
    - We can ingest days anytime without ingesting the rest of csvs
- DAG 3: Generate Gold tables
    - We can generate gold tables only when we have enough data
- DAG 4: Controller of all 3 ingestion DAGs together (just if needed)
    - Just in case it could be so easy to use to execute all the flux with all the days needed
- DAG 5: Query gold and generate data