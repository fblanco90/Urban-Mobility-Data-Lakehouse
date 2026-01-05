# Engineering Log: Sprint 4 - AirFlow reDesign

**Authors:**
- María López Hernández
- Fernando Blanco Membrives
- Joan Sánchez Verdú

**Date:** 05/01/2026

**Status:** Done

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

## 2. Data Layers

The layers have not been updated from last sprint.

### 2.1. Bronze Layer Ingestion `(Done)`
**Schema:**
- ![alt text](../images/Sprint4/Bronze_Schema_diagramS4.png)
---

### 2.2. Silver Layer Transformation `(Done)`
**Schema:**
- ![alt text](../images/Sprint4/Silver_Schema_diagramS4.png)

---

### 2.3. Gold Layer Analytics `(Done)`
**Schema:**
- ![alt text](../images/Sprint4/Gold_Schema_diagramS4.png)

---


## Airflow
NOT Unique DAG for ingestion. DAGs redesign:
![alt text](../images/AirflowDAGs/AirflowMainDAGExecuted.PNG)

- DAG 1: Ingest Bronze and Silver from csvs (All but mobility data)
    - Only executed once.
    - ![alt text](../images/AirflowDAGs/infrastructure_and_dimensions-DAG.png)
- DAG 2: Ingest Bronze and Silver from a Day (Only mobility data) given a date range
    - We can ingest days anytime without ingesting the rest of csvs.
    - - ![alt text](../images/AirflowDAGs/mobility_ingestion-DAG.png)
- DAG 3: Generate Gold tables and consultations
    - We can generate gold tables only when we have enough data
    - DAG 3.1: Query gold and generate data for Bussines question 1
    - DAG 3.2: Query gold and generate data for Bussines question 2
    - DAG 3.3: Query gold and generate data for Bussines question 3 (our bussines question)