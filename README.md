# 🇪🇸 3-Tier Data Lakehouse for Spanish Mobility Analysis

## 👥 Authors

*   **María López Hernández**
*   **Fernando Blanco Membrives**
*   **Joan Sánchez Verdú**


**Date:** December 2025

This project implements a **3-tier Data Lakehouse** (Bronze, Silver, Gold) designed to process and analyze public mobility data from the Spanish Ministry of Transport (MITMA) and the National Statistics Institute (INE).

The infrastructure follows a Medallion Architecture, utilizing **DuckDB** for processing, **AWS S3** for storage, and **Neon (Postgres)** for metadata management.

## 📂 Repository Structure

```text
.
├── airflow/                    # Airflow Orchestration (Astro Project)
│   ├── dags/                   # DAGs for ELT (Data fetched via HTTP)
│   └── requirements.txt        # Dependencies needed to run the DAGs
├── data/                       # Local Data Storage (NOT tracked by Git)
│   ├── raw/                    # Source files for notebook experimentation
│   └── lakehouse/              # Local Bronze/Silver/Gold layers
├── docs/                       # Project Documentation
│   ├── diagrams/               # System architecture and ER diagrams
│   ├── report/                 # Final project analysis report
│   └── sprint_logs/            # Markdown logs for each sprint
├── notebooks/                  # Jupyter Notebooks used for sprint iterations
├── .gitignore                  # Git exclusion rules
├── requirements.txt            # Dependencies for running the Notebooks locally
└── README.md                   # Project overview

```

### 💡 Data Access Logic
*   **Airflow DAGs:** These are designed for automation. They fetch data directly from official sources via **HTTP requests** and process them into the cloud infrastructure.
*   **Notebooks:** These were used during the different **sprints** to prototype logic. They rely on the local `data/` folder and the root `requirements.txt`.


## 📈 Methodology

This project follows an **Agile** methodology. We work in iterative sprints with clear goals and continuous delivery. 

Detailed documentation for each stage of development—including sprint goals, task breakdowns, assignments, and retrospective outcomes—can be found in the following directory:
`docs/sprint_logs/` (e.g., `Sprint-1.md`, `Sprint-2.md`, etc.)


## 🚀 Setup & Installation
### Airflow Orchestration (Astro)
The orchestration layer is managed via the **Astro CLI**.

1. **Prerequisites:** Ensure **Docker Desktop** and **Astro CLI** are installed and running.
2. Open a terminal and navigate to the `airflow/` directory:
   ```bash
   cd airflow
   ```
3. **Initialize the project** (only needed once):
   ```bash
   astro dev init
   ```
   *If prompted because the folder is not empty, type `y` and press enter to confirm (this preserves your existing DAGs).*
4. **Start the environment**:
   ```bash
   astro dev start
   ```
5. **Access the UI**: Once initialized, open your browser at `http://localhost:8080/`.
6. **Stop the services**: To shut down the containers without removing your work, run:
   ```bash
   astro dev stop
   ```


## ☁️ Cloud Lakehouse Configuration

To enable the cloud storage and metadata catalog, you must configure two connections in the Airflow UI (**Admin -> Connections**):

### Connection 1: AWS S3 (`aws_s3_conn`)
*   **Conn Type:** `Amazon Web Services`
*   **Login:** Your AWS Access Key ID
*   **Password:** Your AWS Secret Access Key
*   **Extra:** 
    ```json
    {
    "region_name": "eu-central-1"
    }
    ```

### Connection 2: Neon Postgres (`neon_catalog_conn`)
*   **Conn Type:** `Postgres`
*   **Host:** Your Neon hostname (e.g., `ep-cool-frog...aws.neon.tech`)
*   **Schema/Database:** `neondb`
*   **Login:** `neondb_owner`
*   **Password:** Your Neon Password
*   **Port:** `5432`

## 🧪 Verification

A `connection_test` DAG is included in the `airflow/dags` folder. It is highly recommended to run this DAG first to verify that your Airflow environment can successfully communicate with both AWS S3 and the Neon Metadata Catalog. 

The task will log a confirmation message upon a successful connection and then close the session.

## ⚙️ Data Pipelines (DAGs)

The orchestration logic is divided into specialized DAGs to ensure modularity, scalability, and ease of maintenance:

1.  **Infrastructure & Dimensions DAG**: Handles the ingestion of low-frequency static data (INE demographics, MITMA zoning, and Calendars). It establishes the schema foundations and builds the Bronze and Silver dimension tables.
    
2.  **Mobility Ingestion DAG**: A parameterized worker pipeline designed for high-volume processing. It accepts specific date ranges to ingest, clean, and transform daily mobility files (MITMA OD Matrices) from Bronze to Silver using atomic tasks.
    
3.  **Gold Generation DAG**: Triggered upon the completion of ingestion, this pipeline aggregates Silver data to construct core analytical models (e.g., K-Means Clustering, Gravity Models) in the Gold layer.
    
4.  **Business Reporting DAGs (4, 5 & 6)**: A set of independent, on-demand DAGs designed to query the pre-computed Gold layer. These are isolated from the ELT process to allow "Transport Experts" to generate specific reports (e.g., polygon filters or specific time windows) without re-processing the underlying data.

7.  **Master Orchestrator DAG**: A controller pipeline that manages the sequential dependencies between infrastructure setup, data ingestion, and model training. It ensures referential integrity across the entire Lakehouse.

---
