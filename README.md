# 🇪🇸 3-Tier Data Lakehouse for Spanish Mobility Analysis

## 👥 Authors

*   **María López Hernández**
*   **Fernando Blanco Membrives**
*   **Joan Sánchez Verdú**


**Date:** January 2026

This project implements a **3-tier Data Lakehouse** (Bronze, Silver, Gold) designed to process and analyze public mobility data from the Spanish Ministry of Transport (MITMA) and the National Statistics Institute (INE).

The infrastructure follows a Medallion Architecture, utilizing **DuckDB** for processing, **AWS S3** for storage, and **Neon (Postgres)** for metadata management.

## 📂 Repository Structure

```text
.
├── airflow/                    # Airflow Orchestration (Astro Project)
│   ├── dags/                   # DAGs for ELT (Data fetched via HTTP) and utils
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

To enable cloud storage, metadata management, and elastic compute, you must configure the following in the Airflow UI (**Admin -> Connections**) and your AWS Console.

### Connection 1: AWS S3 (`aws_s3_conn`)
*   **Conn Type:** `Amazon Web Services`
*   **Login:** Your AWS Access Key ID
*   **Password:** Your AWS Secret Access Key
*   **Extra:** 
    ```json
    {
    "region_name": "eu-central-1",
    "bucket_name": "Your_bucket_name"
    }
    ```
*   **Note:** The IAM User associated with these keys must have the `batch:SubmitJob`, `batch:DescribeJobs`, and `iam:PassRole` permissions.

### Connection 2: Neon Postgres (`neon_catalog_conn`)
*   **Conn Type:** `Postgres`
*   **Host:** Your Neon hostname (e.g., `ep-winter-rain...aws.neon.tech`)
*   **Schema/Database:** `neondb`
*   **Login:** `neondb_owner`
*   **Password:** Your Neon Password
*   **Port:** `5432`

### 🚀 AWS Batch Infrastructure (Elastic Compute)
The project offloads heavy SQL transformations to **AWS Batch** to ensure high performance and avoid local memory issues. Ensure the following resources are created in your AWS Console (`eu-central-1`):

1.  **Compute Environment:** 
    *   **Name:** `DuckJobCompute` (or similar)
    *   **Instance Types:** Memory-optimized (e.g., `r5.large`, `r5.xlarge`)
    *   **Provisioning:** Spot instances are recommended for cost efficiency.
2.  **Job Queue:**
    *   **Name:** `DuckJobQueue`
    *   **Priority:** 1
3.  **Job Definition:**
    *   **Name:** `DuckJobDefinition`
    *   **Image:** `public.ecr.aws/p7o6v6h0/upv/duckrunner:latest`
    *   **Resource Requests:** Minimum 2 vCPUs and 4GB-14GB RAM depending on task complexity.
    *   **Job Role:** An IAM role with `AmazonS3FullAccess` and `CloudWatchLogsFullAccess`.
    *   **Execution Role:** `ecsTaskExecutionRole` to allow pulling the image.


## 🧪 Verification

Before running the main pipelines, it is highly recommended to run the following test DAGs to ensure your environment is correctly configured.

### 1. Local Connection Test (`00_connection_test`)
This DAG verifies that your **local Airflow environment** can successfully retrieve credentials and communicate with both AWS S3 and the Neon Metadata Catalog. 

*   **Action:** Triggers a local task that attempts to establish a connection using the defined hooks.
*   **Success Indicator:** The task turns green, and the Airflow logs display a confirmation message: `✅ Connected`.

### 2. AWS Batch & Infrastructure Test (`0_aws_batch_test`)
This DAG performs a full end-to-end test of the **Cloud Infrastructure**. It verifies that Airflow can trigger a remote AWS Batch job, and that the remote instance can correctly access the storage and metadata layers using the provided environment variables.

*   **Action:** Triggers a remote SQL command on an AWS instance that creates a temporary test table in the `gold` schema and immediately drops it.
*   **What to expect:** 
    1.  The task may stay in the `RUNNABLE` state for 1–3 minutes while AWS provisions a Spot instance.
    2.  Once `RUNNING`, it executes the SQL using the `duckrunner` image.
*   **Success Indicator:** 
    1.  The Airflow task status turns to `SUCCEEDED`.
    2.  The AWS CloudWatch logs (linked in the Airflow task details) show the message: `Finished executing the query`.

---

**Note:** If `00_connection_test` succeeds but `0_aws_batch_test` fails, check your AWS IAM permissions (specifically `iam:PassRole`) and ensure your Job Definition names match exactly in both the AWS Console and the DAG code.

## ⚙️ Data Pipelines (DAGs)

The orchestration logic is divided into specialized DAGs to ensure modularity, scalability, and ease of maintenance:

1.  **Infrastructure & Dimensions DAG**: Handles the ingestion of low-frequency static data (INE demographics, MITMA zoning, and Calendars). It establishes the schema foundations and builds the Bronze and Silver dimension tables.
    
2.  **Mobility Ingestion DAG**: A parameterized worker pipeline designed for high-volume processing. It accepts specific date ranges to ingest, clean, and transform daily mobility files (MITMA OD Matrices) from Bronze to Silver using atomic tasks.
    
3.  **Gold Generation DAG**: Triggered upon the completion of ingestion, this pipeline aggregates Silver data to construct core analytical models (e.g., K-Means Clustering, Gravity Models) in the Gold layer.
    
4.  **Business Reporting DAGs (4, 5 & 6)**: A set of independent, on-demand DAGs designed to query the pre-computed Gold layer. These are isolated from the ELT process to allow "Transport Experts" to generate specific reports (e.g., polygon filters or specific time windows) without re-processing the underlying data.

---
