# 3-Tier Data Lakehouse for Mobility Analysis in Spain

This project aims to design, implement, and orchestrate a 3-tier data lakehouse to process and analyze public mobility data from Spain. The infrastructure is built to support transport domain experts by providing robust, business-ready data products for urban mobility planning and analysis.

The architecture leverages modern, lightweight data tools, with a focus on local development and scalability. The primary data sources are the Spanish Ministry of Transport (MITMA) and the National Statistics Institute (INE).

## Agile Methodology

This project follows Agile methodology for collaborative team development. We work in iterative sprints with clear goals, regular standups, and continuous delivery.

### Sprint Structure
- **Sprint Duration:** 1-2 weeks
- **Sprint Planning:** Defining user stories and acceptance criteria
- **Daily Standups:** Progress tracking and blocker resolution  
- **Sprint Reviews:** Demo of completed features
- **Retrospectives:** Process improvement discussions

### Documentation
All sprint documentation, including:
- Sprint goals and planning
- Task breakdowns and assignments
- Progress tracking
- Review notes and demos
- Retrospective outcomes

Can be found in: `docs/sprint-logs/Sprint-X.md` (where X is the sprint number)

## Repository Structure

This repository is organized to clearly separate concerns, separating documentation, source code, notebooks, and orchestration logic.

```
mobility_lakehouse/
│
├── .gitignore
├── README.md
├── requirements.txt
├── docker-compose.yml          
│
├── airflow/                    # Orchestration layer
│   └── dags/                   # DAG definitions
│
├── data/                       # ── NOT TRACKED BY GIT ──
│   ├── raw/                    # Original MITMA & INE dumps
│   └── lakehouse/              # Bronze | Silver | Gold layers
│       ├── metadata.duckdb
│       └── metadata.duckdb.files/
│           ├── bronze/
│           ├── silver/
│           └── gold/
│       
├── docs/
│   ├── diagrams/
│   ├── report/
│   └── sprint-logs/
│       ├── Sprint-1.md
│       └── Sprint-x.md
│
├── notebooks/
│   ├── 1_sprint1_schema_prototyping.ipynb
│   └── 1_sprint2_schema_reprototyping.ipynb
│

│
└── src/
    ├── ingestion/              # Bronze-layer loaders
    │   └── .gitkeep
    └── sql/                    # Re-usable ELT queries
       └── .gitkeep
```

### A Note on the `data/` Directory

**Important:** The `data/` directory is intentionally excluded from this repository via the `.gitignore` file to avoid committing large data files. To run this project, you must create this folder structure locally.

1.  Create the `data/` folder in the root of the project.
2.  Inside `data/`, create two subfolders: `raw/` and `lakehouse/`.
3.  Place the downloaded MITMA and INE source files into the `data/raw/` directory.

The `data/lakehouse/` directory will be populated automatically when you run the ingestion and transformation scripts.



# Airflow Configuration
### Prerequisites
- Astro Installed
- Docker Installed

### Configuration steps:
- **IMPORTANT**: To have Docker running (open Docker Desktop).
- Open a terminal (CMD/console in windows).
- Navigate to the project route and access to `airflow` folder.
    - You can use cd command to move through the directories.
- Once inside, run the command `astro dev init` to initialize the Astro project.
    - This will ask us if we want to run the command in that directory because we already have folders. We put `y`and `enter` to confirm (It will keep our dags while adding all the configuration needed for Airflow)
- Once the Astro project is initalized, we can start it with the command `astro dev start`.
    - This can take a while the first time, as it will create the Docker containers needed to run the project.
- When it finishes, we can access to the Airflow Docker container trhough the url: `http://localhost:8080/` using our browser.
- To stop the project, execute `astro dev stop`. It will shut down the docker containers **(not removing the DAGs!)**.


# Cloud Lakehouse (Neon + S3) Configuration

## 🏗 Architecture Overview

| Component | Cloud (New) | Purpose |
| :--- | :--- | :--- |
| **Compute** | DuckDB (Airflow Worker) | Processing & SQL execution |
| **Storage** | **AWS S3** | Stores raw files and Parquet data |
| **Metadata** | **Neon (Postgres)** | Stores table definitions & handles locking |
| **Concurrency** | **Multi-Writer (Parallel)** | Allows simultaneous task execution |

---

## 1. Cloud Prerequisites (Infrastructure)

### ☁️ AWS (Storage)
1.  **Create an S3 Bucket:**
    *   Name: `ducklake-dbproject`.
    *   Region: `eu-central-1`.
2.  **Create an IAM User:**
    *   Name: `ducklakeUser`.
    *   Permission Policy: `AmazonS3FullAccess`.
3.  **Generate Access Keys:**
    *   Create an Access Key for "CLI/Code".
    *   **Save immediately:** `Access Key ID` and `Secret Access Key`.

### 🐘 Neon (Metadata Catalog)
1.  **Create a Project:**
    *   Name: `ducklake-dbproject`.
2.  **Get Connection Details:**
    *   Dashboard -> Connection Details.
    *   Save the PGHOST, PGDATABASE, PGUSER and PGPASSWORD. 

---

## 2. Configure Airflow Connections
In our project we use the Airflow connections to secure the conection to both Neon and AWS.
Use the Airflow UI (Admin -> Connections) to store them securely.

### Connection 1: AWS S3 (aws_s3_conn)
- Conn Id: `aws_s3_conn`
- Conn Type: `Amazon Web Services`
- Login: (Your AWS Access Key ID)
- Password: (Your AWS Secret Access Key)
- Extra: (JSON format)
- code
    - ```JSON
        {
        "region_name": "eu-central-1"
        }
        ```

### Connection 2: Neon Postgres (neon_catalog_conn)
- Conn Id: `neon_catalog_conn`
- Conn Type: `Postgres`
- Host: (e.g., `ep-cool-frog-123456.eu-central-1.aws.neon.tech`)
- Schema: `neondb`
- Login: (e.g., `neondb_owner`)
- Password: (Your Neon Password)
- Port: `5432`


## 3. Verification
There is a `connection_test` DAG with the only porpuse of checking if the conection is working.
This DAG has only 1 task which will connect, show a logger to confirm the connection, and close the connection.
If this DAG works well, it means that the conections are working fine, so you're ready to execute any DAG.


 
