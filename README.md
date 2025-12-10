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