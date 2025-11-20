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
├── .gitignore          # Specifies files and folders for Git to ignore
├── README.md           # You are here!
├── requirements.txt    # Python project dependencies
├── docker-compose.yml  # Configuration for running Airflow (Sprint 3)
|
├── airflow/            # Airflow-specific files
│   └── dags/           # Scripts for orchestration pipelines (DAGs)
|
├── data/               # --- NOT TRACKED BY GIT ---
│   ├── raw/            # Storage for original, unmodified source data
│   └── lakehouse/      # Local storage for the Bronze, Silver, and Gold data layers
|
├── docs/               # Project documentation
│   ├── diagrams/       # Architecture diagrams
│   └── schemas.md      # Formal definitions of the lakehouse table schemas
|
├── notebooks/          # Jupyter notebooks for exploration, prototyping, and analysis
|
└── src/                # Production-quality source code
    ├── ingestion/      # Scripts for ingesting raw data into the Bronze layer
    ├── sql/            # Reusable SQL queries for ELT transformations
    └── utils/          # Helper functions and utilities
```

### A Note on the `data/` Directory

**Important:** The `data/` directory is intentionally excluded from this repository via the `.gitignore` file to avoid committing large data files. To run this project, you must create this folder structure locally.

1.  Create the `data/` folder in the root of the project.
2.  Inside `data/`, create two subfolders: `raw/` and `lakehouse/`.
3.  Place the downloaded MITMA and INE source files into the `data/raw/` directory.

The `data/lakehouse/` directory will be populated automatically when you run the ingestion and transformation scripts.



