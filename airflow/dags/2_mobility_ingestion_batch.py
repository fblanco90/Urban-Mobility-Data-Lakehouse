from datetime import datetime
import logging
from typing import Any
import pandas as pd
import requests
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.sdk import dag, task, Param
from utils_db import get_connection

BASE_URL_TEMPLATE = "https://movilidad-opendata.mitma.es/estudios_basicos/por-municipios/viajes/ficheros-diarios/{year}-{month}/{date}_Viajes_municipios.csv.gz"

@dag(
    dag_id="2_mobility_ingestion_batch",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="YYYYMMDD"),
        "end_date": Param("20230101", type="string", description="YYYYMMDD"),
    },
    tags=['infrastructure', 'aws_batch']    
)
def mobility_ingestion_batch():

    @task
    def get_valid_dates(**context: Any) -> list[str]:
        """
        Generates a sequence of dates within a specified range and verifies the 
        availability of corresponding remote resources via HTTP HEAD requests. 
        It returns a filtered list containing only the dates for which a valid 
        data file exists at the source.
        """
        params = context["params"]
        all_dates = [
            d.strftime("%Y%m%d")
            for d in pd.date_range(start=params["start_date"], end=params["end_date"])
        ]
        valid_dates = []
        for d in all_dates:
            url = BASE_URL_TEMPLATE.format(year=d[:4], month=d[4:6], date=d)
            if requests.head(url, timeout=10).status_code == 200:
                valid_dates.append(d)
        return valid_dates

    @task
    def ensure_br_mobility_table_exists(**context: Any) -> None:
        """
        Ensures the structural integrity of the mobility data table in the bronze 
        layer by initializing its schema from a remote source if it does not exist. 
        It incorporates ingestion metadata and attempts to establish a 
        partitioning strategy by date.
        """
        logging.info("🛠 Checking/Creating Table Structures.")

        conf = context['dag_run'].conf or {}
        params = context['params']
        date_str = conf.get('start_date') or params['start_date']
        
        year = date_str[:4]
        month = date_str[4:6]
        url = BASE_URL_TEMPLATE.format(year=year, month=month, date=date_str)
        
        with get_connection() as con:
            con.execute(f"""
                    CREATE TABLE IF NOT EXISTS lakehouse.bronze.mobility_data AS 
                    SELECT 
                        *,
                        CURRENT_TIMESTAMP AS ingestion_timestamp,
                        CAST('{url}' AS VARCHAR) AS source_url
                    FROM read_csv_auto('{url}', all_varchar=true, ignore_errors=true)
                    LIMIT 0;
                """)
            try:
                con.execute("ALTER TABLE lakehouse.bronze.mobility_data SET PARTITIONED BY (fecha);")
                logging.info("✅ Bronze Table mobility_data created.")
            except Exception:
                logging.info("ℹ️ Partitioning skipped (Table mobility_data already exists).")

    @task
    def ensure_sl_mobility_table_exists() -> None:
        """
        Ensures the existence and structural configuration of the mobility fact table 
        within the silver layer. It initializes the table schema for storing trip 
        metrics and attempts to establish a partitioning strategy based on the 
        partition date to optimize downstream queries.
        """
        logging.info("🛠 Checking/Creating Table Structures.")
                
        with get_connection() as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_mobility (
                    period TIMESTAMP WITH TIME ZONE,
                    origin_zone_id BIGINT,
                    destination_zone_id BIGINT,
                    trips DOUBLE,
                    processed_at TIMESTAMP,
                    partition_date DATE
                );
            """)
            try:
                con.execute("ALTER TABLE lakehouse.silver.fact_mobility SET PARTITIONED BY (partition_date);")
                logging.info("✅ Silver Table fact_mobility created.")
            except Exception:
                logging.info("ℹ️ Partitioning skipped (Table fact_mobility already exists).")

            logging.info("✅ Table initialization complete.")

    @task
    def prepare_batch_configs(date_list: list[str], layer: str = "bronze") -> list[dict[str, Any]]:
        """
        Generates a list of environment configurations for batch processing tasks in 
        the bronze or silver layers. It constructs idempotent SQL queries—handling 
        either raw CSV ingestion or complex medallion transformations—and bundles 
        them with database credentials and S3 storage metadata for external execution.
        """
        neon = BaseHook.get_connection("neon_catalog_conn")
        aws = BaseHook.get_connection("aws_s3_conn")
        s3_bucket = aws.extra_dejson.get("bucket_name", "ducklake-dbproject")

        configs = []
        for d in date_list:
            url = BASE_URL_TEMPLATE.format(year=d[:4], month=d[4:6], date=d)
            if layer == "bronze":
                query = f"""
                    DELETE FROM bronze.mobility_data WHERE fecha = '{d}';
                    INSERT INTO bronze.mobility_data
                    SELECT *, CURRENT_TIMESTAMP, '{url}'
                    FROM read_csv_auto('{url}', all_varchar=true, ignore_errors=true);
                """
            else:  # silver
                query = f"""
                    DELETE FROM silver.fact_mobility
                    WHERE partition_date = CAST(strptime('{d}', '%Y%m%d') AS DATE);

                    INSERT INTO silver.fact_mobility
                    SELECT (try_strptime(m.fecha, '%Y%m%d') + (CAST(m.periodo AS INTEGER) * INTERVAL 1 HOUR))
                        AT TIME ZONE 'Europe/Madrid'                                     AS period,
                        zo.zone_id                                                         AS origin_zone_id,
                        zd.zone_id                                                         AS destination_zone_id,
                        CAST(m.viajes AS DOUBLE)                                           AS trips,
                        CURRENT_TIMESTAMP                                                  AS processed_at,
                        CAST(try_strptime(m.fecha, '%Y%m%d') AS DATE)                    AS partition_date
                    FROM bronze.mobility_data m
                    JOIN silver.dim_zones zo ON TRIM(m.origen) = zo.mitma_code
                    JOIN silver.dim_zones zd ON TRIM(m.destino) = zd.mitma_code
                    WHERE m.fecha = '{d}';
                """
            configs.append(
                {
                    "environment": [
                        {"name": "memory", "value": "4GB"},
                        {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
                        {"name": "CONTR_POSTGRES", "value": neon.password},
                        {"name": "USUARIO_POSTGRES", "value": neon.login},
                        {"name": "HOST_POSTGRES", "value": neon.host},
                        {"name": "DATABASE_POSTGRES", "value": "neondb"},
                        {"name": "RUTA_S3_DUCKLAKE", "value": f"s3://{s3_bucket}/lakehouse/"},
                        {"name": "SQL_QUERY", "value": query},
                    ]
                }
            )
        return configs
    
    process_bronze = BatchOperator.partial(
        task_id="br_process_day",
        job_name="ingest_mobility_bronze",
        job_definition="DuckJobDefinition",
        job_queue="DuckJobQueue",
        region_name="eu-central-1",
        aws_conn_id="aws_s3_conn",
    ).expand(container_overrides=prepare_batch_configs.override(task_id="prep_bronze")(get_valid_dates()))

    process_silver = BatchOperator.partial(
        task_id="sl_process_day",
        job_name="transform_mobility_silver",
        job_definition="DuckJobDefinition",
        job_queue="DuckJobQueue",
        region_name="eu-central-1",
        aws_conn_id="aws_s3_conn",
    ).expand(container_overrides=prepare_batch_configs.override(task_id="prep_silver")(get_valid_dates(), layer="silver"))


    # --- ORCHESTRATION ---

    task_br_table = ensure_br_mobility_table_exists()
    task_sl_table = ensure_sl_mobility_table_exists()
    [task_br_table, task_sl_table] >> process_bronze >> process_silver
    task_sl_table >> process_silver

mobility_ingestion_batch()