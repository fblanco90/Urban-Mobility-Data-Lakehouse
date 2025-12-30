from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow.models.param import Param
from airflow.decorators import dag, task
from utils_db import get_connection
import logging

BASE_URL_TEMPLATE = "https://movilidad-opendata.mitma.es/estudios_basicos/por-municipios/viajes/ficheros-diarios/{year}-{month}/{date}_Viajes_municipios.csv.gz"

@dag(
    dag_id="2_mobility_ingestion",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    # max_active_tasks=2,
    params={
        "start_date": Param("20230101", type="string", description="YYYYMMDD"),
        "end_date": Param("20230101", type="string", description="YYYYMMDD"),
    },
    tags=['infrastrucure']    
)
def mobility_ingestion():

    @task
    def generate_date_list(**context):
        """Generates list of YYYYMMDD strings."""
        conf = context['dag_run'].conf or {}
        params = context['params']
        start = conf.get('start_date') or params['start_date']
        end = conf.get('end_date') or params['end_date']
        
        return [d.strftime("%Y%m%d") for d in pd.date_range(start=start, end=end)]

    @task
    def ensure_br_mobility_table_exists(**context):
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

    @task(
        max_active_tis_per_dag=2, 
        retries=5,
        retry_delay=timedelta(seconds=30),
        map_index_template="{{ task.op_kwargs['date_str'] }}"
    )
    def br_process_single_day(date_str: str):

        # Input: "20230101"
        year = date_str[:4]    # "2023"
        month = date_str[4:6]  # "01"
        url = BASE_URL_TEMPLATE.format(year=year, month=month, date=date_str)
        logging.info(f"Target URL: {url}")

        try:
            response = requests.head(url, timeout=20)
            
            if response.status_code != 200:
                logging.warning(f"⚠️ File not found (HTTP {response.status_code}) for date {date_str}. Skipping task...")
                return f"Skipped: {date_str} not found"
                
        except Exception as e:
            logging.error(f"❌ Error connecting to the server to verify {date_str}: {e}")
            raise e

        with get_connection() as con:
            con.execute("SET http_keep_alive=false;")
            
            con.execute(f"DELETE FROM lakehouse.bronze.mobility_data WHERE fecha = '{date_str}'")

            con.execute(f"""
                INSERT INTO lakehouse.bronze.mobility_data
                SELECT 
                    *,
                    CURRENT_TIMESTAMP AS ingestion_timestamp,
                    '{url}' AS source_url
                FROM read_csv_auto('{url}', 
                    filename=false, 
                    all_varchar=true, 
                    ignore_errors=true
                );
            """)
            
            b_count = con.execute(f"SELECT COUNT(*) FROM lakehouse.bronze.mobility_data WHERE fecha='{date_str}'").fetchone()[0]
        
        logging.info(f"✅ Bronze: {b_count} rows ingested.")

    @task
    def ensure_sl_mobility_table_exists(**context):
        logging.info("🛠 Checking/Creating Table Structures.")

        # 1. Get a sample URL (using start_date) to infer Bronze Schema
        conf = context['dag_run'].conf or {}
        params = context['params']
        date_str = conf.get('start_date') or params['start_date']
        
        year = date_str[:4]
        month = date_str[4:6]
        url = BASE_URL_TEMPLATE.format(year=year, month=month, date=date_str)
        
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

    @task(
        max_active_tis_per_dag=2, 
        retries=5,
        retry_delay=timedelta(seconds=30), 
        map_index_template="{{ task.op_kwargs['date_str'] }}"
    )
    def sl_process_single_day(date_str: str):

        # Input: "20230101"
        year = date_str[:4]    # "2023"
        month = date_str[4:6]  # "01"
        url = BASE_URL_TEMPLATE.format(year=year, month=month, date=date_str)
        logging.info(f"Target URL: {url}")
        
        try:
            response = requests.head(url, timeout=20)
            
            if response.status_code != 200:
                logging.warning(f"⚠️ File not found (HTTP {response.status_code}) for date {date_str}. Skipping task...")
                return f"Skipped: {date_str} not found"
                
        except Exception as e:
            logging.error(f"❌ Error connecting to the server to verify {date_str}: {e}")
            raise e

        with get_connection() as con:
            con.execute("SET http_keep_alive=false;")
            
            con.execute(f"DELETE FROM lakehouse.silver.fact_mobility WHERE partition_date = strptime('{date_str}', '%Y%m%d')")

            con.execute(f"""
                INSERT INTO lakehouse.silver.fact_mobility
                SELECT 
                    (try_strptime(m.fecha, '%Y%m%d') + (CAST(m.periodo AS INTEGER) * INTERVAL 1 HOUR)) 
                        AT TIME ZONE 'Europe/Madrid' AS period,
                    
                    zo.zone_id AS origin_zone_id,
                    zd.zone_id AS destination_zone_id,
                    
                    CAST(m.viajes AS DOUBLE) AS trips,                      
                    CURRENT_TIMESTAMP AS processed_at,
                    try_strptime(m.fecha, '%Y%m%d') AS partition_date
                    
                FROM lakehouse.bronze.mobility_data m
                INNER JOIN lakehouse.silver.dim_zones zo ON TRIM(m.origen) = zo.mitma_code
                INNER JOIN lakehouse.silver.dim_zones zd ON TRIM(m.destino) = zd.mitma_code

                WHERE 
                    m.fecha = '{date_str}' 
                    AND m.viajes IS NOT NULL;
            """)
            
            s_count = con.execute(f"SELECT COUNT(*) FROM lakehouse.silver.fact_mobility WHERE partition_date = strptime('{date_str}', '%Y%m%d')").fetchone()[0]
        
        logging.info(f"✅ Silver: {s_count} trips transformed.")

    # ==============================================================================
    # ORCHESTRATION FLOW
    # ==============================================================================

    dates_list = generate_date_list()
    init_bronze = ensure_br_mobility_table_exists()
    init_silver = ensure_sl_mobility_table_exists()

    # Dynamic Mapping
    bronze_workers = br_process_single_day.expand(date_str=dates_list)
    silver_workers = sl_process_single_day.expand(date_str=dates_list)

    # 3. Dependencies
    init_bronze >> bronze_workers
    init_silver >> silver_workers
    bronze_workers >> silver_workers

mobility_ingestion()