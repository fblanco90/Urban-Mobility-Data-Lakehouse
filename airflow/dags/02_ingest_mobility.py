from airflow.sdk import dag, task, Param
# fromairflow.sdk.Param import Param
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
import logging

# --- CONFIGURATION ---
BASE_URL_TEMPLATE = "https://movilidad-opendata.mitma.es/estudios_basicos/por-municipios/viajes/ficheros-diarios/{year}-{month}/{date}_Viajes_municipios.csv.gz"

@dag(
    dag_id="mobility_02_ingest_daily",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="YYYYMMDD"),
        "end_date": Param("20230101", type="string", description="YYYYMMDD")
    },
    tags=['mobility', 'ingest']
)
def mobility_02_ingest_daily():

    @task
    def generate_date_list(**context):
        """Generates list of YYYYMMDD strings."""
        conf = context['dag_run'].conf or {}
        params = context['params']
        start = conf.get('start_date') or params['start_date']
        end = conf.get('end_date') or params['end_date']
        
        return [d.strftime("%Y%m%d") for d in pd.date_range(start=start, end=end)]

    @task(max_active_tis_per_dag=1, map_index_template="{{ task.op_kwargs['date_str'] }}")
    def process_single_day(date_str: str):
        """
        Atomic Pipeline: Download -> Bronze -> Silver for one specific day.
        """
        logging.info(f"--- 🚀 Starting Pipeline for {date_str} ---")
        con = get_connection()
        
        # --- 1. URL Construction ---
        # Input: "20230101"
        year = date_str[:4]    # "2023"
        month = date_str[4:6]  # "01"
        
        # Format: /2023-01/20230101...
        url = BASE_URL_TEMPLATE.format(year=year, month=month, date=date_str)
        
        logging.info(f"Target URL: {url}")

        try:
            # ==============================================================================
            # PHASE 1: TABLE INITIALIZATION
            # ==============================================================================
            
            # 1. Check if Bronze table exists
            
            table_exists = con.execute("""
                SELECT count(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'bronze' AND table_name = 'mobility_data'
            """).fetchone()[0]
            
            if table_exists == 0:
                logging.info("Table 'mobility_data' not found. Creating from source schema...")
                # 2. Create Table Structure from the URL (Limit 0)
                # We force 'all_varchar=true' to ensure robust ingestion
                con.execute(f"""
                    CREATE TABLE lakehouse.bronze.mobility_data AS 
                    SELECT 
                        *,
                        CURRENT_TIMESTAMP AS ingestion_timestamp,
                        CAST('{url}' AS VARCHAR) AS source_url
                    FROM read_csv_auto('{url}', all_varchar=true, ignore_errors=true)
                    LIMIT 0;
                """)
                
                # 3. Configure Partitioning immediately after creation
                con.execute("ALTER TABLE lakehouse.bronze.mobility_data SET PARTITIONED BY (fecha);")
                logging.info("✅ Bronze Table created successfully.")

            # Silver Table
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
            except:
                pass

            # ==============================================================================
            # PHASE 2: BRONZE UPSERT (Streaming)
            # ==============================================================================
            logging.info("⬇️ Bronze: Streaming data...")
            
            # Idempotency: Delete if exists
            con.execute(f"DELETE FROM lakehouse.bronze.mobility_data WHERE fecha = '{date_str}'")
            
            # Stream Insert
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

            # ==============================================================================
            # PHASE 3: SILVER UPSERT (Transformation)
            # ==============================================================================
            logging.info("🔄 Silver: Transforming data...")
            
            # Idempotency: Delete partition
            con.execute(f"DELETE FROM lakehouse.silver.fact_mobility WHERE partition_date = strptime('{date_str}', '%Y%m%d')")
            
            # Transform & Load
            con.execute(f"""
                INSERT INTO lakehouse.silver.fact_mobility
                SELECT 
                    (try_strptime(m.fecha, '%Y%m%d') + (CAST(m.periodo AS INTEGER) * INTERVAL 1 HOUR)) 
                        AT TIME ZONE 'Europe/Madrid' AS period,
                    
                    zo.zone_id AS origin_zone_id,
                    zd.zone_id AS destination_zone_id,
                    
                    TRY_CAST(REPLACE(REPLACE(m.viajes, '.', ''), ',', '.') AS DOUBLE) AS trips,
                    
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

        except Exception as e:
            logging.error(f"❌ Failed on {date_str} (URL: {url}): {e}")
            raise e
            
        finally:
            con.close()

    @task
    def audit_batch_results(**context):
        logging.info("🕵️ Starting Batch Quality Audit...")
        con = get_connection()
        
        # 1. Get Range from Params
        conf = context['dag_run'].conf or {}
        params = context['params']
        start_str = conf.get('start_date') or params['start_date']
        end_str = conf.get('end_date') or params['end_date']
        
        logging.info(f"Auditing range: {start_str} to {end_str}")

        try:
            # 2. Calculate Aggregates for the whole batch
            # We query the Silver table for the date range
            stats = con.execute(f"""
                SELECT 
                    COUNT(*) as total_rows,
                    SUM(trips) as total_trips,
                    COUNT(DISTINCT partition_date) as days_loaded,
                    -- Check for NULL zones (Data Quality Indicator)
                    COUNT(*) FILTER (WHERE origin_zone_id IS NULL OR destination_zone_id IS NULL) as bad_rows
                FROM lakehouse.silver.fact_mobility
                WHERE partition_date BETWEEN strptime('{start_str}', '%Y%m%d') AND strptime('{end_str}', '%Y%m%d')
            """).fetchone()
            
            total_rows, total_trips, days_loaded, bad_rows = stats
            
            # 3. Log to Data Quality Table
            batch_note = f"Batch: {start_str}-{end_str}"
            
            # Helper to insert
            def log_dq(metric, val):
                con.execute(f"INSERT INTO lakehouse.silver.data_quality_log VALUES (CURRENT_TIMESTAMP, 'fact_mobility_batch', '{metric}', {val or 0}, '{batch_note}')")

            log_dq('batch_total_rows', total_rows)
            log_dq('batch_total_trips', total_trips)
            log_dq('batch_days_loaded', days_loaded)
            
            # Calculate % of bad rows
            bad_pct = (bad_rows / total_rows * 100.0) if total_rows > 0 else 0.0
            log_dq('batch_bad_rows_pct', bad_pct)

            logging.info(f"📊 Audit Complete. Loaded {days_loaded} days. Trips: {total_trips:,.0f}. Bad Rows: {bad_pct:.2f}%")

        finally:
            con.close()

    # --- Flow ---
    dates = generate_date_list()
    processed_tasks = process_single_day.expand(date_str=dates)
    processed_tasks >> audit_batch_results()

mobility_02_ingest_daily()