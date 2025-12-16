from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import logging
import requests
import io
import matplotlib
# 'Agg' is required for Airflow/Docker (Headless mode, no GUI window)
matplotlib.use('Agg') 
import matplotlib.pyplot as plt 
from pyspainmobility import Zones

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# 1. Static Sources (Dimensions)
SOURCES_CONFIG = [
    {
        'table_name': 'zoning_municipalities',
        'url': 'https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/nombres_municipios.csv',
        'sep': '|', 'header': True, 'encoding': 'utf-8'
    },
    {
        'table_name': 'population_municipalities',
        'url': 'https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/poblacion_municipios.csv',
        'sep': '|', 'header': False, 'encoding': 'utf-8'
    },
    {
        'table_name': 'mapping_ine_mitma',
        'url': 'https://movilidad-opendata.mitma.es/zonificacion/relacion_ine_zonificacionMitma.csv',
        'sep': '|', 'header': True, 'encoding': 'utf-8'
    },
    {
        'table_name': 'work_calendars',
        'url': 'https://datos.madrid.es/egob/catalogo/300082-4-calendario_laboral.csv',
        'sep': ';', 'header': True, 'encoding': 'utf-8'
    },
    {
        'table_name': 'ine_rent_municipalities',
        'url': 'https://www.ine.es/jaxiT3/files/t/es/csv_bd/30824.csv?nocab=1',
        'header': True, 'sep': '\t', 'encoding': 'ISO-8859-1'
    }
]

# 2. Dynamic Sources (Mobility Facts)
BASE_URL_TEMPLATE = "https://movilidad-opendata.mitma.es/estudios_basicos/por-municipios/viajes/ficheros-diarios/{year}-{month}/{date}_Viajes_municipios.csv.gz"

@dag(
    dag_id="mobility_unified_pipeline_withgold",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="YYYYMMDD"),
        "end_date": Param("20230101", type="string", description="YYYYMMDD"),
    },
    tags=['mobility', 'sprint3', 'unified']
)
def mobility_unified_pipeline_withgold():

    # ==============================================================================
    # PART 1: INFRASTRUCTURE & BRONZE
    # ==============================================================================

    @task
    def create_schemas():
        con = get_connection()
        for s in ['bronze', 'silver', 'gold']:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{s}")
        con.execute("""
            CREATE TABLE IF NOT EXISTS lakehouse.silver.data_quality_log (
                check_timestamp TIMESTAMP, table_name VARCHAR, metric_name VARCHAR, metric_value DOUBLE, notes VARCHAR
            );
        """)
        logging.info("✅ Schemas & Log table ready.")
        con.close()

    @task
    def ingest_geo_data():
        logging.info("Starting Geo Ingestion via pyspainmobility...")
        con = get_connection()
        zones_api = Zones(zones="municipios", version=2)
        gdf = zones_api.get_zone_geodataframe()
        if gdf.crs is None: gdf.set_crs(epsg=4326, inplace=True)
        gdf['wkt_polygon'] = gdf.geometry.to_wkt()

        if 'ID' in gdf.columns:
            gdf.rename(columns={'ID': 'id'}, inplace=True)
        else:
            gdf['id'] = gdf.index
        
        df_export = pd.DataFrame(gdf.drop(columns=['geometry']))
        df_export['filename'] = 'pyspainmobility_api_v2'
        df_export['ingestion_timestamp'] = pd.Timestamp.now()
        df_export['source_url'] = 'pypi'
        
        con.register('df_view', df_export)
        con.execute("CREATE OR REPLACE TABLE lakehouse.bronze.geo_municipalities AS SELECT * FROM df_view")
        con.close()

    @task
    def ingest_static_csvs():
        """
        Streams static CSVs directly from WEB.
        Fixes the 'ï»¿' (BOM) artifact in column names.
        """
        con = get_connection()
        con.execute("SET http_keep_alive=false;") 
        
        # Headers to mimic a real Chrome browser (Anti-Blocking)
        HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        for source in SOURCES_CONFIG:
            table = source['table_name']
            url = source['url']
            
            try:
                logging.info(f"⬇️ Ingesting {table} from WEB...")
                
                if 'ine.es' in url:
                    # 1. Download with Timeout and Headers
                    response = requests.get(url, headers=HEADERS, timeout=120)
                    response.raise_for_status()
                    
                    # 2. Read into Pandas (Try utf-8-sig to handle BOM automatically)
                    content = response.content
                    csv_data = io.BytesIO(content) # Use BytesIO to let pandas handle encoding detection
                    
                    try:
                        # 'utf-8-sig' is magic: it automatically removes the 'ï»¿' at the start
                        df_ine = pd.read_csv(csv_data, sep='\t', thousands='.', dtype=str, encoding='utf-8-sig')
                        
                        # Check if separator was wrong (if only 1 column found)
                        if df_ine.shape[1] < 2:
                             csv_data.seek(0)
                             df_ine = pd.read_csv(csv_data, sep=';', thousands='.', dtype=str, encoding='utf-8-sig')
                    except:
                        # Fallback: If utf-8-sig fails, use ISO-8859-1 and clean manually later
                        csv_data.seek(0)
                        df_ine = pd.read_csv(csv_data, sep='\t', thousands='.', dtype=str, encoding='ISO-8859-1')

                    # 3. --- SURGICAL CLEANING (CRITICAL STEP) ---
                    # Strip whitespace from column names
                    df_ine.columns = df_ine.columns.str.strip()
                    # Explicitly remove the BOM artifact if it persists
                    df_ine.columns = df_ine.columns.str.replace('ï»¿', '', regex=False)
                    
                    logging.info(f"✨ Cleaned Columns: {list(df_ine.columns)}")
                    
                    # Safety Check
                    if 'Municipios' not in df_ine.columns:
                        logging.error(f"⚠️ Still cannot find 'Municipios'. Current columns: {df_ine.columns}")
                        # Force rename the first column if it looks like the right one
                        if 'Municipios' in df_ine.columns[0]:
                             cols = list(df_ine.columns)
                             cols[0] = 'Municipios'
                             df_ine.columns = cols
                             logging.info("🔧 Forced rename of column 0 to 'Municipios'")

                    # 4. Save to Bronze
                    df_ine['ingestion_timestamp'] = pd.Timestamp.now()
                    df_ine['source_url'] = url
                    
                    con.register('view_ine', df_ine)
                    con.execute(f"CREATE OR REPLACE TABLE lakehouse.bronze.{table} AS SELECT * FROM view_ine")
                    logging.info(f"✅ {table} ingested (Rows: {len(df_ine)})")
                
                else:
                    # REST OF SOURCES (Direct DuckDB)
                    header = str(source.get('header', True)).lower()
                    sep = source.get('sep', ',')
                    encoding = source.get('encoding', 'utf-8')
                    query = f"""
                        CREATE OR REPLACE TABLE lakehouse.bronze.{table} AS 
                        SELECT *, CURRENT_TIMESTAMP as ingestion_timestamp, '{url}' as source_url
                        FROM read_csv_auto('{url}', all_varchar=true, filename=true, sep='{sep}', header={header}, encoding='{encoding}', ignore_errors=true);
                    """
                    con.execute(query)
                    logging.info(f"✅ {table} ingested via DuckDB")
                    
            except Exception as e:
                logging.error(f"❌ Error ingesting {table}: {e}")
                # We want the task to fail if INE fails, to be aware of data quality issues
                if 'ine.es' in url: raise e
        
        con.close()

    @task
    def build_silver_dimensions():
        """
        Runs SQL transformations to create Silver Dimension tables.
        """
        con = get_connection()
        
        # --- 1. Dim Zones (Geo + Names + Codes) ---
        logging.info("Building Silver: dim_zones")
        query_dim_zones = """
            CREATE OR REPLACE TABLE lakehouse.silver.dim_zones AS
            WITH unique_mapping AS (
                SELECT DISTINCT 
                    CAST(municipio_mitma AS VARCHAR) as mitma_ref,
                    MIN(CAST(municipio_ine AS VARCHAR)) as ine_ref
                FROM lakehouse.bronze.mapping_ine_mitma
                WHERE municipio_mitma IS NOT NULL
                    AND municipio_ine IS NOT NULL
                    AND municipio_ine NOT LIKE 'NA'
                    AND municipio_mitma NOT LIKE 'NA'
                GROUP BY municipio_mitma
            ),
            raw_zones AS (
                SELECT 
                    TRIM(z.ID) AS mitma_code,
                    TRIM(m.ine_ref)  AS ine_code,
                    TRIM(z.name) AS zone_name
                FROM lakehouse.bronze.zoning_municipalities z
                INNER JOIN unique_mapping m 
                    ON TRIM(z.ID) = TRIM(m.mitma_ref)
                WHERE z.ID IS NOT NULL AND z.ID != 'ID'
                GROUP BY z.ID, z.name, m.ine_ref
            )
            SELECT
                -- 1. Codes
                ROW_NUMBER() OVER (ORDER BY mitma_code) AS zone_id,
                mitma_code,
                ine_code,
                zone_name,
                ST_GeomFromText(g.wkt_polygon) AS polygon,
                CURRENT_TIMESTAMP AS processed_at
                
            FROM raw_zones r
            JOIN lakehouse.bronze.geo_municipalities g
                ON TRIM(CAST(g.id AS VARCHAR)) = CAST(r.mitma_code AS VARCHAR)
            ORDER BY zone_id;
        """
        con.execute(query_dim_zones)
        count = con.execute(f"SELECT COUNT(*) FROM lakehouse.silver.dim_zones").fetchone()[0]
        logging.info(f"✅ Table created: lakehouse.silver.dim_zones ({count} rows)")

        # --- 2. Metric Population ---
        logging.info("Building Silver: metric_population")
        con.execute("""
            CREATE OR REPLACE TABLE lakehouse.silver.metric_population AS
            SELECT 
                -- 1. Linking Key (Map column0 -> zone_id)
                z.zone_id,
                
                -- 2. The Metric (Map column1 -> population)
                -- Logic:
                --   a. Cast to Integer
                CAST(TRY_CAST(column1 AS DOUBLE) AS BIGINT) AS population,
                
                -- 3. Metadata
                2023 AS year,
                CURRENT_TIMESTAMP AS processed_at
                
            FROM lakehouse.bronze.population_municipalities p
                JOIN lakehouse.silver.dim_zones z ON TRIM(p.column0) = z.mitma_code
            
            WHERE 
                -- Filter out empty rows
                column0 IS NOT NULL 
                -- Filter out the header row (if the first row contains text like 'ID' or 'Poblacion')
                AND NOT regexp_matches(column1, '[a-zA-Z]') -- Exclude rows where population contains letters
        """)
        count = con.execute(f"SELECT COUNT(*) FROM lakehouse.silver.metric_population").fetchone()[0]
        logging.info(f"✅ Table created: lakehouse.silver.metric_population ({count} rows)")

        # --- 3. Metric Rent ---
        logging.info("Building Silver: metric_ine_rent")
        try:
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.silver.metric_ine_rent AS
                SELECT 
                    -- 1. Master Key (Zone ID from our Dimension)
                    z.zone_id,
                    
                    -- 2. The Metric (Cleaned)
                    -- Format: "13.500" -> 13500. Handle "dirty" data (like ".") using TRY_CAST
                    TRY_CAST(REPLACE(r.Total, '.', '') AS DOUBLE) AS income_per_capita,
                    
                    -- 3. Time Reference
                    CAST(r.Periodo AS INTEGER) AS year,
                    
                    -- 4. Metadata
                    CURRENT_TIMESTAMP AS processed_at
                    
                FROM lakehouse.bronze.ine_rent_municipalities r
                
                -- JOIN Logic: Match Extracted INE Code to Zone INE Code
                -- We split "01001 Name" by space to get "01001"
                JOIN lakehouse.silver.dim_zones z 
                    ON split_part(r.Municipios, ' ', 1) = z.ine_code
                    
                WHERE 
                    -- Filter 1: Only the specific indicator requested
                    r."Indicadores de renta media" = 'Renta neta media por persona'
                    
                    -- Filter 2: Ensure we are at Municipality level (Districts/Sections must be empty/null)
                    AND (r.Distritos IS NULL OR r.Distritos = '')
                    AND (r.Secciones IS NULL OR r.Secciones = '')
                    
                    -- Filter 3: Valid data
                    AND TRY_CAST(REPLACE(r.Total, '.', '') AS DOUBLE) IS NOT NULL
                    AND z.zone_id IS NOT NULL;
            """)
            count = con.execute(f"SELECT COUNT(*) FROM lakehouse.silver.metric_ine_rent").fetchone()[0]
            logging.info(f"✅ Table created: lakehouse.silver.metric_ine_rent ({count} rows)")
        except Exception as e:
             logging.warning(f"Skipping rent table due to schema mismatch or missing data: {e}")

        # --- 4. Dim Zone Holidays ---
        logging.info("Building Silver: dim_zone_holidays")
        con.execute("""
            CREATE OR REPLACE TABLE lakehouse.silver.dim_zone_holidays AS
            WITH national_days_2023 AS (
                -- 1. Get unique National Holidays and force them to year 2023
                SELECT DISTINCT
                    MAKE_DATE(
                        2023, 
                        MONTH(strptime("Dia", '%d/%m/%Y')), 
                        DAY(strptime("Dia", '%d/%m/%Y'))
                    ) AS holiday_date
                FROM lakehouse.bronze.work_calendars
                WHERE "Tipo de Festivo" ILIKE '%festivo nacional%'
                OR "Tipo de Festivo" ILIKE '%fiesta nacional%'
            )
            SELECT 
                z.zone_id,
                nd.holiday_date,
                CURRENT_TIMESTAMP AS processed_at
                
            FROM lakehouse.silver.dim_zones z
            -- CROSS JOIN: Every zone gets these holidays
            CROSS JOIN national_days_2023 nd
            
            ORDER BY z.zone_id, nd.holiday_date;
        """)
        count = con.execute(f"SELECT COUNT(*) FROM lakehouse.silver.dim_zone_holidays").fetchone()[0]
        logging.info(f"✅ Table created: lakehouse.silver.dim_zone_holidays ({count} rows)")

        con.close()

    @task
    def audit_dimensions():
        logging.info("🕵️ Starting Data Quality Audit for Dimensions...")
        con = get_connection()

        # Helper to insert into log
        def log_metric(table, metric, value, notes=''):
            safe_val = value if value is not None else 0.0
            query = f"""
                INSERT INTO lakehouse.silver.data_quality_log 
                VALUES (CURRENT_TIMESTAMP, '{table}', '{metric}', {safe_val}, '{notes}')
            """
            con.execute(query)
            logging.info(f"   -> Audited {table}: {metric} = {safe_val}")

        # 1. Zone Checks
        missing_ine = con.execute("SELECT COUNT(*) FROM lakehouse.silver.dim_zones WHERE ine_code IS NULL").fetchone()[0]
        log_metric('dim_zones', 'zones_missing_ine_code', missing_ine)

        missing_geo = con.execute("SELECT COUNT(*) FROM lakehouse.silver.dim_zones WHERE polygon IS NULL").fetchone()[0]
        log_metric('dim_zones', 'zones_missing_geo_coords', missing_geo)
        
        zone_count = con.execute("SELECT COUNT(*) FROM lakehouse.silver.dim_zones").fetchone()[0]
        log_metric('dim_zones', 'total_zones', zone_count)

        # 2. Population Checks
        pop_sum = con.execute("SELECT SUM(population) FROM lakehouse.silver.metric_population").fetchone()[0]
        log_metric('metric_population', 'total_population_sum', pop_sum, 'Spain Total')

        # 3. Rent Checks
        try:
            avg_rent = con.execute("SELECT AVG(income_per_capita) FROM lakehouse.silver.metric_ine_rent").fetchone()[0]
            log_metric('metric_ine_rent', 'avg_income_per_capita', avg_rent, 'National Avg')
            
            rent_coverage = con.execute("""
                SELECT (SELECT COUNT(DISTINCT zone_id) FROM lakehouse.silver.metric_ine_rent) * 100.0 / NULLIF((SELECT COUNT(*) FROM lakehouse.silver.dim_zones), 0)
            """).fetchone()[0]
            log_metric('metric_ine_rent', 'income_data_coverage_pct', rent_coverage)
        except:
            logging.warning("Skipping rent audit (table might be empty)")

        con.close()

    # ==============================================================================
    # PART 2: SILVER FACTS (Dynamic)
    # ==============================================================================

    @task
    def generate_date_list(**context):
        """Generates list of YYYYMMDD strings."""
        conf = context['dag_run'].conf or {}
        params = context['params']
        start = conf.get('start_date') or params['start_date']
        end = conf.get('end_date') or params['end_date']
        
        return [d.strftime("%Y%m%d") for d in pd.date_range(start=start, end=end)]
    
    @task
    def ensure_fact_tables_exist(**context):
        """
        Runs ONCE before parallel processing to avoid Race Conditions.
        Creates Bronze and Silver tables if they don't exist.
        """
        logging.info("🛠 Checking/Creating Table Structures...")
        con = get_connection()
        
        # 1. Get a sample URL (using start_date) to infer Bronze Schema
        conf = context['dag_run'].conf or {}
        params = context['params']
        date_str = conf.get('start_date') or params['start_date']
        
        year = date_str[:4]
        month = date_str[4:6]
        url = BASE_URL_TEMPLATE.format(year=year, month=month, date=date_str)
        
        try:
            # Start transaction
            con.begin()

            # --- Bronze Init ---
            table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema='bronze' AND table_name='mobility_data'").fetchone()[0]
            
            if table_exists == 0:
                logging.info(f"Creating Bronze table using sample: {url}")
                # Create empty table based on schema from URL
                con.execute(f"""
                    CREATE TABLE lakehouse.bronze.mobility_data AS 
                    SELECT 
                        *,
                        CURRENT_TIMESTAMP AS ingestion_timestamp,
                        CAST('{url}' AS VARCHAR) AS source_url
                    FROM read_csv_auto('{url}', all_varchar=true, ignore_errors=true)
                    LIMIT 0;
                """)
                con.execute("ALTER TABLE lakehouse.bronze.mobility_data SET PARTITIONED BY (fecha);")
            else:
                logging.info("Bronze table already exists.")

            # --- Silver Init ---
            # Standard Create If Not Exists is safe here because this task runs alone
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
            
            logging.info("✅ Table initialization complete.")

            # Commit transaction
            con.commit()

        except Exception as e:
            con.rollback()
            logging.error(f"❌ Failed on creating the bronze and silver mobility tables.")
            raise e
        finally:
            con.close()

    @task(max_active_tis_per_dag=2, map_index_template="{{ task.op_kwargs['date_str'] }}")
    def process_single_day(date_str: str):
        """
        Atomic Pipeline: Download -> Bronze -> Silver for one specific day.
        """
        logging.info(f"--- 🚀 Starting Pipeline for {date_str} ---")

        try:
            con = get_connection()

            # Start Transaction
            con.begin()

            # Si esto no está, DuckDB crashea al ver una URL 'https://'
            con.execute("INSTALL httpfs; LOAD httpfs;")
            
            # Evita que el servidor del ministerio corte la conexión a mitad de descarga
            con.execute("SET http_keep_alive=false;")
            
            # --- 1. URL Construction ---
            # Input: "20230101"
            year = date_str[:4]    # "2023"
            month = date_str[4:6]  # "01"
            
            # Format: /2023-01/20230101...
            url = BASE_URL_TEMPLATE.format(year=year, month=month, date=date_str)
            
            logging.info(f"Target URL: {url}")

        
            # ==============================================================================
            # PHASE 1: BRONZE UPSERT (Streaming)
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
            # PHASE 2: SILVER UPSERT (Transformation)
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
            
            # Commit transaction
            con.commit()

        except Exception as e:
            con.rollback()
            logging.error(f"❌ Failed on {date_str}: {e}")
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
            
            total_rows = stats[0] or 0
            total_trips = stats[1] or 0.0
            days_loaded = stats[2] or 0
            bad_rows = stats[3] or 0
            
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

    # ==============================================================================
    # PART 3: GOLD (Table Creation Only) 
    # ==============================================================================

    @task
    def create_gold_clustering(**context):
        """Generates Clustering Tables using ALL available data (No filters)."""
        logging.info("--- 🏗️ Starting Gold: Clustering Analysis ---")
        con = get_connection()
    
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
        
            # --- 1. DATA PREPARATION (Fetch) ---
            logging.info("   -> Fetching data from Silver Layer...")
            query_fetch = """
                SELECT 
                    partition_date AS date,
                    hour(period)   AS hour,
                    SUM(trips)     AS total_trips
                FROM lakehouse.silver.fact_mobility
                WHERE trips IS NOT NULL
                GROUP BY 1, 2
                ORDER BY 1, 2;
            """
            df = con.execute(query_fetch).df()

            if df.empty:
                logging.warning("   ⚠️ Warning: No data found in fact_mobility.")
                return

            logging.info(f"📊 Rows fetched: {len(df)}")

            # --- PIVOT & NORMALIZE ---
            # Transform from Long to Wide format (Rows=Days, Columns=Hours 0-23)
            df_pivot = df.pivot(index='date', columns='hour', values='total_trips').fillna(0)

            # Ensure all hour columns (0 to 23) exist
            for h in range(24):
                if h not in df_pivot.columns:
                    df_pivot[h] = 0
                    
            # Sort columns numerically
            df_pivot = df_pivot.sort_index(axis=1)

            # Normalize row-wise (each day sums to 1)
            logging.info("   -> Normalizing daily profiles...")
            row_sums = df_pivot.sum(axis=1)
            # Usamos replace(0, 1) para evitar divisiones por cero si un día no tiene viajes
            df_normalized = df_pivot.div(row_sums, axis=0).fillna(0)

            # --- 3. CLUSTERING (K-Means) ---
            # Fixed k=3 as per original script
            n_clusters = 3
            logging.info(f"   -> Running K-Means Clustering (k={n_clusters})...")

            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            clusters = kmeans.fit_predict(df_normalized)

            # Create results DataFrame
            df_results = pd.DataFrame({
                'date': df_normalized.index,
                'cluster_id': clusters
            })

            # --- 4. BUILD GOLD TABLE ---
            logging.info("\n--- 🏗️ Building Gold Table: typical_day_by_cluster ---")

            # Register the clustering results as a virtual view inside DuckDB
            con.register('view_dim_clusters', df_results)

            # Query EXACTA del primer script (usando CTE y columnas extra)
            query_typical_cte = """
                CREATE OR REPLACE TABLE lakehouse.gold.typical_day_by_cluster AS
                
                WITH dim_mobility_patterns AS (
                    SELECT date, cluster_id FROM view_dim_clusters
                )
                
                SELECT 
                    p.cluster_id,
                    hour(f.period) as hour,
                    ROUND(AVG(f.trips), 2) as avg_trips,
                    SUM(f.trips) as total_trips_sample, -- Columna añadida según script 1
                    CURRENT_TIMESTAMP as processed_at   -- Columna añadida según script 1
                FROM lakehouse.silver.fact_mobility f
                JOIN dim_mobility_patterns p ON f.partition_date = p.date
                GROUP BY p.cluster_id, hour(f.period)
                ORDER BY p.cluster_id, hour(f.period);
            """

            con.execute(query_typical_cte)
            logging.info("✅ Created: lakehouse.gold.typical_day_by_cluster")

            # --- 5. CLUSTER INTERPRETATION (Analysis) ---
            logging.info("\n--- 📊 Cluster Interpretation ---")
            
            # Query analítica sobre la vista registrada
            analysis_df = con.execute(""" 
                SELECT 
                    cluster_id, 
                    COUNT(*) as days_in_cluster,
                    MODE(dayname(date)) as typical_day
                FROM view_dim_clusters
                GROUP BY cluster_id
                ORDER BY days_in_cluster DESC
            """).df()

            if not analysis_df.empty:
                # Imprimimos tabla en los logs línea por línea para que sea legible en Airflow
                logging.info("\n" + analysis_df.to_string(index=False))
            else:
                logging.warning("⚠️ The analysis table is empty.")

            try:
                # Query específica para el gráfico (Join con etiquetas de día)
                query_plot = """
                WITH cluster_labels AS (
                    SELECT 
                        cluster_id, 
                        MODE(dayname(date)) as label
                    FROM view_dim_clusters
                    GROUP BY cluster_id
                )
                SELECT 
                    t.hour,
                    -- Etiqueta legible: "Cluster 0 (Sunday)"
                    'Cluster ' || t.cluster_id || ' (' || l.label || ')' as pattern_name,
                    t.avg_trips
                FROM lakehouse.gold.typical_day_by_cluster t
                JOIN cluster_labels l ON t.cluster_id = l.cluster_id
                ORDER BY t.hour;
                """
                
                demand_df = con.execute(query_plot).df()

                if demand_df.empty:
                    logging.warning("⚠️ Gold table empty, skipping plot.")
                else:
                    # Pivotar para Matplotlib
                    pivot_df = demand_df.pivot(index='hour', columns='pattern_name', values='avg_trips')
                    
                    # Crear figura
                    fig, ax = plt.subplots(figsize=(12, 7))
                    
                    # Plotear
                    pivot_df.plot(kind='line', ax=ax, marker='o', markersize=4)
                    
                    # Estilos
                    ax.set_title(f'Typical Daily Mobility Patterns (k={n_clusters})', fontsize=16)
                    ax.set_xlabel('Hour of Day', fontsize=12)
                    ax.set_ylabel('Average Trips per Hour', fontsize=12)
                    ax.set_xticks(range(0, 24))
                    ax.set_xticklabels([f'{h:02d}:00' for h in range(24)], rotation=45, ha='right')
                    ax.grid(True, linestyle='--', alpha=0.6)
                    ax.legend(title='Identified Pattern')
                    plt.tight_layout()
                    
                    # GUARDAR IMAGEN (Importante: No usar show() en Airflow)
                    output_path = "/usr/local/airflow/dags/mobility_patterns1.png"
                    plt.savefig(output_path)
                    plt.close() # Liberar memoria
                    
                    logging.info(f"📸 Graph saved successfully at: {output_path}")

            except Exception as e:
                logging.error(f"❌ Error generating plot: {e}")
                # No hacemos raise aquí para que el DAG no falle si solo es el gráfico lo que falla
            
            # Limpieza
            con.unregister('view_dim_clusters')
            logging.info("✅ Gold Layer Analysis Completed.")

        except Exception as e:
            logging.error(f"❌ Error in Gold Clustering: {e}")
            raise e # Re-raise para que Airflow marque la tarea como fallida
            
        finally:
            con.close()
      

    @task
    def create_gold_gaps(**context):
        """Generates Infrastructure Gaps Table for ALL DATA."""
        logging.info("--- 🏗️ Starting Gold: Infrastructure Gaps (Full Scope) ---")
        params = context['params']
        start, end = params['start_date'], params['end_date']
        
        con = get_connection()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            con.execute(f"""
                CREATE OR REPLACE TABLE lakehouse.gold.infrastructure_gaps AS
                WITH od_pairs AS (
                    SELECT origin_zone_id, destination_zone_id, SUM(trips) AS total_actual_trips
                    FROM lakehouse.silver.fact_mobility
                    WHERE partition_date BETWEEN strptime('{start}', '%Y%m%d') AND strptime('{end}', '%Y%m%d')
                    GROUP BY 1, 2
                ),
                model AS (
                    SELECT m.origin_zone_id, m.destination_zone_id, p.population, r.income_per_capita as rent, m.total_actual_trips,
                    -- Distancia entre centroides de zonas
                    GREATEST(0.5, st_distance_spheroid(ST_Centroid(zo.polygon), ST_Centroid(zd.polygon))/1000) AS dist_km
                    FROM od_pairs m
                    JOIN lakehouse.silver.metric_population p ON m.origin_zone_id = p.zone_id
                    JOIN lakehouse.silver.metric_ine_rent r ON m.destination_zone_id = r.zone_id
                    JOIN lakehouse.silver.dim_zones zo ON m.origin_zone_id = zo.zone_id
                    JOIN lakehouse.silver.dim_zones zd ON m.destination_zone_id = zd.zone_id
                )
                SELECT origin_zone_id, destination_zone_id, total_actual_trips,
                ((population * rent) / (dist_km * dist_km)) AS potential,
                total_actual_trips / NULLIF(((population * rent) / (dist_km * dist_km)), 0) AS mismatch_ratio
                FROM model
            """)
            logging.info("✅ Infrastructure Gaps Gold Created (All Zones).")
        finally:
            con.close()
       

    # ==============================================================================
    # ORCHESTRATION FLOW
    # ==============================================================================
    
    # 1. Setup Phase
    t_schemas = create_schemas()
    t_geo = ingest_geo_data()
    t_csvs = ingest_static_csvs()
    t_dims = build_silver_dimensions()
    t_audit_dims = audit_dimensions()

    # 2. Processing Phase (Silver)
    dates_list = generate_date_list()
    t_init_facts = ensure_fact_tables_exist()
    t_workers = process_single_day.expand(date_str=dates_list)
    t_audit_facts = audit_batch_results()

    # 3. Gold Phase (Table Creation)
    t_gold_cluster = create_gold_clustering()
    t_gold_gaps = create_gold_gaps()

    # --- DEPENDENCIES (Serial Execution) ---
    
    # Setup
    t_schemas >> [t_geo, t_csvs] >> t_dims >> t_audit_dims
    
    # Bridge
    t_audit_dims >> t_init_facts
    
    # Silver Processing
    t_init_facts >> t_workers >> t_audit_facts
    
    # Gold (Run sequentially after Audit to avoid IO Locks)
    t_audit_facts >> t_gold_cluster >> t_gold_gaps

mobility_unified_pipeline_withgold()