from datetime import timedelta
from itertools import count
from airflow.decorators import dag, task
from utils_db import get_connection
import logging
import requests

@dag(
    dag_id="infraestructure_and_dimensions",
    schedule=None,
    catchup=False,
    # max_active_tasks=2,
    tags=['infrastrucure']    
)
def infrastructure_and_dimensions():

    @task
    def create_schemas():
        with get_connection() as con:
            for s in ['bronze', 'silver', 'gold']:
                con.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{s}")
        logging.info("✅ Schemas ready.")
    
    @task
    def create_stats_table():
        with get_connection() as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS lakehouse.silver.data_quality_log (
                    check_timestamp TIMESTAMP, table_name VARCHAR, metric_name VARCHAR, metric_value DOUBLE, notes VARCHAR
                );
            """)
        logging.info("✅ Log table ready.")

    @task
    def br_ingest_geo_data():
        table_name = 'geo_municipalities'
        base_url = 'https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/zonificacion_municipios'
        extensions = ['.shp', '.dbf', '.shx', '.prj']
        missing_files = []

        try:
            for ext in extensions:
                url = f"{base_url}{ext}"
                response = requests.head(url, allow_redirects=True, timeout=10)
                
                if response.status_code != 200:
                    missing_files.append(ext)
            
            if missing_files:
                logging.info(f"Ingestion aborted. Missing extensions for {table_name}: {missing_files}")
                
            else:
                logging.info(f"All files found. Starting spatial ingestion for {table_name}...")
                
                with get_connection() as con:
                    query = f"""
                        CREATE OR REPLACE TABLE lakehouse.bronze.{table_name} AS 
                        SELECT 
                            *,
                            CURRENT_TIMESTAMP as ingestion_timestamp,
                            '{base_url}.shp' as source_url
                        FROM ST_Read('{base_url}.shp');
                    """
                    con.execute(query)
                    logging.info(f"✅ {table_name} (Spatial) correctly ingested.")
                    # Check row count and columns
                    count = con.execute(f"SELECT COUNT(*) FROM lakehouse.bronze.{table_name}").fetchone()[0]
                    cols = con.execute(f"DESCRIBE lakehouse.bronze.{table_name}").fetchall()
                    col_names = [c[0] for c in cols]
                    logging.info(f"✅ {table_name}: {count} rows. Columns: {col_names}")

        except requests.RequestException as e:
            logging.error(f"Network error while attempting to verify the URLs: {e}")

    @task
    def br_ingest_zoning_municipalities():
        table_name = 'zoning_municipalities'
        url = 'https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/nombres_municipios.csv'
        sep = '|'
        header = True
        encoding = 'utf-8'

        # Check url
        try:
            response = requests.head(url, allow_redirects=True, timeout=10)
            
            if response.status_code != 200:
                logging.info(f"File for {table_name} unavailable. Status code: {response.status_code}")
            
            else:
                logging.info(f"File found. Starting ingestion for {table_name}...")
                with get_connection() as con:
                    query = f"""
                        CREATE OR REPLACE TABLE lakehouse.bronze.{table_name} AS 
                        SELECT 
                            *,
                            CURRENT_TIMESTAMP as ingestion_timestamp,
                            '{url}' as source_url
                        FROM read_csv_auto('{url}', 
                            all_varchar=true, 
                            filename=true, 
                            sep='{sep}', 
                            header={header},
                            encoding='{encoding}',
                            ignore_errors=true
                        );
                    """
                    con.execute(query)
                    logging.info(f"✅ {table_name} correctly ingested.")
                    # Check row count and columns
                    count = con.execute(f"SELECT COUNT(*) FROM lakehouse.bronze.{table_name}").fetchone()[0]
                    cols = con.execute(f"DESCRIBE lakehouse.bronze.{table_name}").fetchall()
                    col_names = [c[0] for c in cols]
                    logging.info(f"✅ {table_name}: {count} rows. Columns: {col_names}")

        except requests.RequestException as e:
            logging.error(f"Network error while attempting to verify the URL: {e}")
    
    @task
    def br_ingest_population_municipalities():
        table_name = 'population_municipalities'
        url = 'https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/poblacion_municipios.csv'
        sep = '|'
        header = False
        encoding = 'utf-8'

        # Check url
        # Check url
        try:
            response = requests.head(url, allow_redirects=True, timeout=10)
            
            if response.status_code != 200:
                logging.info(f"File for {table_name} unavailable. Status code: {response.status_code}")
            
            else:
                logging.info(f"File found. Starting ingestion for {table_name}...")
                with get_connection() as con:
                    query = f"""
                        CREATE OR REPLACE TABLE lakehouse.bronze.{table_name} AS 
                        SELECT 
                            *,
                            CURRENT_TIMESTAMP as ingestion_timestamp,
                            '{url}' as source_url
                        FROM read_csv_auto('{url}', 
                            all_varchar=true, 
                            filename=true, 
                            sep='{sep}', 
                            header={header},
                            encoding='{encoding}',
                            ignore_errors=true
                        );
                    """
                    con.execute(query)
                    logging.info(f"✅ {table_name} correctly ingested.")
                    # Check row count and columns
                    count = con.execute(f"SELECT COUNT(*) FROM lakehouse.bronze.{table_name}").fetchone()[0]
                    cols = con.execute(f"DESCRIBE lakehouse.bronze.{table_name}").fetchall()
                    col_names = [c[0] for c in cols]
                    logging.info(f"✅ {table_name}: {count} rows. Columns: {col_names}")

        except requests.RequestException as e:
            logging.error(f"Network error while attempting to verify the URL: {e}")
            
    @task
    def br_ingest_mapping_ine_mitma():
        table_name = 'mapping_ine_mitma'
        url = 'https://movilidad-opendata.mitma.es/zonificacion/relacion_ine_zonificacionMitma.csv'
        sep = '|'
        header = True
        encoding = 'utf-8'

        # Check url
        try:
            response = requests.head(url, allow_redirects=True, timeout=10)
            
            if response.status_code != 200:
                logging.info(f"File for {table_name} unavailable. Status code: {response.status_code}")
            
            else:
                logging.info(f"File found. Starting ingestion for {table_name}...")
                with get_connection() as con:
                    query = f"""
                        CREATE OR REPLACE TABLE lakehouse.bronze.{table_name} AS 
                        SELECT 
                            *,
                            CURRENT_TIMESTAMP as ingestion_timestamp,
                            '{url}' as source_url
                        FROM read_csv_auto('{url}', 
                            all_varchar=true, 
                            filename=true, 
                            sep='{sep}', 
                            header={header},
                            encoding='{encoding}',
                            ignore_errors=true
                        );
                    """
                    con.execute(query)
                    logging.info(f"✅ {table_name} correctly ingested.")
                    # Check row count and columns
                    count = con.execute(f"SELECT COUNT(*) FROM lakehouse.bronze.{table_name}").fetchone()[0]
                    cols = con.execute(f"DESCRIBE lakehouse.bronze.{table_name}").fetchall()
                    col_names = [c[0] for c in cols]
                    logging.info(f"✅ {table_name}: {count} rows. Columns: {col_names}")

        except requests.RequestException as e:
            logging.error(f"Network error while attempting to verify the URL: {e}")
        
    @task
    def br_ingest_work_calendars():
        table_name = 'work_calendars'
        url = 'https://datos.madrid.es/egob/catalogo/300082-4-calendario_laboral.csv'
        sep = ';'
        header = True
        encoding = 'utf-8'

        # Check url
        try:
            response = requests.head(url, allow_redirects=True, timeout=10)
            
            if response.status_code != 200:
                logging.info(f"File for {table_name} unavailable. Status code: {response.status_code}")
            
            else:
                logging.info(f"File found. Starting ingestion for {table_name}...")
                with get_connection() as con:
                    query = f"""
                        CREATE OR REPLACE TABLE lakehouse.bronze.{table_name} AS 
                        SELECT 
                            *,
                            CURRENT_TIMESTAMP as ingestion_timestamp,
                            '{url}' as source_url
                        FROM read_csv_auto('{url}', 
                            all_varchar=true, 
                            filename=true, 
                            sep='{sep}', 
                            header={header},
                            encoding='{encoding}',
                            ignore_errors=true
                        );
                    """
                    con.execute(query)
                    logging.info(f"✅ {table_name} correctly ingested.")
                    # Check row count and columns
                    count = con.execute(f"SELECT COUNT(*) FROM lakehouse.bronze.{table_name}").fetchone()[0]
                    cols = con.execute(f"DESCRIBE lakehouse.bronze.{table_name}").fetchall()
                    col_names = [c[0] for c in cols]
                    logging.info(f"✅ {table_name}: {count} rows. Columns: {col_names}")

        except requests.RequestException as e:
            logging.error(f"Network error while attempting to verify the URL: {e}")
        
    @task
    def br_ingest_ine_rent_municipalities():
        table_name = 'ine_rent_municipalities'
        url = 'https://www.ine.es/jaxiT3/files/t/es/csv_bd/30824.csv?nocab=1'
        sep = '\t'
        header = True
        encoding = 'utf-8'

        # Check url
        try:
            response = requests.head(url, allow_redirects=True, timeout=30)
            
            if response.status_code != 200:
                logging.info(f"File for {table_name} unavailable. Status code: {response.status_code}")
            
            else:
                logging.info(f"File found. Starting ingestion for {table_name}...")
                with get_connection() as con:
                    query = f"""
                        CREATE OR REPLACE TABLE lakehouse.bronze.{table_name} AS 
                        SELECT 
                            *,
                            CURRENT_TIMESTAMP as ingestion_timestamp,
                            '{url}' as source_url
                        FROM read_csv_auto('{url}', 
                            all_varchar=true, 
                            filename=true, 
                            sep='{sep}', 
                            header={header},
                            encoding='{encoding}',
                            ignore_errors=true
                        );
                    """
                    con.execute(query)
                    logging.info(f"✅ {table_name} correctly ingested.")
                    # Check row count and columns
                    count = con.execute(f"SELECT COUNT(*) FROM lakehouse.bronze.{table_name}").fetchone()[0]
                    cols = con.execute(f"DESCRIBE lakehouse.bronze.{table_name}").fetchall()
                    col_names = [c[0] for c in cols]
                    logging.info(f"✅ {table_name}: {count} rows. Columns: {col_names}")

        except requests.RequestException as e:
            logging.error(f"Network error while attempting to verify the URL: {e}")
        
    @task
    def sl_ingest_dim_zones():
        logging.info("Building Silver: dim_zones")
        with get_connection() as con:
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
                    g.geom AS polygon,
                    CURRENT_TIMESTAMP AS processed_at
                    
                FROM raw_zones r
                JOIN lakehouse.bronze.geo_municipalities g
                    ON TRIM(CAST(g.id AS VARCHAR)) = CAST(r.mitma_code AS VARCHAR)
                ORDER BY zone_id;
            """
            con.execute(query_dim_zones)
            # This we can remove later
            count = con.execute(f"SELECT COUNT(*) FROM lakehouse.silver.dim_zones").fetchone()[0]
        logging.info(f"✅ Table created: lakehouse.silver.dim_zones ({count} rows)")

    @task
    def sl_ingest_metric_population():
        logging.info("Building Silver: metric_population")
        with get_connection() as con:
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
         
    @task
    def sl_ingest_metric_ine_rent():
        logging.info("Building Silver: metric_ine_rent")
        with get_connection() as con:
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
        
    @task
    def sl_ingest_dim_zone_holidays():
        logging.info("Building Silver: dim_zone_holidays")
        with get_connection() as con:
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

    @task
    def audit_dimensions():
        logging.info("🕵️ Starting Data Quality Audit for Dimensions.")

        # Helper to insert into log
        def log_metric(table, metric, value, notes=''):
            safe_val = value if value is not None else 0.0
            query = f"""
                INSERT INTO lakehouse.silver.data_quality_log 
                VALUES (CURRENT_TIMESTAMP, '{table}', '{metric}', {safe_val}, '{notes}')
            """
            con.execute(query)
            logging.info(f"   -> Audited {table}: {metric} = {safe_val}")

        with get_connection() as con:
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

            avg_rent = con.execute("SELECT AVG(income_per_capita) FROM lakehouse.silver.metric_ine_rent").fetchone()[0]
            log_metric('metric_ine_rent', 'avg_income_per_capita', avg_rent, 'National Avg')
            
            rent_coverage = con.execute("""
                SELECT (SELECT COUNT(DISTINCT zone_id) FROM lakehouse.silver.metric_ine_rent) * 100.0 / NULLIF((SELECT COUNT(*) FROM lakehouse.silver.dim_zones), 0)
            """).fetchone()[0]
            log_metric('metric_ine_rent', 'income_data_coverage_pct', rent_coverage)
        
        logging.info("🕵️ Dimensions audited.")

    # ==============================================================================
    # ORCHESTRATION FLOW
    # ==============================================================================

    task_schemas = create_schemas()
    task_stats = create_stats_table()

    # Bronze Ingestion
    task_geo = br_ingest_geo_data()
    task_zoning = br_ingest_zoning_municipalities()
    task_pop = br_ingest_population_municipalities()
    task_mapping = br_ingest_mapping_ine_mitma()
    task_calendars = br_ingest_work_calendars()
    task_rent = br_ingest_ine_rent_municipalities()

    # Silver Transformation
    task_dim_zones = sl_ingest_dim_zones()
    task_metric_pop = sl_ingest_metric_population()
    task_metric_rent = sl_ingest_metric_ine_rent()
    task_dim_holidays = sl_ingest_dim_zone_holidays()

    # Audit
    task_audit = audit_dimensions()

    # Dependencies
    task_schemas >> task_stats
    task_stats >> [
        task_geo, 
        task_zoning, 
        task_pop, 
        task_mapping, 
        task_calendars, 
        task_rent
    ]

    [task_geo, task_zoning, task_mapping] >> task_dim_zones

    task_dim_zones >> task_metric_pop
    task_pop >> task_metric_pop

    task_dim_zones >> task_metric_rent
    task_rent >> task_metric_rent

    task_dim_zones >> task_dim_holidays
    task_calendars >> task_dim_holidays

    [task_metric_pop, task_metric_rent, task_dim_holidays] >> task_audit

infrastructure_and_dimensions()

