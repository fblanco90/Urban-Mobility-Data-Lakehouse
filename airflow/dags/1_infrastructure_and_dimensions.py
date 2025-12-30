from datetime import timedelta
from itertools import count
from airflow.decorators import dag, task
from utils_db import get_connection
import logging
import requests

@dag(
    dag_id="1_infrastructure_and_dimensions",
    schedule=None,
    catchup=False,
    # max_active_tasks=2,
    tags=['infrastructure']    
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

    @task(retries=3, retry_delay=timedelta(minutes=1))
    def br_ingest_geo_data():
        table_name = 'geo_municipalities'
        base_url = 'https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/zonificacion_municipios'
        extensions = ['.shp', '.dbf', '.shx', '.prj']
        missing_files = []

        try:
            for ext in extensions:
                url = f"{base_url}{ext}"
                response = requests.head(url, allow_redirects=True, timeout=20)
                
                if response.status_code != 200:
                    missing_files.append(ext)
                
            if missing_files:
                error_msg = f"Ingestion aborted. Missing extensions for {table_name}: {missing_files}"
                logging.info(error_msg)
                raise FileNotFoundError(error_msg)
        
        except Exception as e:
            logging.error(f"Ingestion aborted. Missing extensions for {table_name}: {missing_files}")
            raise e
        
        logging.info(f"All files found. Starting spatial ingestion for {table_name}.")
        
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
        
        logging.info(f"✅ Table created succesfully: lakehouse.bronze.{table_name}")

    @task(retries=3, retry_delay=timedelta(minutes=1))
    def br_ingest_zoning_municipalities():
        table_name = 'zoning_municipalities'
        url = 'https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/nombres_municipios.csv'
        sep = '|'
        header = True
        encoding = 'utf-8'

        # Check url
        try:
            response = requests.head(url, allow_redirects=True, timeout=20)
            
            if response.status_code != 200:
                error_msg = f"Ingestion aborted. Missing file for {table_name}."
                logging.error(error_msg)                
                raise FileNotFoundError(error_msg)
        
        except Exception as e:
            logging.error(f"❌ Error checking/ingesting files ({url}): {e}")
            raise e

        logging.info(f"File found. Starting ingestion for {table_name}.")
        
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

        logging.info(f"✅ Table created succesfully: lakehouse.bronze.{table_name}")
  
    @task(retries=3, retry_delay=timedelta(minutes=1))
    def br_ingest_population_municipalities():
        table_name = 'population_municipalities'
        url = 'https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/poblacion_municipios.csv'
        sep = '|'
        header = False
        encoding = 'utf-8'

        # Check url
        try:
            response = requests.head(url, allow_redirects=True, timeout=20)
            
            if response.status_code != 200:
                error_msg = f"Ingestion aborted. Missing file for {table_name}."
                logging.error(error_msg)                
                raise FileNotFoundError(error_msg)
        
        except Exception as e:
            logging.error(f"❌ Error checking/ingesting files ({url}): {e}")
            raise e
        
        logging.info(f"File found. Starting ingestion for {table_name}.")

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
        
        logging.info(f"✅ Table created succesfully: lakehouse.bronze.{table_name}")

    @task(retries=3, retry_delay=timedelta(minutes=1))
    def br_ingest_mapping_ine_mitma():
        table_name = 'mapping_ine_mitma'
        url = 'https://movilidad-opendata.mitma.es/zonificacion/relacion_ine_zonificacionMitma.csv'
        sep = '|'
        header = True
        encoding = 'utf-8'

        # Check url
        try:
            response = requests.head(url, allow_redirects=True, timeout=20)
            
            if response.status_code != 200:
                error_msg = f"Ingestion aborted. Missing file for {table_name}."
                logging.error(error_msg)                
                raise FileNotFoundError(error_msg)
        
        except Exception as e:
            logging.error(f"❌ Error checking/ingesting files ({url}): {e}")
            raise e
        
        logging.info(f"File found. Starting ingestion for {table_name}.")
        
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
        
        logging.info(f"✅ Table created succesfully: lakehouse.bronze.{table_name}")

    @task(retries=3, retry_delay=timedelta(minutes=1))
    def br_ingest_work_calendars():
        table_name = 'work_calendars'
        url = 'https://datos.madrid.es/egob/catalogo/300082-4-calendario_laboral.csv'
        sep = ';'
        header = True
        encoding = 'utf-8'

        # Check url
        try:
            response = requests.head(url, allow_redirects=True, timeout=20)
            
            if response.status_code != 200:
                error_msg = f"Ingestion aborted. Missing file for {table_name}."
                logging.error(error_msg)                
                raise FileNotFoundError(error_msg)
        
        except Exception as e:
            logging.error(f"❌ Error checking/ingesting files ({url}): {e}")
            raise e
        
        logging.info(f"File found. Starting ingestion for {table_name}.")
        
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
        
        logging.info(f"✅ Table created succesfully: lakehouse.bronze.{table_name}")

    @task(retries=3, retry_delay=timedelta(minutes=1))
    def br_ingest_ine_rent_municipalities():
        table_name = 'ine_rent_municipalities'
        url = 'https://www.ine.es/jaxiT3/files/t/es/csv_bd/30824.csv?nocab=1'
        sep = '\t'
        header = True
        encoding = 'utf-8'

        # Check url
        try:
            response = requests.head(url, allow_redirects=True, timeout=20)
            
            if response.status_code != 200:
                error_msg = f"Ingestion aborted. Missing file for {table_name}."
                logging.error(error_msg)                
                raise FileNotFoundError(error_msg)
        
        except Exception as e:
            logging.error(f"❌ Error checking/ingesting files ({url}): {e}")
            raise e
        
        logging.info(f"File found. Starting ingestion for {table_name}.")
        
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
        
        logging.info(f"✅ Table created succesfully: lakehouse.bronze.{table_name}")

    @task(retries=3, retry_delay=timedelta(minutes=1))
    def sl_ingest_dim_zones():
        logging.info("Building Silver: dim_zones")
        with get_connection() as con:
            con.execute("""
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
            """)

        logging.info(f"✅ Table created succesfully: lakehouse.silver.dim_zones")

    @task(retries=3, retry_delay=timedelta(minutes=1))
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
        
        logging.info(f"✅ Table created succesfully: lakehouse.silver.metric_population")
         
    @task(retries=3, retry_delay=timedelta(minutes=1))
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
        
        logging.info(f"✅ Table created succesfully: lakehouse.silver.metric_ine_rent")
        
    @task(retries=3, retry_delay=timedelta(minutes=1))
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
        
        logging.info(f"✅ Table created succesfully: lakehouse.silver.dim_zone_holidays")

    @task(retries=3, retry_delay=timedelta(minutes=1))
    def sl_create_zone_distance_matrix():
        with get_connection() as con:
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.silver.dim_zone_distances AS
                SELECT 
                    a.zone_id AS origin_zone_id,
                    b.zone_id AS destination_zone_id,
                    GREATEST(0.5, st_distance_spheroid(ST_Centroid(a.polygon), ST_Centroid(b.polygon)) / 1000) AS dist_km
                FROM lakehouse.silver.dim_zones a, lakehouse.silver.dim_zones b
                WHERE a.zone_id != b.zone_id;
            """)
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
    task_dim_zones_distances = sl_create_zone_distance_matrix()
    task_metric_pop = sl_ingest_metric_population()
    task_metric_rent = sl_ingest_metric_ine_rent()
    task_dim_holidays = sl_ingest_dim_zone_holidays()

    # Dependencies
    task_schemas >> [
        task_stats,
        task_geo, 
        task_zoning, 
        task_pop, 
        task_mapping, 
        task_calendars, 
        task_rent
    ]

    [task_geo, task_zoning, task_mapping] >> task_dim_zones

    task_dim_zones >> [task_metric_pop, task_dim_zones_distances]
    task_pop >> task_metric_pop

    task_dim_zones >> task_metric_rent
    task_rent >> task_metric_rent

    task_dim_zones >> task_dim_holidays
    task_calendars >> task_dim_holidays

infrastructure_and_dimensions()

