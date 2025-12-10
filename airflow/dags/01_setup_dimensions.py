from airflow.sdk import dag, task
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
from pyspainmobility import Zones
import logging

# --- CONFIGURATION ---
# Update these URLs if MITMA/INE changes the direct download links

SOURCES_CONFIG = [
    {
        'table_name': 'zoning_municipalities',
        'url': 'https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/nombres_municipios.csv',
        'sep': '|',
        'header': True
    },
    {
        'table_name': 'population_municipalities',
        'url': 'https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/poblacion_municipios.csv',
        'sep': '|',
        'header': False # Usually this file has no header or a weird one
    },
    {
        'table_name': 'mapping_ine_mitma',
        'url': 'https://movilidad-opendata.mitma.es/zonificacion/relacion_ine_zonificacionMitma.csv',
        'sep': '|',
        'header': True
    },
    # Example INE Rent (Update URL to the stable export link)
    {
        'table_name': 'ine_rent_municipalities',
        'url': 'https://www.ine.es/jaxiT3/files/t/es/csv_bd/30824.csv?nocab=1',
        'header': True,
        'sep': '\t',
        # 'encoding': 'ISO-8859-1'
    },
    {
        'table_name': 'work_calendars',
        'url': 'https://datos.madrid.es/egob/catalogo/300082-4-calendario_laboral.csv',
        'sep': ';',
        'header': True
    }
]

@dag(
    dag_id="mobility_01_setup_dimensions",
    start_date=datetime(2023, 1, 1),
    schedule=None, # Triggered manually or by Master DAG
    catchup=False,
    tags=['mobility', 'setup', 'static']
)
def mobility_01_setup_dimensions():

    @task
    def create_schemas():
        """Creates Bronze, Silver, Gold schemas if they don't exist."""
        con = get_connection()
        schemas = ['bronze', 'silver', 'gold']
        for s in schemas:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{s}")
            logging.info(f"Schema checked: {s}")
        
        con.execute("""
            CREATE TABLE IF NOT EXISTS lakehouse.silver.data_quality_log (
                check_timestamp TIMESTAMP,
                table_name VARCHAR,
                metric_name VARCHAR,
                metric_value DOUBLE,
                notes VARCHAR
            );
        """)
        logging.info("✅ Data Quality Log table ready.")

        con.close()

    @task
    def ingest_geo_data():
        """
        Ingests GeoJSON, adds audit columns, and saves to Bronze.
        """
        logging.info("Starting Geo Ingestion via pyspainmobility...")
        con = get_connection()
        
        # 1. Fetch from API
        zones_api = Zones(zones="municipios", version=2)
        gdf = zones_api.get_zone_geodataframe()
        
        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
            
        # 2. Transform Geometry
        gdf['wkt_polygon'] = gdf.geometry.to_wkt()
        
        # 3. Add Audit Columns (Standardizing with other Bronze tables)
        df_export = pd.DataFrame(gdf.drop(columns=['geometry']))
        
        # --- NEW: Audit Columns ---
        df_export['filename'] = 'pyspainmobility_api_v2'
        df_export['ingestion_timestamp'] = pd.Timestamp.now()
        df_export['source_url'] = 'https://pypi.org/project/pyspainmobility/'
        
        # 4. Load into Bronze
        con.register('df_view', df_export)
        con.execute("CREATE OR REPLACE TABLE lakehouse.bronze.geo_municipalities AS SELECT * FROM df_view")
        
        count = con.execute("SELECT COUNT(*) FROM lakehouse.bronze.geo_municipalities").fetchone()[0]
        logging.info(f"✅ Ingested {count} municipalities geometry with audit columns.")
        preview = con.execute("SELECT * FROM lakehouse.bronze.geo_municipalities LIMIT 5").fetchall()
        logging.info("First 5 rows of lakehouse.bronze.geo_municipalities:")
        for row in preview:
            logging.info(row)

        con.close()

    @task
    def ingest_static_csvs():
        """
        Streams CSVs from URLs directly into Bronze tables.
        """
        con = get_connection()
        
        for source in SOURCES_CONFIG:
            table_name = source['table_name']
            url = source['url']
            sep = source['sep']
            header = str(source.get('header', True)).lower() # Convert Python True -> SQL 'true'
            encoding = source.get('encoding', 'utf-8') 

            logging.info(f"Ingesting {table_name} from {url}...")
            
            try:
                # Dynamic SQL construction
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
                
                # Check rows
                count = con.execute(f"SELECT COUNT(*) FROM lakehouse.bronze.{table_name}").fetchone()[0]
                logging.info(f"✅ Table created: lakehouse.bronze.{table_name} ({count} rows)")
                cols = con.execute(f"DESCRIBE lakehouse.bronze.{table_name}").fetchall()
                col_names = [c[0] for c in cols]
                logging.info(f"✅ {table_name}: {count} rows. Columns: {col_names}")
                
            except Exception as e:
                logging.error(f"❌ Error ingesting {table_name}: {e}")
                # We don't raise error here so other tables can proceed, 
                # but in production, you might want to fail the task.
        
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
    
    # --- DAG FLOW ---
    # 1. Create Schema
    # 2. Parallel: Ingest Geo + Ingest CSVs
    # 3. Build Silver (Wait for all Bronze)
    
    init = create_schemas()
    geo = ingest_geo_data()
    csvs = ingest_static_csvs()
    silver = build_silver_dimensions()
    audit = audit_dimensions()


    # Parallel when not in local
    # init >> [geo, csvs] >> silver
    init >> geo >> csvs >> silver >> audit

mobility_01_setup_dimensions()
