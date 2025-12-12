from airflow.sdk import dag, task, Param
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
from pyspainmobility import Zones
import logging

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# 1. Static Sources (Dimensions)
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
        'header': False
    },
    {
        'table_name': 'mapping_ine_mitma',
        'url': 'https://movilidad-opendata.mitma.es/zonificacion/relacion_ine_zonificacionMitma.csv',
        'sep': '|',
        'header': True
    },
    {
        'table_name': 'ine_rent_municipalities',
        'url': 'https://www.ine.es/jaxiT3/files/t/es/csv_bd/30824.csv?nocab=1',
        'header': True,
        'sep': '\t',
    },
    {
        'table_name': 'work_calendars',
        'url': 'https://datos.madrid.es/egob/catalogo/300082-4-calendario_laboral.csv',
        'sep': ';',
        'header': True
    }
]

# 2. Dynamic Sources (Mobility Facts)
BASE_URL_TEMPLATE = "https://movilidad-opendata.mitma.es/estudios_basicos/por-municipios/viajes/ficheros-diarios/{year}-{month}/{date}_Viajes_municipios.csv.gz"

@dag(
    dag_id="mobility_unified_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="YYYYMMDD"),
        "end_date": Param("20230101", type="string", description="YYYYMMDD")
    },
    tags=['mobility', 'sprint3', 'unified']
)
def mobility_unified_pipeline():

    # ==============================================================================
    # PART 1: INFRASTRUCTURE & DIMENSIONS (Static Data)
    # ==============================================================================

    @task
    def create_schemas():
        """Creates Bronze, Silver, and Gold schemas + Audit Log table."""
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

        if 'ID' in gdf.columns:
            gdf.rename(columns={'ID': 'id'}, inplace=True)
        else:
            gdf['id'] = gdf.index
        
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
        cols = con.execute(f"DESCRIBE lakehouse.bronze.geo_municipalities").fetchall()
        col_names = [c[0] for c in cols]
        logging.info(f"✅ geo_municipalities: {count} rows. Columns: {col_names}")
        preview = con.execute("SELECT * FROM lakehouse.bronze.geo_municipalities LIMIT 5").fetchall()
        logging.info("First 5 rows of lakehouse.bronze.geo_municipalities:")
        for row in preview:
            logging.info(row)

        con.close()

    @task
    def ingest_static_csvs():
        """Streams static CSVs (population, rent, etc.) to Bronze."""
        con = get_connection()
        for source in SOURCES_CONFIG:
            try:
                header = str(source.get('header', True)).lower()
                encoding = source.get('encoding', 'utf-8')
                query = f"""
                    CREATE OR REPLACE TABLE lakehouse.bronze.{source['table_name']} AS 
                    SELECT *, CURRENT_TIMESTAMP as ingestion_timestamp, '{source['url']}' as source_url
                    FROM read_csv_auto('{source['url']}', all_varchar=true, filename=true, sep='{source['sep']}', header={header}, encoding='{encoding}', ignore_errors=true);
                """
                con.execute(query)
                logging.info(f"✅ Ingested {source['table_name']}")
            except Exception as e:
                logging.error(f"❌ Error ingesting {source['table_name']}: {e}")
        con.close()

    @task
    def build_silver_dimensions():
        """Transforms Bronze static data into Silver Dimensions."""
        con = get_connection()
        
        # 1. Dim Zones (CRITICAL - Hard Stop if fails)
        logging.info("Building Silver: dim_zones")
        con.execute("""
            CREATE OR REPLACE TABLE lakehouse.silver.dim_zones AS
            WITH unique_mapping AS (
                SELECT DISTINCT CAST(municipio_mitma AS VARCHAR) as mitma_ref, MIN(CAST(municipio_ine AS VARCHAR)) as ine_ref
                FROM lakehouse.bronze.mapping_ine_mitma 
                WHERE municipio_mitma NOT LIKE 'NA' GROUP BY municipio_mitma
            ),
            raw_zones AS (
                SELECT TRIM(z.ID) AS mitma_code, TRIM(m.ine_ref) AS ine_code, TRIM(z.name) AS zone_name
                FROM lakehouse.bronze.zoning_municipalities z
                JOIN unique_mapping m ON TRIM(z.ID) = TRIM(m.mitma_ref)
                WHERE z.ID != 'ID'
            )
            SELECT ROW_NUMBER() OVER (ORDER BY mitma_code) AS zone_id, mitma_code, ine_code, zone_name, ST_GeomFromText(g.wkt_polygon) AS polygon, CURRENT_TIMESTAMP AS processed_at
            FROM raw_zones r JOIN lakehouse.bronze.geo_municipalities g ON TRIM(CAST(g.id AS VARCHAR)) = CAST(r.mitma_code AS VARCHAR);
        """)

        # 2. Metric Population (Soft Failure recommended for Strategy)
        try:
            logging.info("Building Silver: metric_population")
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.silver.metric_population AS
                SELECT z.zone_id, CAST(TRY_CAST(column1 AS DOUBLE) AS BIGINT) AS population, 2023 AS year, CURRENT_TIMESTAMP AS processed_at
                FROM lakehouse.bronze.population_municipalities p
                JOIN lakehouse.silver.dim_zones z ON TRIM(p.column0) = z.mitma_code
                WHERE NOT regexp_matches(column1, '[a-zA-Z]')
            """)
        except Exception as e:
            logging.error(f"⚠️ Population table failed. Gravity model will be impacted: {e}")

        # 3. Metric Rent (Soft Failure)
        try:
            logging.info("Building Silver: metric_ine_rent")
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.silver.metric_ine_rent AS
                SELECT z.zone_id, TRY_CAST(REPLACE(r.Total, '.', '') AS DOUBLE) AS income_per_capita, CAST(r.Periodo AS INTEGER) AS year, CURRENT_TIMESTAMP AS processed_at
                FROM lakehouse.bronze.ine_rent_municipalities r
                JOIN lakehouse.silver.dim_zones z ON split_part(r.Municipios, ' ', 1) = z.ine_code
                WHERE r."Indicadores de renta media" = 'Renta neta media por persona' AND (r.Distritos IS NULL OR r.Distritos = '')
            """)
        except Exception as e:
            logging.warning(f"⚠️ Rent table failed: {e}")

        # 4. Dim Holidays (Soft Failure)
        try:
            logging.info("Building Silver: dim_zone_holidays")
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.silver.dim_zone_holidays AS
                WITH national_days_2023 AS (
                    SELECT DISTINCT MAKE_DATE(2023, MONTH(strptime("Dia", '%d/%m/%Y')), DAY(strptime("Dia", '%d/%m/%Y'))) AS holiday_date
                    FROM lakehouse.bronze.work_calendars
                    WHERE "Tipo de Festivo" ILIKE '%nacional%'
                )
                SELECT z.zone_id, nd.holiday_date, CURRENT_TIMESTAMP AS processed_at FROM lakehouse.silver.dim_zones z CROSS JOIN national_days_2023 nd;
            """)
        except Exception as e:
            logging.warning(f"⚠️ Holidays table failed: {e}")
            
        con.close()

    @task
    def audit_dimensions():
        """Checks quality of created dimensions."""
        con = get_connection()
        zone_count = con.execute("SELECT COUNT(*) FROM lakehouse.silver.dim_zones").fetchone()[0]
        con.execute(f"INSERT INTO lakehouse.silver.data_quality_log VALUES (CURRENT_TIMESTAMP, 'dim_zones', 'total_zones', {zone_count}, 'Setup Phase')")
        logging.info(f"✅ Dimensions Audited. Zones: {zone_count}")
        con.close()

    # ==============================================================================
    # PART 2: MOBILITY DATA INGESTION (Dynamic / Parallel)
    # ==============================================================================

    @task
    def generate_date_list(**context):
        """Generates list of dates based on DAG params."""
        params = context['params']
        return [d.strftime("%Y%m%d") for d in pd.date_range(start=params['start_date'], end=params['end_date'])]

    @task
    def ensure_fact_tables_exist(**context):
        """Creates Fact table structures to avoid race conditions."""
        con = get_connection()
        date_str = context['params']['start_date']
        url = BASE_URL_TEMPLATE.format(year=date_str[:4], month=date_str[4:6], date=date_str)
        
        # Bronze Structure (Empty)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS lakehouse.bronze.mobility_data AS 
            SELECT *, CURRENT_TIMESTAMP AS ingestion_timestamp, CAST('{url}' AS VARCHAR) AS source_url
            FROM read_csv_auto('{url}', all_varchar=true, ignore_errors=true) LIMIT 0;
        """)
        try: con.execute("ALTER TABLE lakehouse.bronze.mobility_data SET PARTITIONED BY (fecha);")
        except: pass

        # Silver Structure
        con.execute("""
            CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_mobility (
                period TIMESTAMP WITH TIME ZONE, origin_zone_id BIGINT, destination_zone_id BIGINT,
                trips DOUBLE, processed_at TIMESTAMP, partition_date DATE
            );
        """)
        try: con.execute("ALTER TABLE lakehouse.silver.fact_mobility SET PARTITIONED BY (partition_date);")
        except: pass
        
        logging.info("✅ Fact tables initialized.")
        con.close()

    @task(max_active_tis_per_dag=1, map_index_template="{{ task.op_kwargs['date_str'] }}")
    def process_single_day(date_str: str):
        """Atomic Pipeline: Download -> Bronze -> Silver for one day."""
        con = get_connection()
        url = BASE_URL_TEMPLATE.format(year=date_str[:4], month=date_str[4:6], date=date_str)
        
        try:
            # Bronze Load
            con.execute(f"DELETE FROM lakehouse.bronze.mobility_data WHERE fecha = '{date_str}'")
            con.execute(f"INSERT INTO lakehouse.bronze.mobility_data SELECT *, CURRENT_TIMESTAMP, '{url}' FROM read_csv_auto('{url}', filename=false, all_varchar=true, ignore_errors=true);")
            
            # Silver Transform (Uses dim_zones from Part 1)
            con.execute(f"DELETE FROM lakehouse.silver.fact_mobility WHERE partition_date = strptime('{date_str}', '%Y%m%d')")
            con.execute(f"""
                INSERT INTO lakehouse.silver.fact_mobility
                SELECT 
                    (try_strptime(m.fecha, '%Y%m%d') + (CAST(m.periodo AS INTEGER) * INTERVAL 1 HOUR)) AT TIME ZONE 'Europe/Madrid',
                    zo.zone_id, zd.zone_id,
                    TRY_CAST(REPLACE(REPLACE(m.viajes, '.', ''), ',', '.') AS DOUBLE),
                    CURRENT_TIMESTAMP, try_strptime(m.fecha, '%Y%m%d')
                FROM lakehouse.bronze.mobility_data m
                JOIN lakehouse.silver.dim_zones zo ON TRIM(m.origen) = zo.mitma_code
                JOIN lakehouse.silver.dim_zones zd ON TRIM(m.destino) = zd.mitma_code
                WHERE m.fecha = '{date_str}' AND m.viajes IS NOT NULL;
            """)
            logging.info(f"✅ Processed {date_str}")
        except Exception as e:
            logging.error(f"❌ Failed on {date_str}: {e}")
            raise e
        finally:
            con.close()

    @task
    def audit_batch_results(**context):
        """Audits the processed batch of trips."""
        con = get_connection()
        params = context['params']
        stats = con.execute(f"""
            SELECT COUNT(*), SUM(trips), COUNT(DISTINCT partition_date) 
            FROM lakehouse.silver.fact_mobility
            WHERE partition_date BETWEEN strptime('{params['start_date']}', '%Y%m%d') AND strptime('{params['end_date']}', '%Y%m%d')
        """).fetchone()
        
        con.execute(f"INSERT INTO lakehouse.silver.data_quality_log VALUES (CURRENT_TIMESTAMP, 'fact_mobility_batch', 'total_trips', {stats[1] or 0}, 'Batch {params['start_date']}-{params['end_date']}')")
        logging.info(f"📊 Batch Audit: Loaded {stats[2]} days, {stats[1]} total trips.")
        con.close()

    # ==============================================================================
    # ORCHESTRATION FLOW
    # ==============================================================================
    
    # 1. Instantiate Tasks
    # Part 1: Dimensions
    t_init_schemas = create_schemas()
    t_geo = ingest_geo_data()
    t_csvs = ingest_static_csvs()
    t_dims = build_silver_dimensions()
    t_audit_dims = audit_dimensions()

    # Part 2: Mobility Facts
    dates_list = generate_date_list()
    t_init_facts = ensure_fact_tables_exist()
    t_workers = process_single_day.expand(date_str=dates_list)
    t_audit_facts = audit_batch_results()

    # 2. Define Dependencies
    
    # Static Flow: Create Schemas -> Ingest Raw -> Build Silver -> Audit
    t_init_schemas >> t_geo >> t_csvs >> t_dims >> t_audit_dims

    # CRITICAL BRIDGE: Do not start processing facts until Dimensions are ready and audited
    t_audit_dims >> t_init_facts

    # Dynamic Flow: Init Fact Tables -> Parallel Workers -> Final Audit
    t_init_facts >> t_workers >> t_audit_facts

mobility_unified_pipeline()