from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from utils_db import get_connection
import logging
import os
import pandas as pd
from keplergl import KeplerGl 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- CONFIGURATION ---
DEFAULT_POLYGON = "POLYGON((715000 4365000, 735000 4365000, 735000 4385000, 715000 4385000, 715000 4365000))"

@dag(
    dag_id="bq3_functional_classification",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="YYYY-MM-DD"),
        "end_date": Param("20231231", type="string", description="YYYY-MM-DD"),
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string", title="Spatial Filter (WKT)", description="Paste your WKT Polygon here.")
    },
    tags=['mobility', 'gold', 'visualization', 'reporting']
)
def bq3_functional_classification_dag():

    @task
    def classify_zone_functions(**context):
        """
        Task: Functional Classification of Zones
        Filtra por polígono y clasifica según flujos de entrada/salida.
        """
        params = context['params']
        input_wkt = params['polygon_wkt']
        sd_raw = str(params['start_date']) # "20230101"
        ed_raw = str(params['end_date'])   # "20231231"

        # Transformamos a formato YYYY-MM-DD (añadiendo los guiones)
        start_date = f"{sd_raw[:4]}-{sd_raw[4:6]}-{sd_raw[6:]}"
        end_date = f"{ed_raw[:4]}-{ed_raw[4:6]}-{ed_raw[6:]}"
        
        logging.info(f"--- 🏷️ Filtrando por polígono: {input_wkt[:50]}... ---")

        with get_connection() as con:
            # La consulta utiliza un INNER JOIN con las zonas filtradas por el polígono
            # para reducir el volumen de datos de movilidad procesados.
            con.execute(f"""
                CREATE OR REPLACE TABLE lakehouse.gold.zone_functional_classification AS
                WITH 
                -- 1. Identificar zonas dentro del polígono
                selected_zones AS (
                    SELECT zone_id, zone_name
                    FROM lakehouse.silver.dim_zones
                    WHERE ST_Intersects(polygon, ST_GeomFromText('{input_wkt}'))
                ),

                -- 2. Filtrar flujos de movilidad solo para esas zonas
                flat_flows AS (
                    SELECT f.origin_zone_id as zone_id, f.trips as outflow, 0 as inflow, 0 as internal
                    FROM lakehouse.silver.fact_mobility f
                    INNER JOIN selected_zones sz ON f.origin_zone_id = sz.zone_id
                    WHERE f.origin_zone_id != f.destination_zone_id
                    AND f.partition_date >= '{start_date}' 
                    AND f.partition_date <= '{end_date}'
                    
                    UNION ALL
                    
                    SELECT f.destination_zone_id as zone_id, 0 as outflow, f.trips as inflow, 0 as internal
                    FROM lakehouse.silver.fact_mobility f
                    INNER JOIN selected_zones sz ON f.destination_zone_id = sz.zone_id
                    WHERE f.origin_zone_id != f.destination_zone_id
                    AND f.partition_date >= '{start_date}' 
                    AND f.partition_date <= '{end_date}'
                    
                    UNION ALL
                    
                    SELECT f.origin_zone_id as zone_id, 0 as outflow, 0 as inflow, f.trips as internal
                    FROM lakehouse.silver.fact_mobility f
                    INNER JOIN selected_zones sz ON f.origin_zone_id = sz.zone_id
                    WHERE f.origin_zone_id = f.destination_zone_id
                    AND f.partition_date >= '{start_date}' 
                    AND f.partition_date <= '{end_date}'
                ),

                -- 3. Agregación de estadísticas por zona
                zone_flow_stats AS (
                    SELECT 
                        zone_id,
                        SUM(internal) as internal_trips,
                        SUM(outflow) as outflow,
                        SUM(inflow) as inflow
                    FROM flat_flows
                    GROUP BY 1
                )

                -- 4. Cálculo de métricas y etiquetas finales
                SELECT 
                    m.zone_id,
                    sz.zone_name,
                    m.internal_trips,
                    m.outflow,
                    m.inflow,
                    
                    -- Net Flow Ratio: (In - Out) / (In + Out)
                    CASE 
                        WHEN (m.inflow + m.outflow) = 0 THEN 0 
                        ELSE (1.0 * m.inflow - m.outflow) / (m.inflow + m.outflow) 
                    END AS net_flow_ratio,

                    -- Retention Rate: Internal / (Out + Internal)
                    CASE 
                        WHEN (m.outflow + m.internal_trips) = 0 THEN 0
                        ELSE (1.0 * m.internal_trips) / (m.outflow + m.internal_trips)
                    END AS retention_rate, 

                    -- Clasificación Final basada en los ratios calculados
                    CASE 
                        WHEN ((1.0 * m.internal_trips) / NULLIF(m.outflow + m.internal_trips, 0)) > 0.20 
                            THEN 'Self-Sustaining Cell'
                        
                        -- 2. Importadores netos (Más gente entra de la que sale)
                        WHEN ((1.0 * m.inflow - m.outflow) / NULLIF(m.inflow + m.outflow, 0)) > 0.001 
                            THEN 'Activity Hub (Importer)'
                        
                        -- 3. Exportadores netos (Más gente sale de la que entra)
                        WHEN ((1.0 * m.inflow - m.outflow) / NULLIF(m.inflow + m.outflow, 0)) < -0.001 
                            THEN 'Bedroom Community (Exporter)'
                        
                        ELSE 'Balanced / Transit Zone'
                    END AS functional_label
                        
                FROM zone_flow_stats m
                JOIN selected_zones sz ON m.zone_id = sz.zone_id
                WHERE (m.internal_trips + m.inflow + m.outflow) > 0;
            """)

        logging.info("✅ Gold: Tabla de clasificación funcional actualizada correctamente.")

    @task
    def generate_kepler_map_bq3(**context):
        """
        Genera un mapa interactivo en Kepler.gl para la Clasificación Funcional (BQ3).
        """
        params = context['params']
        run_id = context['run_id']
        
        logging.info("--- 🗺️ Extrayendo datos para Mapa Kepler BQ3 ---")

        with get_connection() as con:
            # Seguimos tu estructura: CTE para filtrar por polígono y transformación de coordenadas
            df = con.execute(f"""
                SELECT 
                    f.zone_id,
                    f.zone_name,
                    f.functional_label,
                    f.net_flow_ratio,
                    f.retention_rate,
                    f.internal_trips + f.inflow + f.outflow as total_activity,
                    -- Transformación de coordenadas para Kepler (centros de los municipios)
                    ST_X(ST_Transform(m.centroid, 'EPSG:25830', 'OGC:CRS84')) as lon,
                    ST_Y(ST_Transform(m.centroid, 'EPSG:25830', 'OGC:CRS84')) as lat,
                    ST_AsText(m.polygon) as wkt_polygon
                FROM lakehouse.gold.zone_functional_classification f
                JOIN lakehouse.silver.dim_zones m ON f.zone_id = m.zone_id
            """).df()

        if df.empty:
            logging.warning("No data found for the given polygon in BQ3.")
            return

        # --- LIMPIEZA Y TIPADO (Siguiendo tu estructura) ---
        df['net_flow_ratio'] = pd.to_numeric(df['net_flow_ratio'], errors='coerce').fillna(0.0)
        df['retention_rate'] = pd.to_numeric(df['retention_rate'], errors='coerce').fillna(0.0)
        df['total_activity'] = pd.to_numeric(df['total_activity'], errors='coerce').fillna(0).astype(int)

        # Kepler falla si hay NaNs en lat/lon
        df = df.dropna(subset=['lat', 'lon'])

        df["size_activity"] = (df["total_activity"].clip(lower=50).pow(0.5))   # mínimo semántico


        # --- CONFIGURACIÓN KEPLER (Cambiamos Arcos por Polígonos/Puntos) ---
        dataset_name = "Functional Classification"
        kepler_config = {
            "version": "v1",
            "config": {
                "visState": {
                    "layers": [
                        {
                            "id": "label_layer",
                            "type": "point",
                            "config": {
                                "dataId": dataset_name, # Debe coincidir con el nombre en data={}
                                "label": "Municipios por Función",
                                "isVisible": True,
                                "columns": {"lat": "lat", "lng": "lon"},
                                "visConfig": {
                                    "radius": 80,
                                    "fixedRadius": False,
                                    "opacity": 0.8,
                                    "colorRange": {
                                        "name": "Custom Scale",
                                        "type": "ordinal",
                                        "colors": ["#1E90FF", "#FF4500", "#32CD32", "#C8C8C8"]
                                    },
                                    "colorScale": "ordinal"
                                }
                                },
                                "visualChannels": {
                                    # Importante: Asegúrate de que estos nombres existan en el DF
                                    "colorField": {"name": "functional_label", "type": "string"},
                                    "colorScale": "ordinal",
                                    "sizeField": {
                                        "name": "size_activity",
                                        "type": "integer"
                                    },
                                    "sizeScale": "sqrt"
                            }
                        }
                    ]
                },
                "mapState": {
                    # Ajustado a Valencia según tus logs
                    "latitude": 39.45,
                    "longitude": -0.47,
                    "zoom": 10,
                    "pitch": 0,
                    "bearing": 0
                }
            }
        }

        # --- GENERACIÓN Y SUBIDA A S3 (Estructura BQ3) ---
        map_bq3 = KeplerGl(height=800, data={dataset_name: df}, config=kepler_config)
        
        local_path = f"/tmp/kepler_bq3_{run_id}.html"
        map_bq3.save_to_html(file_name=local_path)

        # Usamos el path resultados/bq3/ como pediste
        s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
        s3_hook.load_file(
            filename=local_path,
            key=f"results/bq3/kepler_functional_{run_id}.html",
            bucket_name="ducklake-bdproject",
            replace=True
        )
        
        os.remove(local_path)
        logging.info(f"✅ Mapa BQ3 subido a S3 en results/bq3/kepler_functional_{run_id}.html")

    # --- FLUJO DEL DAG ---
    classification_task = classify_zone_functions()
    map_task = generate_kepler_map_bq3()

    classification_task >> map_task

bq3_functional_classification_dag()