from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from utils_db import get_connection
import logging
import os
import pandas as pd
import json
from keplergl import KeplerGl 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook

# --- CONFIGURATION ---
DEFAULT_POLYGON = "POLYGON((715000 4365000, 735000 4365000, 735000 4385000, 715000 4385000, 715000 4365000))"
aws = BaseHook.get_connection("aws_s3_conn")
S3_BUCKET = aws.extra_dejson.get('bucket_name', 'ducklake-dbproject')

@dag(
    dag_id="33_bq3_functional_classification",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="YYYY-MM-DD"),
        "end_date": Param("20231231", type="string", description="YYYY-MM-DD"),
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string", title="Spatial Filter (WKT)", description="Paste your WKT Polygon here."),
        "input_crs": Param(
            "EPSG:25830", 
            enum=["EPSG:25830", "OGC:CRS84", "EPSG:4326"], 
            title="Coordinate Reference System (CRS)",
            description="25830 (Meters), CRS84 (Lon/Lat), 4326 (Lat/Lon)"
        )
    },
    tags=['mobility', 'gold', 'visualization', 'reporting']
)
def bq3_functional_classification():

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
        input_crs = params['input_crs']

        # Transformamos a formato YYYY-MM-DD (añadiendo los guiones)
        start_date = f"{sd_raw[:4]}-{sd_raw[4:6]}-{sd_raw[6:]}"
        end_date = f"{ed_raw[:4]}-{ed_raw[4:6]}-{ed_raw[6:]}"

        # Construcción dinámica de la geometría de filtrado
        if input_crs == "EPSG:25830":
            # Caso 1: Ya está en el sistema de la capa Silver (metros)
            filter_geom = f"ST_GeomFromText('{input_wkt}')"
            
        elif input_crs == "OGC:CRS84":
            # Caso 2: Estándar GIS (Longitud, Latitud)
            filter_geom = f"ST_Transform(ST_GeomFromText('{input_wkt}'), 'OGC:CRS84', 'EPSG:25830')"
            
        elif input_crs == "EPSG:4326":
            # Caso 3: Estándar Geodésico (Latitud, Longitud)
            filter_geom = f"ST_Transform(ST_GeomFromText('{input_wkt}'), 'EPSG:4326', 'EPSG:25830')"
        
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
                    WHERE ST_Intersects(polygon, {filter_geom})
                ),

                -- 2. Filtrar flujos de movilidad solo para esas zonas
                flat_flows AS (
                    SELECT f.origin_zone_id as zone_id, f.trips as outflow, 0 as inflow, 0 as internal
                    FROM lakehouse.silver.fact_mobility f
                    INNER JOIN selected_zones sz ON f.origin_zone_id = sz.zone_id
                    WHERE f.origin_zone_id != f.destination_zone_id
                        AND f.partition_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
                    
                    UNION ALL
                    
                    SELECT f.destination_zone_id as zone_id, 0 as outflow, f.trips as inflow, 0 as internal
                    FROM lakehouse.silver.fact_mobility f
                    INNER JOIN selected_zones sz ON f.destination_zone_id = sz.zone_id
                    WHERE f.origin_zone_id != f.destination_zone_id
                        AND f.partition_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
                    
                    UNION ALL
                    
                    SELECT f.origin_zone_id as zone_id, 0 as outflow, 0 as inflow, f.trips as internal
                    FROM lakehouse.silver.fact_mobility f
                    INNER JOIN selected_zones sz ON f.origin_zone_id = sz.zone_id
                    WHERE f.origin_zone_id = f.destination_zone_id
                        AND f.partition_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
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
                    ST_AsGeoJSON(
                        ST_Transform(m.polygon, 'EPSG:25830', 'OGC:CRS84')
                    ) AS geometry
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

        # Eliminamos filas si no hay datos esenciales
        df = df.dropna(subset=['functional_label', 'total_activity', 'lat', 'lon', 'geometry'])
        df["geometry"] = df["geometry"].apply(json.loads)

        # Limpiamos espacios y valores nulos
        df['functional_label'] = df['functional_label'].astype(str).str.strip().fillna("Unknown")

        # Filtramos solo los valores válidos
        valid_labels = [
            "Self-Sustaining Cell",
            "Activity Hub (Importer)",
            "Bedroom Community (Exporter)",
            "Balanced / Transit Zone"
        ]
        df = df[df['functional_label'].isin(valid_labels)]

        # --- CONFIGURACIÓN KEPLER ---
        dataset_name = "Functional Classification"
        kepler_config = {
            "version": "v1",
            "config": {
                "visState": {
                    "layers": [
                        {
                            "id": "activity_points_layer",
                            "type": "point",
                            "config": {
                                "dataId": dataset_name,
                                "label": "Intensidad de Actividad (Centroide)",
                                "isVisible": True,
                                "columns": {"lat": "lat", "lng": "lon"},
                                "visConfig": {
                                    "radius": 10,
                                    "fixedRadius": False,
                                    "opacity": 0.9,
                                    "outline": True,
                                    "thickness": 1,
                                    "strokeColor": [255, 255, 255],
                                    "color": [0, 0, 0] # COLOR NEGRO
                                }
                            },
                             "visualChannels": {
                                    "sizeField": {"name": "total_activity", "type": "integer"},
                                    "sizeScale": "sqrt"
                                }
                        },
                        {
                            "id": "polygon_layer",
                            "type": "geojson",
                            "config": {
                                "dataId": dataset_name,
                                "label": "Zonas por Clasificación",
                                "isVisible": True,
                                "columns": {"geojson": "geometry"},
                                "visConfig": {
                                    "opacity": 0.5,
                                    "strokeColor": [255, 255, 255],
                                    "filled": True, 
                                    "thickness": 0.5,
                                    "colorRange": {
                                        "name": "Custom Scale",
                                        "type": "ordinal",
                                        "colors": ["#1E90FF", "#FF4500", "#32CD32", "#C8C8C8"]
                                    }
                                }
                            },
                            "visualChannels": {
                                    "colorField": {"name": "functional_label", "type": "string"},
                                    "colorScale": "ordinal"
                            },
                        }
                        
                    ],
                    "interactionConfig": {
                        "tooltip": {
                            "fieldsToShow": {
                                dataset_name: [
                                    {"name": "zone_name", "format": None},
                                    {"name": "functional_label", "format": None},
                                    {"name": "total_activity", "format": None}
                                ]
                            },
                            "enabled": True
                        }
                    }
                },
                "mapState": {
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
            bucket_name=S3_BUCKET,
            replace=True
        )
        
        os.remove(local_path)
        logging.info(f"✅ Mapa BQ3 subido a S3 en results/bq3/kepler_functional_{run_id}.html")

    # --- FLUJO DEL DAG ---
    classification_task = classify_zone_functions()
    map_task = generate_kepler_map_bq3()

    classification_task >> map_task

bq3_functional_classification()
