from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from utils_db import get_connection, run_batch_sql
import logging
import os
import pandas as pd
import json
from keplergl import KeplerGl

# --- CONFIGURATION ---
DEFAULT_POLYGON = "POLYGON((715000 4365000, 735000 4365000, 735000 4385000, 715000 4385000, 715000 4365000))"
OUTPUT_FOLDER = "include/results/bq3"

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

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    sql_classification = """
    {% set input_crs = params.input_crs %}
    {% set polygon_wkt = params.polygon_wkt %}
    
    {% if input_crs == 'EPSG:25830' %}
        {% set filter_geom = "ST_GeomFromText('" ~ polygon_wkt ~ "')" %}
    {% elif input_crs == 'OGC:CRS84' %}
        {% set filter_geom = "ST_Transform(ST_GeomFromText('" ~ polygon_wkt ~ "'), 'OGC:CRS84', 'EPSG:25830')" %}
    {% elif input_crs == 'EPSG:4326' %}
        {% set filter_geom = "ST_Transform(ST_GeomFromText('" ~ polygon_wkt ~ "'), 'EPSG:4326', 'EPSG:25830')" %}
    {% else %}
        {% set filter_geom = "ST_GeomFromText('" ~ polygon_wkt ~ "')" %}
    {% endif %}    


    CREATE OR REPLACE TABLE gold.zone_functional_classification AS
    WITH selected_zones AS (
        SELECT zone_id, zone_name, centroid, polygon
        FROM silver.dim_zones
        WHERE ST_Intersects({{ filter_geom }}, polygon)
    ),
    flat_flows AS (
        SELECT f.origin_zone_id as zone_id, f.trips as outflow, 0 as inflow, 0 as internal
        FROM silver.fact_mobility f
        INNER JOIN selected_zones sz ON f.origin_zone_id = sz.zone_id
        WHERE f.origin_zone_id != f.destination_zone_id
            AND f.partition_date BETWEEN strptime('{{ params.start_date }}', '%Y%m%d')::DATE 
                                 AND strptime('{{ params.end_date }}', '%Y%m%d')::DATE
        
        UNION ALL
        
        SELECT f.destination_zone_id as zone_id, 0 as outflow, f.trips as inflow, 0 as internal
        FROM silver.fact_mobility f
        INNER JOIN selected_zones sz ON f.destination_zone_id = sz.zone_id
        WHERE f.origin_zone_id != f.destination_zone_id
            AND f.partition_date BETWEEN strptime('{{ params.start_date }}', '%Y%m%d')::DATE 
                                 AND strptime('{{ params.end_date }}', '%Y%m%d')::DATE
        
        UNION ALL
        
        SELECT f.origin_zone_id as zone_id, 0 as outflow, 0 as inflow, f.trips as internal
        FROM silver.fact_mobility f
        INNER JOIN selected_zones sz ON f.origin_zone_id = sz.zone_id
        WHERE f.origin_zone_id = f.destination_zone_id
            AND f.partition_date BETWEEN strptime('{{ params.start_date }}', '%Y%m%d')::DATE 
                                 AND strptime('{{ params.end_date }}', '%Y%m%d')::DATE
    ),
    zone_flow_stats AS (
        SELECT zone_id, SUM(internal) as internal_trips, SUM(outflow) as outflow, SUM(inflow) as inflow
        FROM flat_flows
        GROUP BY 1
    )
    SELECT 
        m.zone_id, sz.zone_name, m.internal_trips, m.outflow, m.inflow,
        CASE WHEN (m.inflow + m.outflow) = 0 THEN 0 ELSE (1.0 * m.inflow - m.outflow) / (m.inflow + m.outflow) END AS net_flow_ratio,
        CASE WHEN (m.outflow + m.internal_trips) = 0 THEN 0 ELSE (1.0 * m.internal_trips) / (m.outflow + m.internal_trips) END AS retention_rate, 
        CASE 
            WHEN (1.0 * m.internal_trips / NULLIF(m.outflow + m.internal_trips, 0)) > 0.20 THEN 'Self-Sustaining Cell'
            WHEN ((1.0 * m.inflow - m.outflow) / NULLIF(m.inflow + m.outflow, 0)) > 0.001 THEN 'Activity Hub (Importer)'
            WHEN ((1.0 * m.inflow - m.outflow) / NULLIF(m.inflow + m.outflow, 0)) < -0.001 THEN 'Bedroom Community (Exporter)'
            ELSE 'Balanced / Transit Zone'
        END AS functional_label
    FROM zone_flow_stats m
    JOIN selected_zones sz ON m.zone_id = sz.zone_id
    WHERE (m.internal_trips + m.inflow + m.outflow) > 0;
    """

    task_batch_classification = run_batch_sql(
        task_id="batch_classify_zones",
        sql_query=sql_classification,
        memory="12GB"
    )

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
        
        file_path = os.path.join(OUTPUT_FOLDER, "functional_classification_map.html")
        map_bq3.save_to_html(file_name=file_path)
        logging.info(f"✅ Kepler HTML saved to: {file_path}")

    @task
    def generate_report_markdown(**context):
        params = context['params']
        sd_raw = params['start_date']
        ed_raw = params['end_date']
        start_readable = f"{sd_raw[:4]}-{sd_raw[4:6]}-{sd_raw[6:]}"
        end_readable = f"{ed_raw[:4]}-{ed_raw[4:6]}-{ed_raw[6:]}"
        md_path = os.path.join(OUTPUT_FOLDER, f"report_BQ3_{sd_raw}.md")
        
        markdown_content = f"""---
## 1. Parameters
This analysis classifies zones based on their net mobility flows (Importers vs Exporters) and their retention capacity.

* **Analysis Period:** {start_readable} to {end_readable}
* **Spatial Filter:** `{params['polygon_wkt']}`

---

## 2. Functional Landscape
We categorize zones into four functional types:
1. **Self-Sustaining Cells:** High internal retention (>20% of trips stay within).
2. **Activity Hubs (Importers):** Significant net inflow (Work/Commercial areas).
3. **Bedroom Communities (Exporters):** Significant net outflow (Residential areas).
4. **Balanced / Transit Zones:** Mixed use or transit corridors.

### Interactive Spatial Analysis
The following map shows the geographical distribution of these roles, where the black bubbles represent total activity (In + Out + Internal).


---
## 3. Technical Specs
* **Metrics:** Net Flow Ratio (In-Out Balance) and Retention Rate (Self-containment).
* **Processing:** Batch SQL using DuckDB.
* **Visualization:** Kepler.gl GeoJSON + Point layers.
"""
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(markdown_content)
        logging.info(f"✅ Markdown report generated at: {md_path}")


    # --- FLUJO DEL DAG ---

    map_task = generate_kepler_map_bq3()

    task_batch_classification >> map_task >> generate_report_markdown()

bq3_functional_classification()
