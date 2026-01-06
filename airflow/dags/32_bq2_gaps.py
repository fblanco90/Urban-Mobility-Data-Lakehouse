import os
from airflow.sdk import dag, task, Param
from pendulum import datetime
import pandas as pd
import logging
from keplergl import KeplerGl
from utils_db import get_connection, run_batch_sql



DEFAULT_POLYGON = "POLYGON((715000 4365000, 735000 4365000, 735000 4385000, 715000 4385000, 715000 4365000))"
OUTPUT_FOLDER = "include/results/bq2"


@dag(
    dag_id="32_bq2_gaps",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="YYYYMMDD"),
        "end_date": Param("20230101", type="string", description="YYYYMMDD"),
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string", title="Spatial Filter (WKT)"),
        "input_crs": Param(
            "EPSG:25830", 
            enum=["EPSG:25830", "OGC:CRS84", "EPSG:4326"], 
            title="Coordinate Reference System (CRS)",
            description="25830 (Meters), CRS84 (Lon/Lat), 4326 (Lat/Lon)"
        )
    },
    tags=['mobility', 'gold', 'analytics', 'aws_batch']
)
def gold_analytics():

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    sql_gaps = """
        CREATE OR REPLACE TABLE gold.infrastructure_gaps AS
        WITH trips_aggregated AS (
            SELECT origin_zone_id AS o, destination_zone_id AS d, SUM(trips) AS total_trips
            FROM silver.fact_mobility
            WHERE partition_date BETWEEN strptime('{{ params.start_date }}', '%Y%m%d')::DATE 
                                    AND strptime('{{ params.end_date }}', '%Y%m%d')::DATE
            GROUP BY 1, 2
        ),
        potential_calc AS (
            SELECT
                t.o AS org_zone_id, 
                t.d AS dest_zone_id, 
                t.total_trips, 
                m1.population AS total_population,
                m2.income_per_capita AS rent,
                dist.dist_km,
                (m1.population * m2.income_per_capita) / (dist.dist_km * dist.dist_km) AS gravity_score
            FROM trips_aggregated t
            JOIN silver.dim_zone_distances dist ON t.o = dist.origin_zone_id AND t.d = dist.destination_zone_id
            JOIN silver.metric_population m1 ON t.o = m1.zone_id
            JOIN silver.metric_ine_rent m2 ON t.d = m2.zone_id
            WHERE m2.year = 2023
        ),
        norm AS (
            SELECT SUM(total_trips) / NULLIF(SUM(gravity_score), 0) as ratio 
            FROM potential_calc
        )
        SELECT
            org_zone_id, 
            dest_zone_id, 
            total_population, 
            rent,
            total_trips, 
            dist_km,
            total_trips / NULLIF(gravity_score * (SELECT ratio FROM norm), 0) AS mismatch_ratio
        FROM potential_calc;
        """

    task_batch_gaps = run_batch_sql(
        task_id="batch_infrastructure_gaps", 
        sql_query=sql_gaps, 
        memory="16GB"
    )

    @task
    def ranking_service(**context):

        params = context['params']
        run_id = context['run_id']
        polygon_wkt = params['polygon_wkt']
        input_crs = params['input_crs']

        # Construcción dinámica de la geometría de filtrado
        if input_crs == "EPSG:25830":
            # Caso 1: Ya está en el sistema de la capa Silver (metros)
            filter_geom = f"ST_GeomFromText('{polygon_wkt}')"
            
        elif input_crs == "OGC:CRS84":
            # Caso 2: Estándar GIS (Longitud, Latitud)
            filter_geom = f"ST_Transform(ST_GeomFromText('{polygon_wkt}'), 'OGC:CRS84', 'EPSG:25830')"
            
        elif input_crs == "EPSG:4326":
            # Caso 3: Estándar Geodésico (Latitud, Longitud)
            filter_geom = f"ST_Transform(ST_GeomFromText('{polygon_wkt}'), 'EPSG:4326', 'EPSG:25830')"

        con = get_connection()

        df = con.execute(f"""
            WITH municipios_in_polygon AS (
                SELECT zone_id, zone_name, centroid, polygon
                FROM lakehouse.silver.dim_zones
                WHERE ST_Intersects({filter_geom}, polygon)
            )
            SELECT 
                zo.zone_name,
                -- Promedio del ratio ponderado por la cantidad de viajes
                SUM(g.mismatch_ratio * g.total_trips) / NULLIF(SUM(g.total_trips), 0) as avg_service_level,
                -- Importancia de la zona (Población * Renta)
                MAX(g.total_population * g.rent) as zone_importance,
                ST_X(ST_Transform(zo.centroid, 'EPSG:25830', 'OGC:CRS84')) as lon,
                ST_Y(ST_Transform(zo.centroid, 'EPSG:25830', 'OGC:CRS84')) as lat
            FROM lakehouse.gold.infrastructure_gaps g
            JOIN municipios_in_polygon zo ON g.org_zone_id = zo.zone_id
            GROUP BY 1, 4, 5
            """).df()
        
        con.close()

        if df.empty:
            logging.warning("No data found for the given polygon.")
            return
        
        df = df.dropna(subset=["avg_service_level", "zone_importance"])
                
        # DEBUG: Mira esto en los logs de Airflow
        logging.info(f"DATOS PARA EL MAPA:\n{df[['zone_name', 'avg_service_level', 'zone_importance']].head(10)}")

        # Aseguramos que las columnas sean números reales
        df["avg_service_level"] = pd.to_numeric(df["avg_service_level"], errors="coerce")
        df["zone_importance"] = pd.to_numeric(df["zone_importance"], errors="coerce")

        # 3. Configuración de Kepler.gl para Puntos (Burbujas)
        kepler_config = {
            "version": "v1",
            "config": {
                "visState": {
                    "layers": [{
                        "id": "zone_ranking_layer",
                        "type": "point",
                        "config": {
                            "dataId": "datos_ranking",
                            "label": "Nivel de Servicio por Zona",
                            "isVisible": True,
                            "columns": {
                                "lat": "lat",
                                "lng": "lon"
                            },
                            "visConfig": {
                                "radius": 20,
                                "fixedRadius": False,
                                "opacity": 0.8,
                                "radiusRange": [5,120],
                                "colorRange": {
                                    "name": "Custom RdYlGn",
                                    "type": "diverging",
                                    "colors": [
                                        "#d73027", "#f46d43", "#fdae61",
                                        "#fee08b", "#ffffbf",
                                        "#d9ef8b", "#a6d96a",
                                        "#66bd63", "#1a9850"
                                    ]
                                }
                            }
                        },
                        "visualChannels": { 
                            "colorField": {
                                "name": "avg_service_level",
                                "type": "real"
                            },
                            "colorScale": "quantile",
                            "radiusField": {
                                "name": "zone_importance",
                                "type": "real"
                            },
                            "radiusScale": "sqrt"
                        }
                    }]
                },
                "mapState": {
                    "latitude": 40.4168,
                    "longitude": -3.7038,
                    "zoom": 6
                }
            }
        }

        # 4. Creación del objeto Mapa
        map_1 = KeplerGl(height=800, data={"datos_ranking": df}, config=kepler_config)

        file_path = os.path.join(OUTPUT_FOLDER, "ranking_service_map.html")
        map_1.save_to_html(file_name=file_path)
        logging.info(f"✅ Ranking HTML saved to: {file_path}")
    
    @task
    def kepler_mobility(**context):
        params = context['params']
        run_id = context['run_id']
        polygon_wkt = params['polygon_wkt']
        input_crs = params['input_crs']

        # Construcción dinámica de la geometría de filtrado
        if input_crs == "EPSG:25830":
            # Caso 1: Ya está en el sistema de la capa Silver (metros)
            filter_geom = f"ST_GeomFromText('{polygon_wkt}')"
            
        elif input_crs == "OGC:CRS84":
            # Caso 2: Estándar GIS (Longitud, Latitud)
            filter_geom = f"ST_Transform(ST_GeomFromText('{polygon_wkt}'), 'OGC:CRS84', 'EPSG:25830')"
            
        elif input_crs == "EPSG:4326":
            # Caso 3: Estándar Geodésico (Latitud, Longitud)
            filter_geom = f"ST_Transform(ST_GeomFromText('{polygon_wkt}'), 'EPSG:4326', 'EPSG:25830')"
        
        # Transformamos el polígono a long y lat para poder usar Kepler.gl
        con = get_connection()

        df = con.execute(f"""
            WITH municipios_in_polygon AS (
                SELECT zone_id, centroid, polygon
                FROM lakehouse.silver.dim_zones
                WHERE ST_Contains({filter_geom}, polygon)
            )
            SELECT 
                g.total_trips as actual_trips, 
                g.mismatch_ratio,
                ST_X(ST_Transform(zo.centroid, 'EPSG:25830', 'OGC:CRS84')) as lon_origen,
                ST_Y(ST_Transform(zo.centroid, 'EPSG:25830', 'OGC:CRS84')) as lat_origen,
                ST_X(ST_Transform(zd.centroid, 'EPSG:25830', 'OGC:CRS84')) as lon_destino,
                ST_Y(ST_Transform(zd.centroid, 'EPSG:25830', 'OGC:CRS84')) as lat_destino
            FROM lakehouse.gold.infrastructure_gaps g
            JOIN municipios_in_polygon zo ON g.org_zone_id = zo.zone_id
            JOIN municipios_in_polygon zd ON g.dest_zone_id = zd.zone_id
            WHERE g.total_trips > 10;
            """).df()
        
        con.close()


        if df.empty:
            logging.warning("No data found for the given polygon.")
            return

        # Configuraciones para Kepler.gl
        # Aseguramos tipos numéricos
        df['actual_trips'] = pd.to_numeric(df['actual_trips'], errors='coerce').fillna(0).astype(int)
        df['mismatch_ratio'] = pd.to_numeric(df['mismatch_ratio'], errors='coerce').fillna(0.0)

        # Limpieza estricta de datos
        df = df.dropna(subset=['lat_origen', 'lon_origen', 'lat_destino', 'lon_destino'])
        # Limitamos a los 3000 registros con más viajes reales para evitar sobrecargar Kepler.gl
        df = df.sort_values('actual_trips', ascending=False).head(3000)

        # Configuración del mapa Kepler.gl
        kepler_config = {
            "version": "v1",
            "config": {
                "visState": {
                    "layers": [
                        {
                            "id": "arc_layer_mobility", 
                            "type": "arc",
                            "config": {
                                "dataId": "Mobility Gaps", 
                                "label": "Viajes Interurbanos",
                                "isVisible": True,
                                "columns": {
                                    "lat0": "lat_origen",
                                    "lng0": "lon_origen",
                                    "lat1": "lat_destino",
                                    "lng1": "lon_destino"
                                },
                                "visConfig": {
                                    "opacity": 0.9,
                                    "thickness": 0.9,
                                    "colorRange": {
                                        "name": "Global Warming",
                                        "type": "sequential",
                                        "category": "Uber",
                                        "colors": ["#5A1846", "#900C3F", "#C70039", "#E3611C", "#F1920E", "#FFC300"]
                                    },
                                    "sizeRange": [0.8, 0.8]
                                }
                            },
                            "visualChannels": {
                                "colorField": {
                                    "name": "mismatch_ratio",
                                    "type": "real"
                                },
                                "colorScale": "quantile",
                                "sizeField": {
                                    "name": "actual_trips",
                                    "type": "integer"
                                },
                                "sizeScale": "log"
                            }
                        }
                    ]
                },
                "mapState": {
                    "latitude": 40.4168,
                    "longitude": -3.7038,
                    "zoom": 6
                }
            }
        }

        dataset_name = "Mobility Gaps" 

        
        map_1 = KeplerGl(
            height=800, 
            data={dataset_name: df}, 
            config=kepler_config  
        )

        file_path = os.path.join(OUTPUT_FOLDER, "mobility_gaps_map.html")
        map_1.save_to_html(file_name=file_path)
        logging.info(f"✅ Mobility Gaps HTML saved to: {file_path}")
    
    @task
    def generate_report_markdown(**context):
        params = context['params']
        sd_raw = params['start_date']
        ed_raw = params['end_date']
        start_readable = f"{sd_raw[:4]}-{sd_raw[4:6]}-{sd_raw[6:]}"
        end_readable = f"{ed_raw[:4]}-{ed_raw[4:6]}-{ed_raw[6:]}"
        
        md_path = os.path.join(OUTPUT_FOLDER, f"report_BQ2_{sd_raw}.md")
        
        markdown_content = f"""# Business Question 2: Infrastructure Gaps & Mobility Potential
## 1. Execution Summary
This analysis identifies areas where current mobility flows do not match the expected potential based on population and income (Gravity Model).

* **Period:** {start_readable} to {end_readable}
* **Spatial Filter:** `{params['polygon_wkt']}`

---

## 2. Interactive Maps
Detailed spatial analysis using Kepler.gl:

### A. Zone Service Level Ranking
Visualizes "Mismatch Ratio" vs "Zone Importance". 
*   **Red zones:** Service level below potential (Infrastructure Gap).
*   **Green zones:** Service level meets or exceeds potential.
*   **Bubble Size:** Economic importance (Population * Rent).

👉 [**Open Ranking Map (HTML)**](./ranking_service_map.html)

### B. Inter-urban Mobility Gaps (Arc Map)
Visualizes the OD flows between zones.
*   **Arc Color:** Mismatch ratio (Red = Under-served flow).
*   **Arc Thickness:** Volume of trips.

👉 [**Open Mobility Gaps Map (HTML)**](./mobility_gaps_map.html)

---
## 3. Methodology
1. **Gravity Model:** Calculated potential flow based on $P_o \cdot I_d / dist^2$.
2. **Mismatch Ratio:** Observed Trips / Potential Trips.
3. **Filtering:** Focused on significant flows (>10 trips) within the provided polygon.
"""
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(markdown_content)
        logging.info(f"✅ Markdown report generated: {md_path}")

    # Orchestration
    task_batch_gaps >> [ranking_service(), kepler_mobility()] >> generate_report_markdown()

gold_analytics()