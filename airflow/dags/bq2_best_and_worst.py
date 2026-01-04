from duckdb import df
from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
import logging
import os
from keplergl import KeplerGl 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Polígono en metros (EPSG:25830)
DEFAULT_POLYGON = "POLYGON((715000 4365000, 735000 4365000, 735000 4385000, 715000 4385000, 715000 4365000))"
# DEFAULT_POLYGON = "POLYGON((-0.55 39.35, -0.25 39.35, -0.25 39.65, -0.55 39.65, -0.55 39.35))"

PATH_PROYECTO = "/usr/local/airflow/include/ranking_servicio.html"

@dag(
    dag_id="bq2_zones_kepler_ranking",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string", title="Spatial Filter (WKT)"),
        "input_crs": Param(
            "EPSG:25830", 
            enum=["EPSG:25830", "OGC:CRS84", "EPSG:4326"], 
            title="Coordinate Reference System (CRS)",
            description="25830 (Meters), CRS84 (Lon/Lat), 4326 (Lat/Lon)"
        )
    },
    tags=['mobility', 'visualization', 'kepler']
)
def mobility_gaps_bestworstzones():
    @task
    def generate_and_upload_map_zones(**context):

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

        # 5. Guardado Local y Subida a S3
        # local_path = f"/tmp/kepler_zones_{run_id}.html"
        # map_1.save_to_html(file_name=local_path)

        # s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
        # s3_hook.load_file(
        #     filename=local_path,
        #     key=f"results/bq2/ranking_service_{run_id}.html",
        #     bucket_name="ducklake-bdproject",
        #     replace=True
        # )
        
        # os.remove(local_path)
        # logging.info("Mapa de Ranking Zonal subido exitosamente a S3.")

        map_1.save_to_html(file_name=PATH_PROYECTO)
        
        logging.info(f"✅ MAPA GUARDADO EN: {PATH_PROYECTO}")

    generate_and_upload_map_zones()

mobility_gaps_bestworstzones()