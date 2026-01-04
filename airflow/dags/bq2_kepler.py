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

@dag(
    dag_id="bq2_consultation_kepler",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string", title="Spatial Filter (WKT)")
    },
    tags=['mobility', 'visualization', 'kepler']
)
def mobility_06_gaps_kepler():

    @task
    def generate_and_upload_map(**context):
        params = context['params']
        run_id = context['run_id']
        polygon_wkt = params['polygon_wkt']
        
        # Transformamos el polígono a long y lat para poder usar Kepler.gl
        con = get_connection()
        df = con.execute(f"""
            WITH municipios_in_polygon AS (
                SELECT zone_id, centroid, polygon
                FROM lakehouse.silver.dim_zones
                WHERE ST_Contains(ST_GeomFromText('{polygon_wkt}'), polygon)
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
                                    "sizeRange": [0.9, 0.9]
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

        # Guardar y Subir
        local_path = f"/tmp/kepler_map_{run_id}.html"
        
        # Guardamos el HTML
        map_1.save_to_html(file_name=local_path)

        s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
        s3_hook.load_file(
            filename=local_path,
            key=f"results/bq2/kepler_mobility_{run_id}.html",
            bucket_name="ducklake-bdproject",
            replace=True
        )
        
        os.remove(local_path)
        logging.info("Mapa Kepler subido exitosamente a S3.")

    generate_and_upload_map()

mobility_06_gaps_kepler()