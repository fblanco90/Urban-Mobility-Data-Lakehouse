from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.sdk.bases.hook import BaseHook
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
import logging
import os
import folium
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from pyproj import Transformer
import math

# Default spatial filter covering mainland Spain in EPSG:25830 (UTM 30N) meters
DEFAULT_POLYGON = "POLYGON((-150000 3900000, -150000 4900000, 1100000 4900000, 1100000 3900000, -150000 3900000))"

@dag(
    dag_id="bq2_consultation_fixed",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string", title="Spatial Filter (WKT)")
    },
    tags=['mobility', 'visualization']
)
def mobility_06_gaps_map():
    """
    DAG to visualize mobility gaps between zones. 
    It fetches origin-destination flows, transforms coordinates, and generates a Folium map.
    """
    @task
    def generate_and_upload_map(**context):
        params = context['params']
        run_id = context['run_id']
        polygon_wkt = params['polygon_wkt']
        
        con = get_connection()
        # Coordinates are being transformed from EPSG:25830 to EPSG:4326.
        # This is computationally expensive and could increase query time.
        df = con.execute(f"""
            WITH municipios_in_polygon AS (
                SELECT zone_id, 
                ST_Y(ST_Transform(centroid, 'EPSG:25830', 'EPSG:4326')) as lat,
                ST_X(ST_Transform(centroid, 'EPSG:25830', 'EPSG:4326')) as lon
                FROM lakehouse.silver.dim_zones
                WHERE ST_Contains(ST_GeomFromText('{polygon_wkt}'), polygon)
            )
            SELECT 
                g.total_trips as actual_trips, 
                g.mismatch_ratio,
                zo.lat as lat_origen, zo.lon as lon_origen,
                zd.lat as lat_destino, zd.lon as lon_destino
            FROM lakehouse.gold.infrastructure_gaps g
            JOIN municipios_in_polygon zo ON g.org_zone_id = zo.zone_id
            JOIN municipios_in_polygon zd ON g.dest_zone_id = zd.zone_id
            WHERE g.total_trips > 10;
            """).df()
        
        con.close()

        if df.empty:
            logging.warning("No data found for the given polygon.")
            return

        # Map Generation
        # Initialize map centered on Spain with a clean background
        m = folium.Map(location=[40.4, -3.7], zoom_start=6, tiles="cartodbpositron")
        all_coords = []

        for _, row in df.iterrows():
            start = [row.lat_origen, row.lon_origen]
            end = [row.lat_destino, row.lon_destino]
            all_coords.extend([start, end])

            # Color coding based on mismatch ratio (red for critical gaps)
            color = "#d32f2f" if row.mismatch_ratio < 0.1 else "#ff9800" 
            
            # Use logarithmic scale for line weight.
            base_weight = math.log10(row.actual_trips) if row.actual_trips > 0 else 1
            weight = min(8, max(1, base_weight)) # Limitamos entre 1px y 8px máximo
            
            # Use dynamic opacity to reduce visual clutter in high-density areas
            opacity = 0.4 if weight < 3 else 0.7

            folium.PolyLine(
                [start, end],
                color=color,
                weight=weight,
                opacity=opacity, # Opacidad dinámica o más baja (0.4 es bueno para muchas líneas)
                tooltip=f"Ratio: {row.mismatch_ratio:.2f} | Viajes: {int(row.actual_trips)}"
            ).add_to(m)

        # Automatically adjust map zoom to fit all drawn coordinates
        if all_coords:
            m.fit_bounds(all_coords)

        # Storage and Upload
        local_path = f"/tmp/map_{run_id}.html"
        m.save(local_path)

        s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
        s3_hook.load_file(
            filename=local_path,
            key=f"gold/report/mismatch_map_{run_id}.html",
            bucket_name="ducklake-bdproject",
            replace=True
        )
        
        os.remove(local_path)
        logging.info("Mapa subido exitosamente a S3.")

    generate_and_upload_map()

mobility_06_gaps_map()