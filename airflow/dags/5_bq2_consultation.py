from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
import logging
import os
import json

# --- CONFIGURATION ---
DEFAULT_POLYGON = "POLYGON((-18.5 27.4, -18.5 44.0, 4.5 44.0, 4.5 27.4, -18.5 27.4))"

@dag(
    dag_id="5_bq2_consultations",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        # Date params kept for consistency, though this DAG queries the pre-built Gold table
        "start_date": Param("20230101", type="string", description="YYYY-MM-DD"),
        "end_date": Param("20231231", type="string", description="YYYY-MM-DD"),
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string", title="Spatial Filter (WKT)", description="Paste your WKT Polygon here.")
    },
    tags=['mobility', 'gold', 'visualization', 'kepler']
)
def bq2_consultations():

    @task
    def consult_infrastructure_gaps(**context):
        """
        Task 1: Queries Top 10 Mismatches and extracts Coordinates for mapping.
        Returns: A JSON-serializable list of records to be used by the Kepler task.
        """
        logging.info("--- 🏗️ Consulting: Spatial Infrastructure Gaps ---")
        params = context['params']
        input_wkt = params['polygon_wkt']

        con = get_connection()
        try:
            # Note: I corrected 'g.potential' to 'g.estimated_potential_trips' based on your previous DAG.
            # I also added ST_X/ST_Y extraction to get coordinates for KeplerGL.
            query_consult = f"""
                SELECT 
                    g.origin_zone_id,
                    g.destination_zone_id,
                    g.total_trips as actual_trips, 
                    CAST(g.estimated_potential_trips AS INT) as potential_trips,
                    ROUND(g.mismatch_ratio, 4) as mismatch_ratio,
                    
                    -- Extract Coordinates for Visualization (Centroids)
                    ST_Y(ST_Centroid(zo.polygon)) as source_lat,
                    ST_X(ST_Centroid(zo.polygon)) as source_lon,
                    ST_Y(ST_Centroid(zd.polygon)) as target_lat,
                    ST_X(ST_Centroid(zd.polygon)) as target_lon,

                    -- Recalculate distance for reference
                    ROUND(GREATEST(0.5, st_distance_spheroid(ST_Centroid(zo.polygon), ST_Centroid(zd.polygon))/1000), 2) AS dist_km
                
                FROM lakehouse.gold.infrastructure_gaps g
                JOIN lakehouse.silver.dim_zones zo ON g.origin_zone_id = zo.zone_id
                JOIN lakehouse.silver.dim_zones zd ON g.destination_zone_id = zd.zone_id
                WHERE 
                    ST_Intersects(zo.polygon, ST_GeomFromText('{input_wkt}'))
                    AND ST_Intersects(zd.polygon, ST_GeomFromText('{input_wkt}'))
                    AND g.total_trips > 10  -- Filter noise
                ORDER BY g.mismatch_ratio ASC
                LIMIT 10;
            """
            
            df_gaps = con.execute(query_consult).df()

            if df_gaps.empty:
                logging.warning("⚠️ No infrastructure gaps found within this polygon.")
                return []
            else:
                logging.info(f"\n🔎 TOP 10 Mismatches in Selected Polygon:\n{df_gaps.to_string(index=False)}")
                # Convert to dict to pass to the next task via XCom (Data is small: 10 rows)
                return df_gaps.to_dict(orient='records')

        finally:
            con.close()

    @task
    def generate_kepler_map(gaps_data: list):
        """
        Task 2: Visualizes the gaps using KeplerGL.
        Input: List of records from Task 1.
        Output: Saves an HTML map.
        """
        if not gaps_data:
            logging.warning("⚠️ No data received from consultation task. Skipping map generation.")
            return

        logging.info(f"--- 🗺️ Generating KeplerGL Map for {len(gaps_data)} routes ---")
        
        try:
            from keplergl import KeplerGl
        except ImportError:
            logging.error("❌ KeplerGL library not found. Please run 'pip install keplergl'")
            return

        # 1. Prepare Data
        df = pd.DataFrame(gaps_data)
        
        # 2. Configure KeplerGL State (Force Arc Layer)
        # This config ensures the map opens with Arcs (Arrows) drawn between points
        config = {
            "version": "v1",
            "config": {
                "visState": {
                    "layers": [
                        {
                            "type": "arc",
                            "config": {
                                "label": "Infrastructure Gaps",
                                "columns": {
                                    "lat0": "source_lat",
                                    "lng0": "source_lon",
                                    "lat1": "target_lat",
                                    "lng1": "target_lon"
                                },
                                "color": [255, 0, 0],  # Red Arrows
                                "visConfig": {
                                    "opacity": 0.8,
                                    "thickness": 2,
                                    "colorRange": {
                                        "name": "Global Warming",
                                        "type": "sequential",
                                        "category": "Uber",
                                        "colors": ["#5A1846", "#900C3F", "#C70039", "#E3611C", "#F1920E", "#FFC300"]
                                    },
                                    "sizeField": {"name": "potential_trips", "type": "integer"}
                                }
                            }
                        }
                    ]
                }
            }
        }

        # 3. Initialize Map
        map_1 = KeplerGl(height=600, config=config)
        map_1.add_data(data=df, name='Top 10 Gaps')

        # 4. Save to Disk
        output_folder = 'results'
        filename = 'infrastructure_gaps_map.html'
        
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            
        save_path = os.path.join(output_folder, filename)
        
        # Save HTML
        map_1.save_to_html(file_name=save_path)
        logging.info(f"✅ Map successfully saved to: {save_path}")

    # --- DAG FLOW ---
    top_gaps_data = consult_infrastructure_gaps()
    generate_kepler_map(top_gaps_data)

bq2_consultations()
