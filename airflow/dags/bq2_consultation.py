from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
import logging
import os
# Import matplotlib and set backend to 'Agg' for headless server rendering
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# --- CONFIGURATION ---
DEFAULT_POLYGON = "POLYGON((-18.5 27.4, -18.5 44.0, 4.5 44.0, 4.5 27.4, -18.5 27.4))"

@dag(
    dag_id="bq2_consultation",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string", title="Spatial Filter (WKT)")
    },
    tags=['mobility', 'gold', 'visualization', 'png']
)
def mobility_06_gaps_static_image():

    # --- HELPER FUNCTIONS FOR COORDINATE TRANSFORMATION ---
    def transform_wkt_to_wgs84(wkt_str, source_epsg):
        """Helper: Transforms a WKT string from DB CRS to WGS84 (Lat/Lon)"""
        from shapely import wkt
        from shapely.ops import transform
        from pyproj import CRS, Transformer
        if not wkt_str: return None
        project = Transformer.from_crs(CRS(source_epsg), CRS("EPSG:4326"), always_xy=True).transform
        return transform(project, wkt.loads(wkt_str)).wkt

    def get_projected_search_wkt(input_wkt, target_epsg):
        """Helper: Transforms Input WKT (Lat/Lon) to DB CRS for searching"""
        from shapely import wkt
        from shapely.ops import transform
        from pyproj import CRS, Transformer
        project = Transformer.from_crs(CRS("EPSG:4326"), CRS(target_epsg), always_xy=True).transform
        return transform(project, wkt.loads(input_wkt)).wkt

    @task
    def consult_gaps_with_shapes(**context):
        logging.info("--- 🏗️ Consulting: Gaps & Fetching Polygon Shapes ---")
        input_wkt = context['params']['polygon_wkt']
        con = get_connection()
        
        # Candidates for DB projection
        candidates = ["EPSG:25830", "EPSG:3857"]

        try:
            for epsg_code in candidates:
                logging.info(f"🔄 ATTEMPT: Using projection {epsg_code}...")
                try:
                    search_wkt = get_projected_search_wkt(input_wkt, epsg_code)
                except Exception as e:
                     logging.error(f"Projection init failed: {e}"); continue

                # 1. Fetch raw WKTs from DB (in DB's coordinate system)
                query = f"""
                    SELECT 
                        g.total_trips, 
                        ROUND(g.mismatch_ratio, 4) as mismatch_ratio,
                        ST_AsText(zo.polygon) as source_wkt_raw,
                        ST_AsText(zd.polygon) as target_wkt_raw
                    FROM lakehouse.gold.infrastructure_gaps g
                    JOIN lakehouse.silver.dim_zones zo ON g.org_zone_id = zo.zone_id
                    JOIN lakehouse.silver.dim_zones zd ON g.dest_zone_id = zd.zone_id
                    WHERE ST_Intersects(zo.polygon, ST_GeomFromText('{search_wkt}'))
                      AND ST_Intersects(zd.polygon, ST_GeomFromText('{search_wkt}'))
                      AND g.total_trips > 10
                    ORDER BY g.mismatch_ratio ASC LIMIT 10;
                """
                df = con.execute(query).df()

                if not df.empty:
                    logging.info(f"✅ Found data with {epsg_code}. Processing shapes...")
                    # 2. Python-side Transformation of full polygons to WGS84
                    df['source_wkt_84'] = df['source_wkt_raw'].apply(lambda x: transform_wkt_to_wgs84(x, epsg_code))
                    df['target_wkt_84'] = df['target_wkt_raw'].apply(lambda x: transform_wkt_to_wgs84(x, epsg_code))
                    # Drop raw columns to save space in XCom
                    df = df.drop(columns=['source_wkt_raw', 'target_wkt_raw'])
                    return df.to_dict(orient='records')
            
            logging.error("❌ Failed to find data.")
            return []
        finally:
            con.close()

    @task
    def generate_static_image(gaps_data: list):
        if not gaps_data: logging.warning("No data"); return
        
        logging.info("--- 🎨 Generating Static PNG Map ---")
        try:
            import geopandas as gpd
            from shapely import wkt
            from shapely.geometry import LineString
            import contextily as ctx # For basemaps
        except ImportError:
            logging.error("❌ Missing libraries. Run: pip install geopandas matplotlib contextily shapely")
            return

        df = pd.DataFrame(gaps_data)

        # 1. Create GeoDataFrames for Sources and Targets
        # Convert WKT strings to geometry objects
        df['source_geom'] = df['source_wkt_84'].apply(wkt.loads)
        df['target_geom'] = df['target_wkt_84'].apply(wkt.loads)

        gdf_source = gpd.GeoDataFrame(df, geometry='source_geom', crs="EPSG:4326")
        gdf_target = gpd.GeoDataFrame(df, geometry='target_geom', crs="EPSG:4326")

        # 2. Create Connection Lines (for arrows/lines)
        # We create a line from source centroid to target centroid
        lines = [LineString([row['source_geom'].centroid, row['target_geom'].centroid]) for _, row in df.iterrows()]
        gdf_lines = gpd.GeoDataFrame(df, geometry=lines, crs="EPSG:4326")

        # 3. Reproject to WebMercator (EPSG:3857) for plotting with Contextily basemaps
        gdf_source_wm = gdf_source.to_crs(epsg=3857)
        gdf_target_wm = gdf_target.to_crs(epsg=3857)
        gdf_lines_wm = gdf_lines.to_crs(epsg=3857)

        # --- PLOTTING ---
        fig, ax = plt.subplots(figsize=(15, 15)) # Large high-res figure

        # A. Plot Polygons
        # Source = Blue, Target = Red, semi-transparent
        gdf_source_wm.plot(ax=ax, color='blue', alpha=0.4, edgecolor='darkblue', linewidth=1, label='Origin Zone')
        gdf_target_wm.plot(ax=ax, color='red', alpha=0.4, edgecolor='darkred', linewidth=1, label='Dest Zone')

        # B. Plot Lines (Color by mismatch ratio)
        # We use a colormap (plasma) to show severity of mismatch
        gdf_lines_wm.plot(ax=ax, column='mismatch_ratio', cmap='plasma', linewidth=3, legend=True, 
                          legend_kwds={'label': "Mismatch Ratio (Severity)", 'shrink': 0.5})

        # C. Add Arrows (Tricky in matplotlib, simple approach here)
        # We iterate and draw a small arrow at the end of the line
        for _, row in gdf_lines_wm.iterrows():
            start = row.geometry.coords[0]
            end = row.geometry.coords[1]
            ax.annotate("",
                        xy=end, xycoords='data',
                        xytext=start, textcoords='data',
                        arrowprops=dict(arrowstyle="->", color="black", lw=1.5, alpha=0.7))

        # D. Add Basemap (OpenStreetMap via CartoDB style)
        try:
            ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)
        except Exception as e:
            logging.warning(f"Could not add basemap (internet needed): {e}")

        # E. Final Touches
        ax.set_title(f"Top 10 Infrastructure Gaps (Red=Dest, Blue=Origin)", fontsize=16)
        ax.set_axis_off() # Remove X/Y axis numbers
        plt.legend(loc='upper right')
        plt.tight_layout()

        # --- SAVE ---
        airflow_home = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
        save_path = os.path.join(airflow_home, "dags", "results", "infrastructure_gaps_map.png")
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        plt.close(fig) # Clear memory
        logging.info(f"✅ Static Image Saved: {save_path}")

    # Flow
    data = consult_gaps_with_shapes()
    generate_static_image(data)

dag = mobility_06_gaps_static_image()