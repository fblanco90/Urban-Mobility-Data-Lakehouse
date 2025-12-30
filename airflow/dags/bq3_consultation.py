from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
import logging
import os
import json

# --- MATPLOTLIB HEADLESS SETUP ---
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt

# --- CONFIGURATION ---
DEFAULT_POLYGON = "POLYGON((-18.5 27.4, -18.5 44.0, 4.5 44.0, 4.5 27.4, -18.5 27.4))"

@dag(
    dag_id="mobility_06_functional_viz",
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
def mobility_06_functional_viz():

    @task
    def fetch_classification_data(**context):
        """
        Task 1: Fetches the pre-computed classification metrics and zone coordinates.
        """
        logging.info("--- 🏷️ Fetching Functional Classification Data ---")
        params = context['params']
        input_wkt = params['polygon_wkt']

        con = get_connection()
        try:
            # We fetch metrics + Centroid coordinates for the map
            query = f"""
                SELECT 
                    c.zone_id,
                    c.zone_name,
                    c.functional_label,
                    c.net_flow_ratio,
                    c.retention_rate,
                    c.internal_trips,
                    c.inflow,
                    c.outflow,
                    
                    -- Coordinates for Mapping
                    ST_Y(ST_Centroid(z.polygon)) as lat,
                    ST_X(ST_Centroid(z.polygon)) as lon
                    
                FROM lakehouse.gold.zone_functional_classification c
                JOIN lakehouse.silver.dim_zones z ON c.zone_id = z.zone_id
                WHERE ST_Intersects(z.polygon, ST_GeomFromText('{input_wkt}'))
                ORDER BY c.net_flow_ratio DESC;
            """
            
            df = con.execute(query).df()

            if df.empty:
                logging.warning("⚠️ No classification data found. Ensure 'mobility_03_gold_analytics' has run.")
                return None
            
            logging.info(f"Fetched {len(df)} zones for analysis.")
            return df.to_dict(orient='records')

        finally:
            con.close()

    @task
    def generate_analytical_plots(data_records: list):
        """
        Task 2: Generates visual analysis (Scatter Plot + Map).
        """
        if not data_records:
            logging.warning("Skipping visualization due to empty data.")
            return

        df = pd.DataFrame(data_records)
        output_folder = 'results'
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        # --- PART A: SCATTER PLOT (Science View) ---
        # Logic: Plot 'Retention' (Y) vs 'Net Flow' (X) to show the clusters.
        try:
            logging.info("Generating Scatter Plot...")
            fig, ax = plt.subplots(figsize=(10, 8))
            
            # Scatter plot with custom colors mapping
            colors = {
                'Activity Hub (Importer)': 'red',
                'Bedroom Community (Exporter)': 'blue',
                'Self-Sustaining Cell': 'green',
                'Balanced / Transit Zone': 'gray'
            }
            
            # Iterate categories to plot them with correct legend
            for label, color in colors.items():
                subset = df[df['functional_label'] == label]
                if not subset.empty:
                    ax.scatter(
                        subset['net_flow_ratio'], 
                        subset['retention_rate'], 
                        c=color, 
                        label=label, 
                        alpha=0.6, 
                        edgecolors='w', 
                        s=80
                    )

            # Reference Lines
            ax.axvline(x=0, color='black', linestyle='--', linewidth=0.8, alpha=0.5) # Balanced Flow
            ax.axhline(y=0.5, color='black', linestyle='--', linewidth=0.8, alpha=0.5) # 50% Retention

            # Labels
            ax.set_title('Functional Classification Analysis\n(Net Flow vs. Local Retention)', fontsize=14)
            ax.set_xlabel('Net Flow Ratio ( < 0 Exporter | Importer > 0 )', fontsize=12)
            ax.set_ylabel('Retention Rate (Local Trips / Total Generated)', fontsize=12)
            ax.legend(title="Zone Function")
            ax.grid(True, linestyle='--', alpha=0.3)
            
            # Annotate Top Hubs
            top_hubs = df[df['functional_label'] == 'Activity Hub (Importer)'].head(3)
            for _, row in top_hubs.iterrows():
                ax.annotate(
                    row['zone_name'], 
                    (row['net_flow_ratio'], row['retention_rate']),
                    xytext=(5, 5), textcoords='offset points', fontsize=8
                )

            # Save
            plot_path = os.path.join(output_folder, 'functional_classification_matrix.png')
            plt.savefig(plot_path, dpi=300)
            logging.info(f"✅ Scatter plot saved: {plot_path}")
            plt.close(fig)

        except Exception as e:
            logging.error(f"❌ Scatter plot failed: {e}")

        # --- PART B: KEPLER MAP (Spatial View) ---
        # Logic: Map zones colored by their Functional Label.
        try:
            from keplergl import KeplerGl
            logging.info("Generating Kepler Map...")

            # Config: Color points by 'functional_label'
            config = {
                "version": "v1",
                "config": {
                    "visState": {
                        "layers": [
                            {
                                "type": "point",
                                "config": {
                                    "label": "Functional Zones",
                                    "columns": {
                                        "lat": "lat",
                                        "lng": "lon",
                                    },
                                    "color": [18, 147, 154],
                                    "visConfig": {
                                        "radius": 15,
                                        "opacity": 0.8,
                                        "colorRange": {
                                            "name": "Custom",
                                            "type": "qualitative",
                                            "colors": ["#FF0000", "#0000FF", "#00FF00", "#808080"] # Red, Blue, Green, Gray
                                        }
                                    }
                                },
                                "visualChannels": {
                                    "colorField": {"name": "functional_label", "type": "string"},
                                    "colorScale": "ordinal"
                                }
                            }
                        ]
                    }
                }
            }

            map_1 = KeplerGl(height=600, config=config)
            map_1.add_data(data=df, name='Functional Zones')
            
            map_path = os.path.join(output_folder, 'functional_zones_map.html')
            map_1.save_to_html(file_name=map_path)
            logging.info(f"✅ Map saved: {map_path}")

        except ImportError:
            logging.warning("⚠️ KeplerGL not installed. Skipping map generation.")
        except Exception as e:
            logging.error(f"❌ Kepler map failed: {e}")

    # --- DAG FLOW ---
    data = fetch_classification_data()
    generate_analytical_plots(data)

mobility_06_functional_viz()