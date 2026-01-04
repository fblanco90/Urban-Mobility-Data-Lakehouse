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
    dag_id="bq3_funtional_classification",
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
def bq3_funtional_classification():

    @task
    def classify_zone_functions():
        """
        Task: Functional Classification of Zones
        OPTIMIZATION: Aggregates raw mobility data first, then joins dimensions.
        """
        logging.info("--- 🏷️ Starting Table 3: Functional Zone Classification (Optimized) ---")

        with get_connection() as con:
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.gold.zone_functional_classification AS
                WITH pre_aggregated_flows AS (
                    SELECT 
                        origin_zone_id,
                        destination_zone_id,
                        SUM(trips) as total_trips
                    FROM lakehouse.silver.fact_mobility
                    GROUP BY 1, 2
                ),
                zone_flow_stats AS (
                    SELECT 
                        -- We take the union of all zones appearing as origin or dest
                        coalesce(o.origin_zone_id, d.destination_zone_id) as zone_id,
                        
                        -- Internal: Origin = Dest
                        SUM(CASE 
                            WHEN o.origin_zone_id = o.destination_zone_id THEN o.total_trips 
                            ELSE 0 
                        END) as internal_trips,

                        -- Outflow: Trips starting here (excluding internal)
                        SUM(CASE 
                            WHEN o.origin_zone_id IS NOT NULL AND o.origin_zone_id != o.destination_zone_id THEN o.total_trips 
                            ELSE 0 
                        END) as outflow,

                        -- Inflow: Trips ending here (excluding internal)
                        SUM(CASE 
                            WHEN d.destination_zone_id IS NOT NULL AND d.origin_zone_id != d.destination_zone_id THEN d.total_trips 
                            ELSE 0 
                        END) as inflow

                    FROM pre_aggregated_flows o
                    FULL OUTER JOIN pre_aggregated_flows d ON o.origin_zone_id = d.destination_zone_id
                    GROUP BY 1
                ),
                metrics_calc AS (
                    -- Calculate Ratios
                    SELECT 
                        zone_id,
                        internal_trips,
                        outflow,
                        inflow,
                        (outflow + internal_trips) AS total_generated, -- Total Trips Originating Here
                        
                        -- Net Flow Ratio
                        CASE 
                            WHEN (inflow + outflow) = 0 THEN 0 
                            ELSE (inflow - outflow) / (inflow + outflow) 
                        END AS net_flow_ratio,

                        -- Retention Rate
                        CASE 
                            WHEN (outflow + internal_trips) = 0 THEN 0
                            ELSE internal_trips / (outflow + internal_trips)
                        END AS retention_rate
                    FROM zone_flow_stats
                )
                -- 4. JOIN LAST: Attach Zone Names and Apply Labels
                SELECT 
                    m.zone_id,
                    z.zone_name,
                    m.internal_trips,
                    m.inflow,
                    m.outflow,
                    ROUND(m.net_flow_ratio, 3) as net_flow_ratio,
                    ROUND(m.retention_rate, 3) as retention_rate,
                    
                    CASE 
                        WHEN m.retention_rate > 0.6 THEN 'Self-Sustaining Cell'
                        WHEN m.net_flow_ratio > 0.15 THEN 'Activity Hub (Importer)'
                        WHEN m.net_flow_ratio < -0.15 THEN 'Bedroom Community (Exporter)'
                        ELSE 'Balanced / Transit Zone'
                    END AS functional_label,
                    
                    CURRENT_TIMESTAMP as processed_at
                FROM metrics_calc m
                JOIN lakehouse.silver.dim_zones z ON m.zone_id = z.zone_id
                WHERE (m.internal_trips + m.inflow + m.outflow) > 0;
            """)

        logging.info("✅ Gold: Zone Functional Classification table created/updated.")

    @task
    def fetch_classification_data(ready: bool, **context):
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
                    c.*,
                    -- Coordinates for Mapping
                    ST_Y(ST_Centroid(z.polygon)) as lat,
                    ST_X(ST_Centroid(z.polygon)) as lon
                    
                FROM lakehouse.gold.zone_functional_classification c
                JOIN lakehouse.silver.dim_zones z ON c.zone_id = z.zone_id
                WHERE ST_Intersects(z.polygon, ST_GeomFromText('{input_wkt}'));
            """
            
            df = con.execute(query).df()

            if df.empty:
                logging.warning("⚠️ No classification data found.")
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
    # 1. Ejecutar clasificación (Gold Table)
    ready_signal = classify_zone_functions()
    
    # 2. Pasar la señal de "listo" a la extracción de datos
    data = fetch_classification_data(ready=ready_signal)
    
    # 3. Generar visualizaciones finales
    generate_analytical_plots(data)

bq3_funtional_classification()