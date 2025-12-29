from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
import logging
import os

# --- MATPLOTLIB HEADLESS SETUP ---
# Critical: Must be set before importing pyplot to avoid "no display name" errors
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt

# --- CONFIGURATION ---
DEFAULT_POLYGON = "POLYGON((-18.5 27.4, -18.5 44.0, 4.5 44.0, 4.5 27.4, -18.5 27.4))"

@dag(
    dag_id="mobility_04_visualization",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="YYYY-MM-DD"),
        "end_date": Param("20231231", type="string", description="YYYY-MM-DD"),
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string", title="Spatial Filter (WKT)", description="Paste your WKT Polygon here.")
    },
    tags=['mobility', 'visualization', 'reporting']
)
def mobility_04_visualization():

    @task
    def plot_clustering_results(**context):
        """
        Task: Visualization
        Generates a .png plot of the typical daily profiles found in the Gold Layer.
        """
        logging.info("--- 🎨 Plotting Cluster Results ---")
        
        # 1. Retrieve Params (Requested as inputs, even if Gold table is pre-calculated)
        params = context['params']
        input_start_date = params['start_date']
        input_end_date = params['end_date']
        
        con = get_connection()

        try:
            # 2. Query Data 
            # We join the profile data (lines) with the assignment data (to calculate labels like "Weekday/Weekend")
            query = """
            WITH cluster_labels AS (
                SELECT 
                    cluster_id, 
                    MODE(dayname(date)) as label
                FROM lakehouse.gold.dim_cluster_assignments
                GROUP BY cluster_id
            )
            SELECT 
                t.hour,
                'Cluster ' || t.cluster_id || ' (' || l.label || ')' as pattern_name,
                t.avg_trips
            FROM lakehouse.gold.typical_day_by_cluster t
            JOIN cluster_labels l ON t.cluster_id = l.cluster_id
            ORDER BY t.hour;
            """
            
            logging.info("Fetching data from Gold Layer...")
            demand_df = con.execute(query).df()

            if demand_df.empty:
                logging.error("❌ No data to plot. Ensure 'mobility_03_gold_analytics' has run successfully.")
                return

            # 3. Pivot for Plotting
            logging.info("Pivoting data for Matplotlib...")
            pivot_df = demand_df.pivot(index='hour', columns='pattern_name', values='avg_trips')

            # 4. Generate Plot
            logging.info(f"Generating plot for period: {input_start_date} - {input_end_date}")
            fig, ax = plt.subplots(figsize=(12, 7))
            
            pivot_df.plot(kind='line', ax=ax, marker='o', markersize=4)
            
            ax.set_title(f'Typical Daily Mobility Patterns (Clustered Profiles)\nPeriod: {input_start_date} to {input_end_date}', fontsize=16)
            ax.set_xlabel('Hour of Day', fontsize=12)
            ax.set_ylabel('Average Trips per Hour', fontsize=12)
            
            ax.set_xticks(range(0, 24))
            ax.set_xticklabels([f'{h:02d}:00' for h in range(24)], rotation=45, ha='right')
            ax.grid(True, linestyle='--', alpha=0.6)
            ax.legend(title='Identified Pattern')
            
            plt.tight_layout()

            # 5. Save to Disk
            output_folder = 'results'
            filename = f'typical_daily_patterns_{input_start_date}_{input_end_date}.png'
            
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
                logging.info(f"Created directory: {output_folder}")
                
            save_path = os.path.join(output_folder, filename)
            plt.savefig(save_path, dpi=300)
            
            logging.info(f"✅ Plot successfully saved to: {save_path}")
            
            plt.close(fig)

        except Exception as e:
            logging.error(f"❌ Plotting failed: {e}")
            raise e

        finally:
            con.close()

    # --- DAG FLOW ---
    plot_clustering_results()

mobility_04_visualization()