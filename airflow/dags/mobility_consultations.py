from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
import matplotlib.pyplot as plt
import logging
import os

# --- CONFIGURATION ---
DEFAULT_POLYGON = "POLYGON((-7.55 36.0, -7.55 38.8, -1.0 38.8, -1.0 36.0, -7.55 36.0))"

@dag(
    dag_id="mobility_consultations",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("2023-01-01", type="string", description="YYYY-MM-DD"),
        "end_date": Param("2023-01-31", type="string", description="YYYY-MM-DD"),
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string", title="Spatial Filter (WKT)", description="Paste WKT to filter the existing Gold data.")
    },
    tags=['mobility', 'gold', 'consultation']
)
def mobility_consultations():

    @task
    def consult_clustering_patterns(**context):
        """
        Consultation 1: Mobility Patterns
        Query: 'lakehouse.gold.typical_day_by_cluster' & 'lakehouse.gold.dim_cluster_assignments'
        Filter: Applies the Input Date Range to the assignments.
        Output: Generates a plot of the profiles active during that window.
        """
        logging.info("--- 📊 Consulting Gold: Mobility Patterns ---")
        params = context['params']
        start_date = params['start_date']
        end_date = params['end_date']

        con = get_connection()
        try:
            # 1. Get the Profile Definitions (The "Shapes" of the day)
            # We don't filter this by date/polygon usually, as the profiles are definitions (Cluster 0, 1, 2)
            query_profiles = "SELECT * FROM lakehouse.gold.typical_day_by_cluster ORDER BY cluster_id, hour"
            df_profiles = con.execute(query_profiles).df()

            # 2. Get the Assignments (Which day was which cluster)
            # HERE we apply the Date Filter to see only the relevant period
            query_assignments = f"""
                SELECT * FROM lakehouse.gold.dim_cluster_assignments
                WHERE date BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY date
            """
            df_assignments = con.execute(query_assignments).df()

            if df_profiles.empty or df_assignments.empty:
                logging.warning("⚠️ No data found in Gold tables for this date range.")
                return

            # --- VISUALIZATION LOGIC ---
            logging.info(f"Found {len(df_assignments)} assigned days in the selected range.")
            
            # Calculate distribution for the logs
            dist = df_assignments['cluster_id'].value_counts(normalize=True) * 100
            logging.info(f"Cluster Distribution in selected period:\n{dist.to_string()}")

            # Plotting
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # Get unique clusters active in this window
            active_clusters = df_assignments['cluster_id'].unique()
            
            for cid in active_clusters:
                subset = df_profiles[df_profiles['cluster_id'] == cid]
                if not subset.empty:
                    ax.plot(subset['hour'], subset['avg_trips'], marker='o', label=f'Cluster {cid}')

            ax.set_title(f'Mobility Profiles (Active from {start_date} to {end_date})')
            ax.set_xlabel('Hour of Day')
            ax.set_ylabel('Average Trips')
            ax.grid(True, linestyle='--', alpha=0.5)
            ax.legend()
            
            # Save Plot
            output_dir = "consultation_results"
            os.makedirs(output_dir, exist_ok=True)
            save_path = os.path.join(output_dir, f"consultation_patterns_{start_date}.png")
            plt.savefig(save_path)
            plt.close(fig)
            
            logging.info(f"✅ Plot saved to: {save_path}")

        except Exception as e:
            logging.error(f"Error consulting clusters: {e}")
            raise e
        finally:
            con.close()

    @task
    def consult_infrastructure_gaps(**context):
        """
        Consultation 2: Infrastructure Gaps
        Query: 'lakehouse.gold.infrastructure_gaps'
        Filter: JOINS with 'dim_zones' to apply the Input Polygon Filter (WKT).
        Output: Logs the Top 10 Mismatches found INSIDE the polygon.
        """
        logging.info("--- 🏗️ Consulting Gold: Infrastructure Gaps ---")
        params = context['params']
        input_wkt = params['polygon_wkt']

        con = get_connection()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")

            # We join the Gold table (Results) back to Silver (Geometry) to apply the spatial filter
            query_consult = f"""
                SELECT 
                    g.org_zone_id,
                    g.dest_zone_id,
                    g.total_trips as actual_trips,
                    g.estimated_potential_trips as potential,
                    g.mismatch_ratio,
                    g.geographic_distance_km
                FROM lakehouse.gold.infrastructure_gaps g
                JOIN lakehouse.silver.dim_zones zo ON g.org_zone_id = zo.zone_id
                JOIN lakehouse.silver.dim_zones zd ON g.dest_zone_id = zd.zone_id
                WHERE 
                    -- SPATIAL FILTER applied to the existing Gold data
                    ST_Intersects(zo.polygon, ST_GeomFromText('{input_wkt}'))
                    AND ST_Intersects(zd.polygon, ST_GeomFromText('{input_wkt}'))
                ORDER BY g.mismatch_ratio ASC
                LIMIT 10;
            """
            
            df_gaps = con.execute(query_consult).df()

            if df_gaps.empty:
                logging.warning("⚠️ No infrastructure gaps found within this polygon in the Gold table.")
            else:
                logging.info(f"\n🔎 TOP 10 Underserved Routes in Selected Polygon:\n{df_gaps.to_string(index=False)}")

        except Exception as e:
            logging.error(f"Error consulting gaps: {e}")
            raise e
        finally:
            con.close()

    # --- DAG FLOW ---
    # These tasks run independently as they are just consulting different tables
    consult_clustering_patterns()
    consult_infrastructure_gaps()

mobility_consultations()