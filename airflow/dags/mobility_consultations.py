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
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string", title="Spatial Filter (WKT)", description="Paste WKT to filter the consultation.")
    },
    tags=['mobility', 'gold', 'consultation']
)
def mobility_consultations():

    @task
    def consult_clustering_by_polygon(**context):
        """
        Consultation 1: Spatial Clustering Patterns
        
        Logic:
        1. Get 'Cluster Assignments' from GOLD (Which dates are Cluster 0, 1, 2?)
        2. Get 'Actual Trips' from SILVER filtered by INPUT POLYGON.
        3. Join them to see the profile of THIS POLYGON broken down by the Clusters.
        """
        logging.info("--- 📊 Consulting: Polygon Specific Clustering ---")
        params = context['params']
        start_date = params['start_date']
        end_date = params['end_date']
        input_wkt = params['polygon_wkt']

        con = get_connection()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")

            # 1. Fetch the Classifications (Gold)
            # "On this date, the system says it was Cluster X"
            query_assignments = f"""
                SELECT date, cluster_id 
                FROM lakehouse.gold.dim_cluster_assignments
                WHERE date BETWEEN '{start_date}' AND '{end_date}'
            """
            
            # 2. Fetch the Activity for the Input Polygon (Silver)
            # "On this date/hour, how many trips happened inside the user's polygon?"
            query_mobility = f"""
                SELECT 
                    m.partition_date AS date,
                    hour(m.period)   AS hour,
                    SUM(m.trips)     AS local_trips
                FROM lakehouse.silver.fact_mobility m
                JOIN lakehouse.silver.dim_zones zo ON m.origin_zone_id = zo.zone_id
                JOIN lakehouse.silver.dim_zones zd ON m.destination_zone_id = zd.zone_id
                WHERE m.partition_date BETWEEN '{start_date}' AND '{end_date}'
                  -- SPATIAL FILTER
                  AND ST_Intersects(zo.polygon, ST_GeomFromText('{input_wkt}'))
                  AND ST_Intersects(zd.polygon, ST_GeomFromText('{input_wkt}'))
                GROUP BY 1, 2
            """

            # 3. Join & Calculate Profile
            # We do this in SQL for efficiency
            full_query = f"""
                WITH gold_assignments AS ({query_assignments}),
                     silver_activity AS ({query_mobility})
                
                SELECT 
                    g.cluster_id,
                    s.hour,
                    AVG(s.local_trips) as avg_trips,
                    COUNT(DISTINCT s.date) as days_in_sample
                FROM silver_activity s
                JOIN gold_assignments g ON s.date = g.date
                GROUP BY g.cluster_id, s.hour
                ORDER BY g.cluster_id, s.hour
            """

            df_result = con.execute(full_query).df()

            if df_result.empty:
                logging.warning("⚠️ No data found (Check Polygon overlap or Date Range).")
                return

            logging.info(f"Generated profiles based on {df_result['days_in_sample'].max()} days of data.")

            # --- VISUALIZATION ---
            fig, ax = plt.subplots(figsize=(12, 6))
            
            unique_clusters = df_result['cluster_id'].unique()
            for cid in unique_clusters:
                subset = df_result[df_result['cluster_id'] == cid]
                ax.plot(subset['hour'], subset['avg_trips'], marker='o', label=f'Cluster {cid}')

            ax.set_title(f'Average Daily Profile for Input Polygon\n(Split by Gold Cluster ID)')
            ax.set_xlabel('Hour of Day')
            ax.set_ylabel('Average Trips (Inside Polygon)')
            ax.grid(True, linestyle='--', alpha=0.5)
            ax.legend(title="Day Type (Gold)")
            
            # Save
            output_dir = "consultation_results"
            os.makedirs(output_dir, exist_ok=True)
            save_path = os.path.join(output_dir, f"polygon_cluster_profile_{start_date}.png")
            plt.savefig(save_path)
            plt.close(fig)
            
            logging.info(f"✅ Polygon-specific plot saved to: {save_path}")

        except Exception as e:
            logging.error(f"Error in spatial clustering consultation: {e}")
            raise e
        finally:
            con.close()

    @task
    def consult_infrastructure_gaps(**context):
        """
        Consultation 2: Infrastructure Gaps
        Query: 'lakehouse.gold.infrastructure_gaps'
        Filter: Applies Input Polygon Filter (WKT) to existing Gold data.
        """
        logging.info("--- 🏗️ Consulting: Spatial Infrastructure Gaps ---")
        params = context['params']
        input_wkt = params['polygon_wkt']

        con = get_connection()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")

            query_consult = f"""
                SELECT 
                    g.org_zone_id,
                    g.dest_zone_id,
                    g.total_trips as actual_trips,
                    CAST(g.estimated_potential_trips AS INT) as potential,
                    ROUND(g.mismatch_ratio, 4) as mismatch_ratio,
                    ROUND(g.geographic_distance_km, 2) as dist_km
                FROM lakehouse.gold.infrastructure_gaps g
                JOIN lakehouse.silver.dim_zones zo ON g.org_zone_id = zo.zone_id
                JOIN lakehouse.silver.dim_zones zd ON g.dest_zone_id = zd.zone_id
                WHERE 
                    ST_Intersects(zo.polygon, ST_GeomFromText('{input_wkt}'))
                    AND ST_Intersects(zd.polygon, ST_GeomFromText('{input_wkt}'))
                ORDER BY g.mismatch_ratio ASC
                LIMIT 10;
            """
            
            df_gaps = con.execute(query_consult).df()

            if df_gaps.empty:
                logging.warning("⚠️ No infrastructure gaps found within this polygon in Gold data.")
            else:
                logging.info(f"\n🔎 TOP 10 Mismatches in Selected Polygon:\n{df_gaps.to_string(index=False)}")

        finally:
            con.close()

    # --- DAG FLOW ---
    consult_clustering_by_polygon()
    consult_infrastructure_gaps()

mobility_consultations()