from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
from sklearn.cluster import KMeans # Added dependency
import matplotlib.pyplot as plt
import logging
import os

# --- CONFIGURATION ---
DEFAULT_POLYGON = "POLYGON((-7.55 36.0, -7.55 38.8, -1.0 38.8, -1.0 36.0, -7.55 36.0))"

@dag(
    dag_id="mobility_consultations_fixed_v2",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="YYYY-MM-DD"),
        "end_date": Param("20230131", type="string", description="YYYY-MM-DD"),
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string", title="Spatial Filter (WKT)", description="Paste WKT to filter the consultation.")
    },
    tags=['mobility', 'gold', 'consultation']
)
def mobility_consultations_fixed_v2():

    @task
    def consult_clustering_by_polygon(**context):
        """
        Consultation 1: Spatial Clustering Patterns
        
        Strategy:
        Since the Gold table doesn't store WHICH date belongs to WHICH cluster,
        we must re-run the K-Means logic here (using the same random_state) 
        to recover the classifications before analyzing the specific polygon.
        """
        logging.info("--- 📊 Consulting: Polygon Specific Clustering ---")
        params = context['params']
        start_date = params['start_date']
        end_date = params['end_date']
        input_wkt = params['polygon_wkt']

        con = get_connection()
        try:

            # --- STEP 1: RECONSTRUCT CLUSTER ASSIGNMENTS ---
            # We fetch global totals to replicate the 'create_gold_clustering' logic
            logging.info(" 🔄 Re-calculating cluster assignments (K-Means)...")
            query_global = """
                SELECT 
                    partition_date AS date,
                    hour(period)   AS hour,
                    SUM(trips)     AS total_trips
                FROM lakehouse.silver.fact_mobility
                WHERE trips IS NOT NULL
                GROUP BY 1, 2
            """
            df_global = con.execute(query_global).df()
            
            if df_global.empty:
                logging.warning("⚠️ No global data found to build clusters.")
                return

            # Replicate Pivot & Normalize Logic
            df_pivot = df_global.pivot(index='date', columns='hour', values='total_trips').fillna(0)
            for h in range(24): # Ensure 0-23 cols
                if h not in df_pivot.columns: df_pivot[h] = 0
            df_pivot = df_pivot.sort_index(axis=1)
            
            # Normalize
            row_sums = df_pivot.sum(axis=1)
            df_normalized = df_pivot.div(row_sums, axis=0).fillna(0)

            # Run K-Means (Must use same params as Creation Task: k=3, seed=42)
            kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
            clusters = kmeans.fit_predict(df_normalized)

            # Create the mapping dataframe
            df_mapping = pd.DataFrame({
                'date': df_normalized.index,
                'cluster_id': clusters
            })
            
            # Register as View for the next query
            con.register('view_recalculated_clusters', df_mapping)


            # --- STEP 2: CONSULT SPECIFIC POLYGON ---
            # Now we query Silver data filtered by the Polygon and join our Mapping
            query_consult = f"""
                WITH silver_activity AS (
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
                )
                SELECT 
                    c.cluster_id,
                    s.hour,
                    AVG(s.local_trips) as avg_trips,
                    COUNT(DISTINCT s.date) as days_in_sample
                FROM silver_activity s
                JOIN view_recalculated_clusters c ON s.date = c.date
                GROUP BY c.cluster_id, s.hour
                ORDER BY c.cluster_id, s.hour
            """

            df_result = con.execute(query_consult).df()

            if df_result.empty:
                logging.warning("⚠️ No data found (Check Polygon overlap).")
                return

            logging.info(f"Generated profiles based on {df_result['days_in_sample'].max()} days of data.")

            # --- VISUALIZATION ---
            fig, ax = plt.subplots(figsize=(12, 6))
            unique_clusters = df_result['cluster_id'].unique()
            for cid in unique_clusters:
                subset = df_result[df_result['cluster_id'] == cid]
                ax.plot(subset['hour'], subset['avg_trips'], marker='o', label=f'Cluster {cid}')

            ax.set_title(f'Average Daily Profile for Input Polygon\n(Split by Re-calculated Clusters)')
            ax.set_xlabel('Hour of Day')
            ax.set_ylabel('Average Trips (Inside Polygon)')
            ax.grid(True, linestyle='--', alpha=0.5)
            ax.legend(title="Cluster ID")
            
            output_dir = "consultation_results"
            os.makedirs(output_dir, exist_ok=True)
            save_path = os.path.join(output_dir, f"polygon_cluster_profile_{start_date}.png")
            plt.savefig(save_path)
            plt.close(fig)
            logging.info(f"✅ Plot saved to: {save_path}")

        except Exception as e:
            logging.error(f"Error in spatial clustering consultation: {e}")
            raise e
        finally:
            con.close()

    @task
    def consult_infrastructure_gaps(**context):
        """
        Consultation 2: Infrastructure Gaps
        """
        logging.info("--- 🏗️ Consulting: Spatial Infrastructure Gaps ---")
        params = context['params']
        input_wkt = params['polygon_wkt']

        con = get_connection()
        try:

            query_consult = f"""
                SELECT 
                    g.origin_zone_id,
                    g.destination_zone_id,
                    g.total_actual_trips, 
                    CAST(g.potential AS INT) as potential,
                    ROUND(g.mismatch_ratio, 4) as mismatch_ratio,
                    -- Recalculate distance on the fly
                    ROUND(GREATEST(0.5, st_distance_spheroid(ST_Centroid(zo.polygon), ST_Centroid(zd.polygon))/1000), 2) AS dist_km
                FROM lakehouse.gold.infrastructure_gaps g
                JOIN lakehouse.silver.dim_zones zo ON g.origin_zone_id = zo.zone_id
                JOIN lakehouse.silver.dim_zones zd ON g.destination_zone_id = zd.zone_id
                WHERE 
                    ST_Intersects(zo.polygon, ST_GeomFromText('{input_wkt}'))
                    AND ST_Intersects(zd.polygon, ST_GeomFromText('{input_wkt}'))
                ORDER BY g.mismatch_ratio ASC
                LIMIT 10;
            """
            
            df_gaps = con.execute(query_consult).df()

            if df_gaps.empty:
                logging.warning("⚠️ No infrastructure gaps found within this polygon.")
            else:
                logging.info(f"\n🔎 TOP 10 Mismatches in Selected Polygon:\n{df_gaps.to_string(index=False)}")

        finally:
            con.close()

    # --- DAG FLOW ---
    consult_clustering_by_polygon()
    consult_infrastructure_gaps()

mobility_consultations_fixed_v2()