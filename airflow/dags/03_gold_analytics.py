from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import logging
import os

# --- CONFIGURATION ---
# Covers Spain entirely including the islands, Ceuta and Melilla
DEFAULT_POLYGON = "POLYGON((-18.5 27.4, -18.5 44.0, 4.5 44.0, 4.5 27.4, -18.5 27.4))"

@dag(
    dag_id="mobility_03_gold_analytics",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="YYYY-MM-DD"),
        "end_date": Param("20231231", type="string", description="YYYY-MM-DD"),
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string", title="Spatial Filter (WKT)", description="Paste your WKT Polygon here.")
    },
    tags=['mobility', 'gold', 'analytics']
)
def mobility_03_gold_analytics():

    @task
    def create_typical_day_cluster(**context):
        """
        Task 1: Clustering Analysis (K-Means)
        Optimization: Combined Spatial Setup + Fetch into 1 block. Combined DDLs + Analysis Select into 1 block.
        """
        logging.info("--- 🏗️ Starting Table 1: Clustering Analysis ---")
        params = context['params']
        input_start_date = params['start_date']
        input_end_date = params['end_date']
        input_polygon_wkt = params['polygon_wkt']

        con = get_connection()
        try:
            # ---------------------------------------------------------
            # Fetch Data for Python
            # ---------------------------------------------------------
            logging.info(f"-> Fetching data ({input_start_date} to {input_end_date})...")
            
            # We chain the commands. .df() returns the result of the LAST statement (the SELECT)
            fetch_script = f"""
                SELECT 
                    m.partition_date AS date,
                    hour(m.period)   AS hour,
                    SUM(m.trips)     AS total_trips
                FROM lakehouse.silver.fact_mobility m
                JOIN lakehouse.silver.dim_zones zo ON m.origin_zone_id = zo.zone_id
                JOIN lakehouse.silver.dim_zones zd ON m.destination_zone_id = zd.zone_id
                WHERE m.trips IS NOT NULL
                  AND m.partition_date BETWEEN '{input_start_date}' AND '{input_end_date}'
                  AND ST_Intersects(zo.polygon, ST_GeomFromText('{input_polygon_wkt}'))
                  AND ST_Intersects(zd.polygon, ST_GeomFromText('{input_polygon_wkt}'))
                GROUP BY 1, 2
                ORDER BY 1, 2;
            """
            df = con.execute(fetch_script).df()

            if df.empty:
                logging.warning("⚠️ No data found. Skipping clustering.")
                return

            # ---------------------------------------------------------
            # PYTHON: In-Memory Processing (Pivot -> K-Means)
            # ---------------------------------------------------------
            # Pivot
            df_pivot = df.pivot(index='date', columns='hour', values='total_trips').fillna(0)
            for h in range(24):
                if h not in df_pivot.columns: df_pivot[h] = 0
            df_pivot = df_pivot.sort_index(axis=1)
            
            # Normalize
            row_sums = df_pivot.sum(axis=1)
            df_normalized = df_pivot.div(row_sums.replace(0, 1), axis=0).fillna(0)

            # Clustering
            n_clusters = 3
            logging.info(f"-> Running K-Means (k={n_clusters})...")
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            clusters = kmeans.fit_predict(df_normalized)

            df_results = pd.DataFrame({
                'date': df_normalized.index,
                'cluster_id': clusters
            })

            # Register DataFrame as a View (Does not count as an execute, it's a memory pointer)
            con.register('view_dim_clusters', df_results)

            # ---------------------------------------------------------
            # EXECUTION 2: Materialize Tables & Return Analysis
            # ---------------------------------------------------------
            logging.info("-> Materializing Tables & Analyzing...")
            
            # We combine the creation of both tables AND the final analysis query into one script.
            # The .df() will return the result of the FINAL Select statement.
            save_and_analyze_script = f"""
                -- 1. Save Assignments
                CREATE OR REPLACE TABLE lakehouse.gold.dim_cluster_assignments AS 
                SELECT * FROM view_dim_clusters;

                -- 2. Save Profiles
                CREATE OR REPLACE TABLE lakehouse.gold.typical_day_by_cluster AS
                WITH dim_mobility_patterns AS (
                    SELECT date, cluster_id FROM view_dim_clusters
                )
                SELECT 
                    p.cluster_id,
                    hour(f.period) as hour,
                    ROUND(AVG(f.trips), 2) as avg_trips,
                    SUM(f.trips) as total_trips_sample,
                    CURRENT_TIMESTAMP as processed_at,
                    '{input_start_date}' as analysis_start,
                    '{input_end_date}' as analysis_end
                FROM lakehouse.silver.fact_mobility f
                JOIN dim_mobility_patterns p ON f.partition_date = p.date
                JOIN lakehouse.silver.dim_zones zo ON f.origin_zone_id = zo.zone_id
                JOIN lakehouse.silver.dim_zones zd ON f.destination_zone_id = zd.zone_id
                WHERE f.partition_date BETWEEN '{input_start_date}' AND '{input_end_date}'
                  AND ST_Intersects(zo.polygon, ST_GeomFromText('{input_polygon_wkt}'))
                  AND ST_Intersects(zd.polygon, ST_GeomFromText('{input_polygon_wkt}'))
                GROUP BY p.cluster_id, hour(f.period)
                ORDER BY p.cluster_id, hour(f.period);

                -- 3. Return Analysis (The return value of the function)
                SELECT cluster_id, COUNT(*) as days, MODE(dayname(date)) as typical_day
                FROM view_dim_clusters GROUP BY cluster_id;
            """
            
            analysis_df = con.execute(save_and_analyze_script).df()
            logging.info(f"📊 Cluster Analysis:\n{analysis_df.to_string(index=False)}")

            # Cleanup
            con.unregister('view_dim_clusters')
            logging.info("✅ Clustering Complete.")

        except Exception as e:
            logging.error(f"❌ Failed in Table 1: {e}")
            raise e
        
        finally:
            con.close()

    @task
    def validate_clusters_vs_calendar():
        """
        Task 2: Validation
        """
        logging.info("--- 🕵️‍♂️ VALIDATION: 3 Clusters vs. Real Calendar ---")
        con = get_connection()
        pd.set_option('display.max_colwidth', None)

        try:
            query_validation = """
            WITH national_holidays AS (
                SELECT DISTINCT holiday_date
                FROM lakehouse.silver.dim_zone_holidays
            ),
            labeled_data AS (
                SELECT 
                    p.cluster_id,
                    p.date,
                    dayname(p.date) as day_of_week,
                    CASE 
                        WHEN h.holiday_date IS NOT NULL THEN 'National Holiday'
                        WHEN dayname(p.date) = 'Sunday' THEN 'Sunday'
                        WHEN dayname(p.date) = 'Saturday' THEN 'Saturday'
                        ELSE 'Weekday (Mon-Fri)'
                    END as real_category
                FROM lakehouse.gold.dim_cluster_assignments p
                LEFT JOIN national_holidays h ON p.date = h.holiday_date
            )
            SELECT 
                cluster_id,
                real_category,
                COUNT(*) as total_days,
                list(strftime(date, '%Y-%m-%d') ORDER BY date) as specific_dates
            FROM labeled_data
            GROUP BY cluster_id, real_category
            ORDER BY cluster_id, total_days DESC;
            """
            
            df_val = con.execute(query_validation).df()

            if df_val.empty:
                logging.warning("⚠️ Validation table empty.")
            else:
                logging.info(f"\n{df_val.to_string(index=False)}")
                
        finally:
            con.close()

    @task
    def create_infrastructure_gaps(**context):
        """
        Task 3: Infrastructure Gaps (Gravity Model)
        """
        logging.info("--- 🏗️ Starting Table 2: Infrastructure Gaps ---")
        params = context['params']
        input_start_date = params['start_date']
        input_end_date = params['end_date']
        input_polygon_wkt = params['polygon_wkt']

        con = get_connection()
        try:
            logging.info(f"-> Executing Gravity Model ({input_start_date} to {input_end_date})...")

            # We create one massive SQL script
            full_script = f"""
                CREATE OR REPLACE TABLE lakehouse.gold.infrastructure_gaps AS
                WITH od_pairs AS (
                    SELECT
                        origin_zone_id,
                        destination_zone_id,
                        SUM(trips) AS total_actual_trips
                    FROM lakehouse.silver.fact_mobility
                    WHERE partition_date BETWEEN '{input_start_date}' AND '{input_end_date}'
                    GROUP BY 1, 2
                ),
                unique_rent AS (
                    SELECT zone_id, income_per_capita AS rent
                    FROM lakehouse.silver.metric_ine_rent
                    WHERE year = 2023
                ),
                model_calculation AS (
                    SELECT
                        m.origin_zone_id AS org_zone_id,
                        m.destination_zone_id AS dest_zone_id,
                        p.population AS total_population,               -- P_i
                        r.rent,                                         -- E_j
                        m.total_actual_trips AS total_trips,            -- Actual trips
                        
                        -- Calculate distance (Spheroid)
                        GREATEST(
                            0.5, 
                            st_distance_spheroid(
                                ST_Centroid(z_org.polygon), 
                                ST_Centroid(z_dest.polygon)
                            ) / 1000 
                        ) AS geographic_distance_km
                            
                    FROM od_pairs AS m
                    JOIN lakehouse.silver.metric_population AS p ON m.origin_zone_id = p.zone_id
                    JOIN unique_rent AS r ON m.destination_zone_id = r.zone_id
                    JOIN lakehouse.silver.dim_zones as z_org ON m.origin_zone_id = z_org.zone_id
                    JOIN lakehouse.silver.dim_zones as z_dest ON m.destination_zone_id = z_dest.zone_id

                    WHERE p.population > 0 
                      AND r.rent > 0
                      AND z_org.polygon IS NOT NULL
                      AND z_dest.polygon IS NOT NULL
                      AND m.origin_zone_id != m.destination_zone_id 
                      -- SPATIAL FILTER
                      AND ST_Intersects(z_org.polygon, ST_GeomFromText('{input_polygon_wkt}'))
                      AND ST_Intersects(z_dest.polygon, ST_GeomFromText('{input_polygon_wkt}'))
                )
                SELECT
                    org_zone_id,
                    dest_zone_id,
                    total_trips,
                    total_population,
                    rent,
                    geographic_distance_km,
                    -- Gravity Model Calculation
                    (1.0 * (CAST(total_population AS DOUBLE) * CAST(rent AS DOUBLE))) / 
                    (geographic_distance_km * geographic_distance_km) AS estimated_potential_trips,
                    -- Mismatch Ratio
                    total_trips / NULLIF(estimated_potential_trips, 0) AS mismatch_ratio,
                    CURRENT_TIMESTAMP as processed_at
                FROM model_calculation;
            """
            
            con.execute(full_script)
            logging.info("✅ Table 'lakehouse.gold.infrastructure_gaps' created.")

        finally:
            con.close()
        
    @task
    def verify_infrastructure_gaps():
        """
        Task 4: Verification
        """
        logging.info("--- 🔎 Verification: Infrastructure Gaps ---")
        con = get_connection()
        pd.set_option('display.max_colwidth', None)
        
        try:
            verification_bq2 = """
                SELECT 
                    org_zone_id,
                    dest_zone_id,
                    total_trips,
                    estimated_potential_trips,
                    geographic_distance_km,
                    mismatch_ratio
                FROM lakehouse.gold.infrastructure_gaps
                WHERE total_trips > 10
                ORDER BY mismatch_ratio ASC
                LIMIT 10;
            """
            
            logging.info("Querying Top 10 Mismatches...")
            df_verify = con.execute(verification_bq2).df()
            
            if df_verify.empty:
                logging.warning("⚠️ Verification returned no rows (maybe no trips > 10?).")
            else:
                logging.info(f"\n{df_verify.to_string(index=False)}")
                
        finally:
            con.close()

    # --- DAG FLOW ---
    # Branch 1: Clustering
    t1_clustering = create_typical_day_cluster()
    t3_validate = validate_clusters_vs_calendar()

    # Branch 2: Infrastructure
    t2_gaps = create_infrastructure_gaps()
    t5_verify = verify_infrastructure_gaps()

    # Dependencies
    t1_clustering >> t3_validate
    t2_gaps >> t5_verify

mobility_03_gold_analytics()