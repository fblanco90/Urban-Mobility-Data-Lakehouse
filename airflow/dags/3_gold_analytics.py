from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import logging
import os
from datetime import date, timedelta


@dag(
    dag_id="3_gold_analytics",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['mobility', 'gold', 'analytics']
)
def gold_analytics():

    # @task
    # def create_typical_day_cluster(**context):
    #     """
    #     Task 1: Clustering Analysis (K-Means)
    #     Optimization: Combined Spatial Setup + Fetch into 1 block. Combined DDLs + Analysis Select into 1 block.
    #     """
    #     logging.info("--- 🏗️ Starting Table 1: Clustering Analysis ---")

    #     con = get_connection()
    #     try:
    #         # ---------------------------------------------------------
    #         # Fetch Data for Python
    #         # ---------------------------------------------------------
    #         logging.info(f"-> Fetching data from Silver...")
            
    #         # We chain the commands. .df() returns the result of the LAST statement (the SELECT)
    #         con.execute("""SET s3_uploader_max_parts_per_file = 100;""")
    #         con.execute("""SET s3_url_style = 'path';""")
    #         fetch_script = f"""
    #             SELECT 
    #                 m.partition_date AS date,
    #                 hour(m.period)   AS hour,
    #                 SUM(m.trips)     AS total_trips
    #             FROM lakehouse.silver.fact_mobility m
    #             GROUP BY 1, 2
    #             ORDER BY 1, 2;
    #         """
    #         df = con.execute(fetch_script).df()
            
    #     finally:
    #         con.close()

    #     if df.empty:
    #         return

    #         # ---------------------------------------------------------
    #         # PYTHON: In-Memory Processing (Pivot -> K-Means)
    #         # ---------------------------------------------------------
    #         # Pivot
    #     df_pivot = df.pivot(index='date', columns='hour', values='total_trips').fillna(0)
    #     for h in range(24):
    #         if h not in df_pivot.columns: df_pivot[h] = 0
    #     df_pivot = df_pivot.sort_index(axis=1)
            
    #         # Normalize
    #     row_sums = df_pivot.sum(axis=1)
    #     df_normalized = df_pivot.div(row_sums.replace(0, 1), axis=0).fillna(0)

    #     # Clustering
    #     n_clusters = 3
    #     logging.info(f"-> Running K-Means (k={n_clusters})...")
    #     kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    #     clusters = kmeans.fit_predict(df_normalized)

    #     df_results = pd.DataFrame({
    #             'date': df_normalized.index,
    #             'cluster_id': clusters
    #     })

    #         # Register DataFrame as a View (Does not count as an execute, it's a memory pointer)
    #     con_gold=get_connection
    #     try:
    #         con_gold.register('view_dim_clusters', df_results)

    #             # ---------------------------------------------------------
    #             # EXECUTION 2: Materialize Tables & Return Analysis
    #             # ---------------------------------------------------------
    #         logging.info("-> Materializing Tables & Analyzing...")
                
    #             # We combine the creation of both tables AND the final analysis query into one script.
    #             # The .df() will return the result of the FINAL Select statement.
    #         save_and_analyze_script = f"""
    #                 -- 1. Save Assignments
    #                 CREATE OR REPLACE TABLE lakehouse.gold.dim_cluster_assignments AS 
    #                 SELECT * FROM view_dim_clusters;

    #                 -- 2. Save Profiles
    #                 CREATE OR REPLACE TABLE lakehouse.gold.typical_day_by_cluster AS
    #                 WITH dim_mobility_patterns AS (
    #                     SELECT date, cluster_id FROM view_dim_clusters
    #                 )
    #                 SELECT 
    #                     p.cluster_id,
    #                     hour(f.period) as hour,
    #                     ROUND(AVG(f.trips), 2) as avg_trips,
    #                     SUM(f.trips) as total_trips_sample,
    #                     CURRENT_TIMESTAMP as processed_at,
    #                 FROM lakehouse.silver.fact_mobility f
    #                 JOIN dim_mobility_patterns p ON f.partition_date = p.date
    #                 JOIN lakehouse.silver.dim_zones zo ON f.origin_zone_id = zo.zone_id
    #                 JOIN lakehouse.silver.dim_zones zd ON f.destination_zone_id = zd.zone_id
    #                 GROUP BY p.cluster_id, hour(f.period)
    #                 ORDER BY p.cluster_id, hour(f.period);

    #                 -- 3. Return Analysis (The return value of the function)
    #                 SELECT cluster_id, COUNT(*) as days, MODE(dayname(date)) as typical_day
    #                 FROM view_dim_clusters GROUP BY cluster_id;
    #             """
                
    #         analysis_df = con_gold.execute(save_and_analyze_script).df()
    #         logging.info(f"📊 Cluster Analysis:\n{analysis_df.to_string(index=False)}")

    #             # Cleanup
    #         con_gold.unregister('view_dim_clusters')
    #         logging.info("✅ Clustering Complete.")

    #     except Exception as e:
    #         logging.error(f"❌ Failed in Table 1: {e}")
    #         raise e
        
    #     finally:
    #         con_gold.close()

    @task(execution_timeout=None)
    def create_infrastructure_gaps(**context):
        """
        Infrastructure Gaps (Gravity Model) – daily chunked using partition_date
        """
        logging.info("--- 🏗️ Starting Table 2: Infrastructure Gaps (daily) ---")

        # Date range for processing – can come from DAG params or config
        start_date = date(2023, 1, 1)
        end_date   = date(2023, 12, 31)

        all_dates = []
        current = start_date
        while current <= end_date:
            all_dates.append(current)
            current += timedelta(days=1)

        with get_connection() as con:
            # DuckLake / Neon settings
            con.execute("SET http_keep_alive=true;")
            con.execute("SET threads = 4;")

            # Step 0 – temporary table for daily aggregation
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.silver.tmp_actuals_daily (
                    o BIGINT,
                    d BIGINT,
                    partition_date DATE,
                    t BIGINT
                );
            """)

            logging.info(f"📊 Step 1/4: Aggregating actual trips by day ({len(all_dates)} days)...")

            for single_date in all_dates:
                logging.info(f"Processing {single_date} ...")
                con.execute(f"""
                    INSERT INTO lakehouse.silver.tmp_actuals_daily
                    SELECT
                        origin_zone_id AS o,
                        destination_zone_id AS d,
                        partition_date,
                        SUM(trips) AS t
                    FROM lakehouse.silver.fact_mobility
                    WHERE partition_date = DATE '{single_date}'
                    GROUP BY 1,2,3;
                """)

            logging.info("📊 Step 2/4: Load metrics and rents ...")
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.silver.metrics_daily AS
                SELECT zone_id AS id, population AS pop
                FROM lakehouse.silver.metric_population;
            """)

            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.silver.rents_daily AS
                SELECT zone_id AS id, income_per_capita AS r
                FROM lakehouse.silver.metric_ine_rent
                WHERE year = 2023;
            """)

            logging.info("📊 Step 3/4: Compute potential trips ...")
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.silver.potential_daily AS
                SELECT
                    act.o AS org_zone_id,
                    act.d AS dest_zone_id,
                    act.partition_date,
                    act.t AS total_trips,
                    m1.pop AS total_population,
                    m2.r AS rent,
                    dist.dist_km,
                    (1.0 * (m1.pop * m2.r)) / (dist.dist_km * dist.dist_km) AS potential
                FROM lakehouse.silver.tmp_actuals_daily act
                JOIN lakehouse.silver.metrics_daily m1 ON act.o = m1.id
                JOIN lakehouse.silver.rents_daily   m2 ON act.d = m2.id
                JOIN lakehouse.silver.dim_zone_distances dist
                ON act.o = dist.origin_zone_id
                AND act.d = dist.destination_zone_id;
            """)

            logging.info("📊 Step 4/4: Compute mismatch ratio and save final table ...")
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.gold.infrastructure_gaps AS
                SELECT
                    org_zone_id,
                    dest_zone_id,
                    partition_date,
                    total_trips,
                    total_population,
                    rent,
                    dist_km,
                    potential,
                    total_trips / NULLIF(potential, 0) AS mismatch_ratio
                FROM lakehouse.silver.potential_daily;
            """)

        logging.info("✅ Table 'lakehouse.gold.infrastructure_gaps' created.")




    # --- DAG FLOW ---
    # Branch 1: Clustering
    # t1_clustering = create_typical_day_cluster()

    # Branch 2: Infrastructure
    t2_gaps = create_infrastructure_gaps()

    # Dependencies
    t2_gaps
    # t1_clustering >> t2_gaps

gold_analytics()