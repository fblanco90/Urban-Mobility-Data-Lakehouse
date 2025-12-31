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
    params={
        "start_date": Param("20230101", type="string", description="YYYYMMDD"),
        "end_date": Param("20230101", type="string", description="YYYYMMDD"),
    },
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

    @task
    def generate_date_list(**context):
        """Generates list of YYYY-MM-DD strings for SQL."""
        conf = context['dag_run'].conf or {}
        params = context['params']
        start = conf.get('start_date') or params['start_date']
        end = conf.get('end_date') or params['end_date']
        
        # Produce dates as 'YYYY-MM-DD'
        return [d.strftime("%Y-%m-%d") for d in pd.date_range(start=start, end=end)]


    @task
    def create_tmp_actuals_table():
        """Temporary table for daily aggregation."""
        with get_connection() as con:
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.silver.tmp_actuals_daily (
                    o BIGINT,
                    d BIGINT,
                    partition_date DATE,
                    t BIGINT
                );
            """)
        logging.info("✅ Temporary table 'tmp_actuals_daily' created.")

    @task(
        max_active_tis_per_dag=4, 
        retries=5,
        retry_delay=timedelta(seconds=30),
    )
    def aggregate_actuals_for_day(single_date: str):
        """Aggregate trips for a single day into tmp_actuals_daily."""
        with get_connection() as con:
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
        logging.info(f"✅ Aggregated trips for {single_date}.")

    @task
    def create_metrics_and_rents_tables():
        """Create smaller reference tables for metrics and rents."""
        with get_connection() as con:
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
        logging.info("✅ Metrics and rents tables created.")

    @task
    def compute_potential():
        """Compute potential trips for all tmp_actuals_daily."""
        with get_connection() as con:
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
        logging.info("✅ Potential trips calculated.")

    @task
    def compute_mismatch_ratio_and_create_gold():
        """Create final gold table with mismatch ratios."""
        with get_connection() as con:
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
        logging.info("✅ Gold table 'infrastructure_gaps' created.")

    @task
    def cleanup_intermediate_tables():
        """Drop temporary tables we no longer need."""
        with get_connection() as con:
            con.execute("DROP TABLE IF EXISTS lakehouse.silver.tmp_actuals_daily;")
            con.execute("DROP TABLE IF EXISTS lakehouse.silver.metrics_daily;")
            con.execute("DROP TABLE IF EXISTS lakehouse.silver.rents_daily;")
            con.execute("DROP TABLE IF EXISTS lakehouse.silver.potential_daily;")
        logging.info("🗑 Intermediate tables cleaned up.")

    # ==========================
    # DAG Orchestration
    # ==========================
    dates_list = generate_date_list()
    tmp_table = create_tmp_actuals_table()
    metrics_rents = create_metrics_and_rents_tables()

    # Map one task per date
    daily_aggregates = aggregate_actuals_for_day.expand(single_date=dates_list)

    potential_calc = compute_potential()
    gold_table = compute_mismatch_ratio_and_create_gold()
    cleanup = cleanup_intermediate_tables()

    # ==========================
    # Dependencies
    # ==========================
    tmp_table >> daily_aggregates
    metrics_rents >> potential_calc
    daily_aggregates >> potential_calc
    potential_calc >> gold_table >> cleanup



    # t1_clustering >> t2_gaps

gold_analytics()