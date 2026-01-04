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
    def create_tmp_profiles_table():
        """Creates a temporary table in Silver for daily hourly profiles."""
        with get_connection() as con:
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.silver.tmp_daily_profiles (
                    partition_date DATE,
                    hour INTEGER,
                    total_trips DOUBLE
                );
            """)
        logging.info("✅ Temporary profiles table created.")

    @task(max_active_tis_per_dag=4,
        retries=5,
        retry_delay=timedelta(seconds=30),
    )
    def aggregate_profile_for_day(single_date: str):
        """Calculates the 24-hour profile for one specific day."""
        with get_connection() as con:
            con.execute(f"""
                INSERT INTO lakehouse.silver.tmp_daily_profiles
                SELECT
                    partition_date,
                    hour(period) as hour,
                    SUM(trips) as total_trips
                FROM lakehouse.silver.fact_mobility
                WHERE partition_date = DATE '{single_date}'
                GROUP BY 1, 2;
            """)
        logging.info(f"✅ Profile aggregated for {single_date}.")

    @task
    def run_kmeans_on_profiles():
        """Fetches the small aggregated table and performs K-Means in Python."""
        con = get_connection()
        try:
            # 1. Fetch only the aggregated 8,760 rows
            df = con.execute("""
                PIVOT lakehouse.silver.tmp_daily_profiles 
                ON hour IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)
                USING SUM(total_trips) GROUP BY partition_date;
            """).df().fillna(0)
            
            # 2. Normalize and Clustering (K-Means)
            data = df.drop(columns=['partition_date'])
            norm_data = data.div(data.sum(axis=1).replace(0, 1), axis=0)
            
            kmeans = KMeans(n_clusters=3, random_state=42).fit(norm_data)
            df['cluster_id'] = kmeans.labels_
            
            # 3. Save assignments back to Gold
            con.register('temp_assignments', df[['partition_date', 'cluster_id']])
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.gold.dim_cluster_assignments AS 
                SELECT * FROM temp_assignments;
            """)
        finally:
            con.close()

    @task
    def create_gold_typical_days():
        """Materializes the final average hourly patterns for business experts."""
        with get_connection() as con:
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.gold.typical_day_patterns AS
                SELECT 
                    a.cluster_id,
                    p.hour,
                    AVG(p.total_trips) as avg_trips,
                    CURRENT_TIMESTAMP as processed_at
                FROM lakehouse.silver.tmp_daily_profiles p
                JOIN lakehouse.gold.dim_cluster_assignments a ON p.partition_date = a.partition_date
                GROUP BY 1, 2;
            """)
    
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
                    p.partition_date,
                    dayname(p.partition_date) as day_of_week,
                    CASE 
                        WHEN h.holiday_date IS NOT NULL THEN 'National Holiday'
                        WHEN dayname(p.partition_date) = 'Sunday' THEN 'Sunday'
                        WHEN dayname(p.partition_date) = 'Saturday' THEN 'Saturday'
                        ELSE 'Weekday (Mon-Fri)'
                    END as real_category
                FROM lakehouse.gold.dim_cluster_assignments p
                LEFT JOIN national_holidays h ON p.partition_date = h.holiday_date
            )
            SELECT 
                cluster_id,
                real_category,
                COUNT(*) as total_days,
                list(strftime(partition_date, '%Y-%m-%d') ORDER BY partition_date) as specific_dates
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
    def cleanup_intermediate_tables():
        """Drop temporary tables we no longer need."""
        with get_connection() as con:
            con.execute("DROP TABLE IF EXISTS lakehouse.silver.tmp_daily_profiles;")
            con.execute("DROP TABLE IF EXISTS lakehouse.gold.dim_cluster_assignments;")
        logging.info("🗑 Intermediate tables cleaned up.")

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
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.silver.tmp_actuals (
                    o BIGINT,
                    d BIGINT,
                    t BIGINT
                );
            """)
        logging.info("✅ Temporary table 'tmp_actuals_daily' created.")

    @task(max_active_tis_per_dag=4, 
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
    def aggregate_all_trips():
        with get_connection() as con:
            con.execute(f"""
                INSERT INTO lakehouse.silver.tmp_actuals
                SELECT
                    o, d,
                    SUM(t) AS total_trips
                FROM lakehouse.silver.tmp_actuals_daily
                GROUP BY 1,2;
                """)
        logging.info(f"✅ Aggregated trips.")

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
        """Compute potential trips for all tmp_actuals."""
        with get_connection() as con:
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.silver.potential AS
                SELECT
                    act.o AS org_zone_id,
                    act.d AS dest_zone_id,
                    act.t AS total_trips,
                    m1.pop AS total_population,
                    m2.r AS rent,
                    dist.dist_km,
                    (m1.pop * m2.r) / (dist.dist_km * dist.dist_km) AS gravity_score
                FROM lakehouse.silver.tmp_actuals act
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
                    total_trips,
                    dist_km,
                    total_trips / NULLIF(
                        gravity_score * (
                            SELECT SUM(total_trips) / SUM(gravity_score) FROM lakehouse.silver.potential
                        ),
                        0
                    ) AS mismatch_ratio
                FROM lakehouse.silver.potential;
            """)
        logging.info("✅ Gold table 'infrastructure_gaps' created.")

    @task
    def cleanup_intermediate_tables_gaps():
        """Drop temporary tables we no longer need."""
        with get_connection() as con:
            con.execute("DROP TABLE IF EXISTS lakehouse.silver.tmp_actuals_daily;")
            con.execute("DROP TABLE IF EXISTS lakehouse.silver.tmp_actuals;")
            con.execute("DROP TABLE IF EXISTS lakehouse.silver.metrics_daily;")
            con.execute("DROP TABLE IF EXISTS lakehouse.silver.rents_daily;")
            con.execute("DROP TABLE IF EXISTS lakehouse.silver.potential;")
        logging.info("Intermediate tables cleaned up.")


    # ==============================================================================
    # ORCHESTRATION FLOW
    # ==============================================================================

    dates_list = generate_date_list()

    # Map one task per date
    init_patterns = create_tmp_profiles_table()
    agg_patterns = aggregate_profile_for_day.expand(single_date=dates_list)

    init_gaps = create_tmp_actuals_table()
    agg_gaps = aggregate_actuals_for_day.expand(single_date=dates_list)
    metrics = create_metrics_and_rents_tables()

    # Table 1: Typical Day Patterns via K-Means
    dates_list >> init_patterns >> agg_patterns >> run_kmeans_on_profiles() >> create_gold_typical_days() >> validate_clusters_vs_calendar() >> cleanup_intermediate_tables()

    # Table 2: Infrastructure Gaps Analysis
    [dates_list, metrics] >> init_gaps >> agg_gaps >> aggregate_all_trips() >> compute_potential() >> compute_mismatch_ratio_and_create_gold() >> cleanup_intermediate_tables_gaps()

gold_analytics()
