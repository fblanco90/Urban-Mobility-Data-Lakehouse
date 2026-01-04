from airflow.sdk import dag, task, Param
from pendulum import datetime
from utils_db import get_connection, run_batch_sql
import pandas as pd
from sklearn.cluster import KMeans
import logging

@dag(
    dag_id="3_gold_analytics",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="YYYYMMDD"),
        "end_date": Param("20230101", type="string", description="YYYYMMDD"),
    },
    tags=['mobility', 'gold', 'analytics', 'aws_batch']
)
def gold_analytics():

    # --- 1. INFRASTRUCTURE GAPS (AWS BATCH) ---
    sql_gaps = """
        CREATE OR REPLACE TABLE gold.infrastructure_gaps AS
        WITH trips_aggregated AS (
            SELECT origin_zone_id AS o, destination_zone_id AS d, SUM(trips) AS total_trips
            FROM silver.fact_mobility
            WHERE partition_date BETWEEN strptime('{{ params.start_date }}', '%Y%m%d')::DATE 
                                    AND strptime('{{ params.end_date }}', '%Y%m%d')::DATE
            GROUP BY 1, 2
        ),
        potential_calc AS (
            SELECT
                t.o AS org_zone_id, 
                t.d AS dest_zone_id, 
                t.total_trips, 
                m1.population AS total_population,
                m2.income_per_capita AS rent,
                dist.dist_km,
                (m1.population * m2.income_per_capita) / (dist.dist_km * dist.dist_km) AS gravity_score
            FROM trips_aggregated t
            JOIN silver.dim_zone_distances dist ON t.o = dist.origin_zone_id AND t.d = dist.destination_zone_id
            JOIN silver.metric_population m1 ON t.o = m1.zone_id
            JOIN silver.metric_ine_rent m2 ON t.d = m2.zone_id
            WHERE m2.year = 2023
        ),
        norm AS (
            SELECT SUM(total_trips) / NULLIF(SUM(gravity_score), 0) as ratio 
            FROM potential_calc
        )
        SELECT
            org_zone_id, 
            dest_zone_id, 
            total_population, 
            rent,
            total_trips, 
            dist_km,
            total_trips / NULLIF(gravity_score * (SELECT ratio FROM norm), 0) AS mismatch_ratio
        FROM potential_calc;
        """

    task_batch_gaps = run_batch_sql(
        task_id="batch_infrastructure_gaps", 
        sql_query=sql_gaps, 
        memory="16GB"
    )

    # --- 2. TYPICAL DAY PATTERNS - AGGREGATION (AWS BATCH) ---
    sql_profiles = """
    CREATE OR REPLACE TABLE silver.tmp_gold_profiles_agg AS
    SELECT
        partition_date,
        hour(period) as hour,
        SUM(trips) as total_trips
    FROM silver.fact_mobility
    WHERE partition_date BETWEEN strptime('{{ params.start_date }}', '%Y%m%d')::DATE 
                             AND strptime('{{ params.end_date }}', '%Y%m%d')::DATE
    GROUP BY 1, 2;
    """

    task_batch_profiles = run_batch_sql(
        task_id="batch_prepare_profiles", 
        sql_query=sql_profiles, 
        memory="12GB"
    )

    # --- 3. TYPICAL DAY PATTERNS - K-MEANS (LOCAL AIRFLOW) ---
    @task
    def process_gold_patterns_locally():
        # Step A: Fetch data from the table Batch just created
        with get_connection() as con:
            df_raw = con.execute("SELECT * FROM lakehouse.silver.tmp_gold_profiles_agg").df()
        
        if df_raw.empty:
            logging.warning("No data found in tmp_gold_profiles_agg")
            return

        # Step B: Run K-Means
        df_pivot = df_raw.pivot(index='partition_date', columns='hour', values='total_trips').fillna(0)
        norm_data = df_pivot.div(df_pivot.sum(axis=1).replace(0, 1), axis=0)
        kmeans = KMeans(n_clusters=3, random_state=42, n_init='auto').fit(norm_data)
        
        df_clusters = pd.DataFrame({'partition_date': df_pivot.index, 'cluster_id': kmeans.labels_})
        df_final = df_raw.merge(df_clusters, on='partition_date')
        
        gold_table = df_final.groupby(['cluster_id', 'hour'])['total_trips'].mean().reset_index()
        gold_table.columns = ['cluster_id', 'hour', 'avg_trips']
        gold_table['processed_at'] = pd.Timestamp.now()

        # Step C: Save to Gold
        with get_connection() as con:
            con.register('tmp_gold_patterns', gold_table)
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.gold.typical_day_patterns AS
                SELECT cluster_id::INT as cluster_id, hour::INT as hour, 
                       avg_trips::DOUBLE as avg_trips, processed_at::TIMESTAMP as processed_at
                FROM tmp_gold_patterns;
            """)
            con.register('tmp_assignments', df_clusters)
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.gold.dim_cluster_assignments AS 
                SELECT partition_date, cluster_id FROM tmp_assignments;
            """)

    @task
    def validate_and_cleanup():
        with get_connection() as con:
            # Validation
            df_val = con.execute("""
                SELECT cluster_id, dayname(partition_date) as day, COUNT(*) as count
                FROM lakehouse.gold.dim_cluster_assignments
                GROUP BY 1, 2 ORDER BY 1, 3 DESC;
            """).df()
            logging.info(f"Validation:\n{df_val.to_string()}")
            
            # Cleanup intermediate table
            con.execute("DROP TABLE IF EXISTS silver.tmp_gold_profiles_agg;")
            con.execute("DROP TABLE IF EXISTS gold.dim_cluster_assignments;")

    # --- ORCHESTRATION ---
    task_batch_profiles >> process_gold_patterns_locally() >> validate_and_cleanup()

gold_analytics()
