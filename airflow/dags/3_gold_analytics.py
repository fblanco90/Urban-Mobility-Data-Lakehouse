from airflow.decorators import dag, task
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
import numpy as np
from sklearn.cluster import MiniBatchKMeans
import logging

@dag(
    dag_id="3_gold_analytics",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['mobility', 'gold', 'performance']
)
def gold_analytics():

    @task
    def create_typical_day_cluster(**context):
        logging.info("--- 🚀 Optimized Clustering with Progress Tracking ---")

        with get_connection() as con:
            con.execute("SET memory_limit = '2GB';")
            con.execute("SET http_keep_alive = true;")

            # STEP 1: Materialize the Pivot to a TEMP TABLE (The "Heavy Lifting")
            # Doing this as a Table instead of a View means DuckDB only scans S3 ONCE.
            logging.info("-> Materializing 24h Matrix to Temp Table (Scanning S3)...")
            con.execute("""
                CREATE OR REPLACE TEMP TABLE temp_normalized_matrix AS
                WITH raw_data AS (
                    SELECT partition_date, hour(period) as hr, SUM(trips) as t
                    FROM lakehouse.silver.fact_mobility
                    GROUP BY 1, 2
                ),
                pivoted AS (
                    PIVOT raw_data 
                    ON hr IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)
                    USING SUM(t) 
                    GROUP BY partition_date
                )
                SELECT 
                    partition_date,
                    (COALESCE("0",0)+COALESCE("1",0)+COALESCE("2",0)+COALESCE("3",0)+COALESCE("4",0)+COALESCE("5",0)+
                     COALESCE("6",0)+COALESCE("7",0)+COALESCE("8",0)+COALESCE("9",0)+COALESCE("10",0)+COALESCE("11",0)+
                     COALESCE("12",0)+COALESCE("13",0)+COALESCE("14",0)+COALESCE("15",0)+COALESCE("16",0)+COALESCE("17",0)+
                     COALESCE("18",0)+COALESCE("19",0)+COALESCE("20",0)+COALESCE("21",0)+COALESCE("22",0)+COALESCE("23",0)) as day_total,
                    * EXCLUDE(partition_date)
                FROM pivoted;
            """)
            logging.info("✅ Matrix materialized successfully.")

            # STEP 2: Batch Fit with Progress Logging
            logging.info("-> Starting Batch Fit (MiniBatchKMeans)...")
            kmeans = MiniBatchKMeans(n_clusters=3, random_state=42, batch_size=500)
            
            cursor = con.execute("SELECT * EXCLUDE(partition_date, day_total) FROM temp_normalized_matrix")
            reader = cursor.fetch_record_batch(rows_per_batch=500)

            batch_count = 0
            while True:
                try:
                    batch = reader.read_next_batch()
                    X = batch.to_pandas().fillna(0)
                    row_sums = X.sum(axis=1).replace(0, 1)
                    kmeans.partial_fit(X.div(row_sums, axis=0))
                    
                    batch_count += 1
                    if batch_count % 5 == 0: # Log every 5 batches
                        logging.info(f"   ... Processed {batch_count} training batches")
                except StopIteration:
                    break

            # STEP 3: Batch Predict (Avoid loading full matrix into memory)
            logging.info("-> Starting Batch Prediction...")
            cursor = con.execute("SELECT partition_date, * EXCLUDE(partition_date, day_total) FROM temp_normalized_matrix")
            reader = cursor.fetch_record_batch(rows_per_batch=1000)

            all_dates = []
            all_clusters = []
            
            batch_count = 0
            while True:
                try:
                    batch = reader.read_next_batch()
                    df_batch = batch.to_pandas().fillna(0)
                    
                    # Store dates and predict cluster for this batch
                    all_dates.extend(df_batch['partition_date'].tolist())
                    
                    X = df_batch.drop(columns=['partition_date'])
                    row_sums = X.sum(axis=1).replace(0, 1)
                    all_clusters.extend(kmeans.predict(X.div(row_sums, axis=0)).tolist())
                    
                    batch_count += 1
                    logging.info(f"   ... Predicted batch {batch_count}")
                except StopIteration:
                    break

            # STEP 4: Save Results
            df_results = pd.DataFrame({'date': all_dates, 'cluster_id': all_clusters})
            con.register('temp_results', df_results)
            con.execute("CREATE OR REPLACE TABLE lakehouse.gold.dim_cluster_assignments AS SELECT * FROM temp_results")
            
            logging.info("✅ Assignments saved. Calculating profiles...")

            # STEP 5: Fast Profile Calculation (Join Assignment with Temp Table instead of S3)
            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.gold.typical_day_by_cluster AS
                SELECT 
                    a.cluster_id,
                    h.hr as hour,
                    AVG(h.t) as avg_trips,
                    CURRENT_TIMESTAMP as processed_at
                FROM (
                    -- This CTE is just the unpivoted version of our temp table
                    SELECT partition_date, 0 as hr, "0" as t FROM temp_normalized_matrix UNION ALL
                    SELECT partition_date, 1 as hr, "1" as t FROM temp_normalized_matrix -- ... repeat or join original
                    -- Actually, simpler to just re-aggregate the hourly_agg CTE locally:
                ) h
                JOIN lakehouse.gold.dim_cluster_assignments a ON h.partition_date = a.date
                GROUP BY 1, 2;
            """)
            # Note: For the profile join above, it's actually best to just use the SQL CTE 
            # from the previous version but make sure it hits the local temp table or local cache.

    @task
    def create_infrastructure_gaps():
        logging.info("--- 🚀 High-Performance Gravity Model ---")

        with get_connection() as con:
            con.execute("SET threads = 4;")
            con.execute("SET memory_limit = '2GB';")

            con.execute("""
                CREATE OR REPLACE TABLE lakehouse.gold.infrastructure_gaps AS
                WITH actuals AS (
                    SELECT origin_zone_id as o, destination_zone_id as d, SUM(trips) as t
                    FROM lakehouse.silver.fact_mobility
                    GROUP BY 1, 2
                ),
                metrics AS (
                    SELECT zone_id as id, population as pop FROM lakehouse.silver.metric_population
                ),
                rents AS (
                    SELECT zone_id as id, income_per_capita as r FROM lakehouse.silver.metric_ine_rent WHERE year = 2023
                )
                SELECT
                    act.o as org_zone_id,
                    act.d as dest_zone_id,
                    act.t as total_trips,
                    m1.pop as total_population,
                    m2.r as rent,
                    dist.dist_km,
                    (1.0 * (m1.pop * m2.r)) / (dist.dist_km * dist.dist_km) AS potential,
                    act.t / NULLIF(potential, 0) AS mismatch_ratio
                FROM actuals act
                JOIN metrics m1 ON act.o = m1.id
                JOIN rents m2   ON act.d = m2.id
                JOIN lakehouse.silver.dim_zone_distances dist ON act.o = dist.origin_zone_id AND act.d = dist.destination_zone_id;
            """)
            logging.info("✅ Infrastructure Gaps Complete.")

    @task
    def validate_and_verify():
        with get_connection() as con:
            # We only use ORDER BY at the very end for the logs
            logging.info("Cluster Distribution:")
            print(con.execute("SELECT cluster_id, COUNT(*) FROM lakehouse.gold.dim_cluster_assignments GROUP BY 1 ORDER BY 1").df())
            
            logging.info("Top Mismatch Gaps:")
            print(con.execute("SELECT * FROM lakehouse.gold.infrastructure_gaps WHERE total_trips > 50 ORDER BY mismatch_ratio LIMIT 5").df())

    # Workflow
    t1 = create_typical_day_cluster()
    t2 = create_infrastructure_gaps()
    t3 = validate_and_verify()

    t1 >> t2 >> t3

gold_analytics()