from airflow.sdk import dag, task, Param
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import logging

# --- CONFIGURATION ---
# Default Polygon (South Spain) - Can be overridden in Airflow UI
DEFAULT_POLYGON = "POLYGON((-7.55 36.0, -7.55 38.8, -1.0 38.8, -1.0 36.0, -7.55 36.0))"

@dag(
    dag_id="mobility_03_gold_analytics",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="YYYY-MM-DD"),
        "end_date": Param("20230101", type="string", description="YYYY-MM-DD"),
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string",title="Spatial Filter (WKT)", description="Paste your WKT Polygon here.")
    },
    tags=['mobility', 'gold', 'analytics']
)
def mobility_03_gold_analytics():

    @task
    def create_typical_day_cluster(**context):
        """
        Gold Table 1: typical_day_by_cluster
        Logic: Fetches range, runs K-Means (Python), and stores profiles in DuckDB.
        """
        logging.info("--- 🏗️ Starting Table 1: Clustering Analysis ---")
        
        # 1. Get Parameters
        params = context['params']
        input_start_date = params['start_date']
        input_end_date = params['end_date']
        input_polygon_wkt = params['polygon_wkt']

        con = get_connection()

        try:
            con.execute("INSTALL spatial; LOAD spatial;")

            # --- 1. DATA PREPARATION ---
            logging.info(f"-> Fetching data ({input_start_date} to {input_end_date})...")
            
            query_fetch = f"""
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
            
            df = con.execute(query_fetch).df()

            if df.empty:
                logging.warning("⚠️ No data found for Table 1. Skipping clustering.")
                return

            # Pivot: Rows=Days, Columns=Hours 0-23
            df_pivot = df.pivot(index='date', columns='hour', values='total_trips').fillna(0)
            for h in range(24):
                if h not in df_pivot.columns:
                    df_pivot[h] = 0
            df_pivot = df_pivot.sort_index(axis=1)

            # --- 2. NORMALIZATION ---
            logging.info("-> Normalizing profiles...")
            row_sums = df_pivot.sum(axis=1)
            df_normalized = df_pivot.div(row_sums.replace(0, 1), axis=0).fillna(0)

            # --- 3. CLUSTERING ---
            n_clusters = 3
            logging.info(f"-> Running K-Means (k={n_clusters})...")
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            clusters = kmeans.fit_predict(df_normalized)

            df_results = pd.DataFrame({
                'date': df_normalized.index,
                'cluster_id': clusters
            })

            # --- 4. BUILD GOLD TABLE ---
            logging.info("-> Materializing 'typical_day_by_cluster'...")
            con.register('view_dim_clusters', df_results)

            query_typical_cte = f"""
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
            """
            con.execute(query_typical_cte)
            con.unregister('view_dim_clusters')
            
            # --- 5. INTERPRETATION ---
            analysis_df = con.execute("""
                SELECT cluster_id, COUNT(*) as days, MODE(dayname(date)) as typical_day
                FROM view_dim_clusters GROUP BY cluster_id
            """).df()
            logging.info(f"📊 Cluster Analysis:\n{analysis_df.to_string(index=False)}")

        except Exception as e:
            logging.error(f"❌ Failed in Table 1: {e}")
            raise e
        finally:
            con.close()

    @task
    def create_infrastructure_gaps(**context):
        """
        Gold Table 2: infrastructure_gaps
        Logic: Gravity model calculation comparing actual trips vs potential (Rent * Pop / Dist^2)
        """
        logging.info("--- 🏗️ Starting Table 2: Infrastructure Gaps ---")
        
        # 1. Get Parameters
        params = context['params']
        input_start_date = params['start_date']
        input_end_date = params['end_date']
        input_polygon_wkt = params['polygon_wkt']

        con = get_connection()
        
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            
            logging.info(f"-> Executing Gravity Model ({input_start_date} to {input_end_date})...")

            # We use f-string to inject variables safely
            gold_bq2_query = f"""
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
            
            con.execute(gold_bq2_query)
            logging.info("✅ Table 'lakehouse.gold.infrastructure_gaps' created.")

            # --- Verification Log ---
            verification_df = con.execute("""
                SELECT 
                    org_zone_id, dest_zone_id, total_trips, 
                    ROUND(estimated_potential_trips, 2) as est_potential, 
                    ROUND(mismatch_ratio, 4) as mismatch
                FROM lakehouse.gold.infrastructure_gaps
                WHERE total_trips > 10
                ORDER BY mismatch_ratio ASC
                LIMIT 5;
            """).df()
            
            logging.info(f"📊 Top 5 High-Mismatch Zones:\n{verification_df.to_string(index=False)}")

        except Exception as e:
            logging.error(f"❌ Failed in Table 2: {e}")
            raise e
        finally:
            con.close()

    # --- DAG FLOW ---
    # These tasks are independent analytics layers, so they run in parallel.
    t1 = create_typical_day_cluster()
    t2 = create_infrastructure_gaps()

mobility_03_gold_analytics()