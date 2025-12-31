from airflow.decorators import dag, task
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import logging
import os

@dag(
    dag_id="mobility_03_gold_analytics",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['mobility', 'gold', 'analytics']
)
def mobility_03_gold_analytics():

    @task
    def create_typical_day_cluster():
        """
        Task 1: Clustering Analysis (K-Means)
        Calculates clusters based on ALL data found in Silver.
        """
        logging.info("--- 🏗️ Starting Table 1: Clustering Analysis (Full History) ---")

        con = get_connection()
        try:
            # ---------------------------------------------------------
            # Fetch Data for Python (No Filters)
            # ---------------------------------------------------------
            logging.info("-> Fetching all available mobility data...")
            
            fetch_script = """
                SELECT 
                    m.partition_date AS date,
                    hour(m.period)   AS hour,
                    SUM(m.trips)     AS total_trips
                FROM lakehouse.silver.fact_mobility m
                JOIN lakehouse.silver.dim_zones zo ON m.origin_zone_id = zo.zone_id
                JOIN lakehouse.silver.dim_zones zd ON m.destination_zone_id = zd.zone_id
                WHERE m.trips IS NOT NULL
                GROUP BY 1, 2
                ORDER BY 1, 2;
            """
            df = con.execute(fetch_script).df()

            if df.empty:
                logging.warning("⚠️ No data found in Silver. Skipping clustering.")
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

            # Register DataFrame as a View 
            con.register('view_dim_clusters', df_results)

            # ---------------------------------------------------------
            # EXECUTION 2: Materialize Tables & Return Analysis
            # ---------------------------------------------------------
            logging.info("-> Materializing Tables & Analyzing...")
            
            # Note: For analysis_start/end, we now calculate min/max from the table 
            # dynamically since we removed the input parameters.
            save_and_analyze_script = """
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
                    MIN(f.partition_date) OVER() as analysis_start,
                    MAX(f.partition_date) OVER() as analysis_end
                FROM lakehouse.silver.fact_mobility f
                JOIN dim_mobility_patterns p ON f.partition_date = p.date
                JOIN lakehouse.silver.dim_zones zo ON f.origin_zone_id = zo.zone_id
                JOIN lakehouse.silver.dim_zones zd ON f.destination_zone_id = zd.zone_id
                -- No spatial or temporal filters
                GROUP BY p.cluster_id, hour(f.period), f.partition_date
                ORDER BY p.cluster_id, hour(f.period);

                -- 3. Return Analysis
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
    def create_infrastructure_gaps():
        """
        Task 3: Infrastructure Gaps - Manual Haversine Calculation (Fail-Safe)
        """
        logging.info("--- 🏗️ Starting Table 2: Infrastructure Gaps ---")
        con = get_connection()
        try:
            logging.info("-> Executing Gravity Model with Manual Math...")
            
            # 1. Load Spatial Extension (still needed to extract X/Y)
            con.execute("INSTALL spatial; LOAD spatial;")

            full_script = """
                CREATE OR REPLACE TABLE lakehouse.gold.infrastructure_gaps AS
                WITH od_pairs AS (
                    SELECT
                        origin_zone_id,
                        destination_zone_id,
                        SUM(trips) AS total_actual_trips
                    FROM lakehouse.silver.fact_mobility
                    GROUP BY 1, 2
                ),
                unique_rent AS (
                    SELECT zone_id, income_per_capita AS rent
                    FROM lakehouse.silver.metric_ine_rent
                    WHERE year = 2023
                ),
                
                -- STEP 1: Extract Coordinates & Convert to Radians
                -- We assume Standard WKT: X=Longitude, Y=Latitude
                geo_data AS (
                    SELECT 
                        zone_id,
                        radians(ST_Y(ST_Centroid(polygon))) as lat_rad,
                        radians(ST_X(ST_Centroid(polygon))) as lon_rad,
                        ST_Y(ST_Centroid(polygon)) as raw_lat  -- kept for validity check
                    FROM lakehouse.silver.dim_zones
                    WHERE polygon IS NOT NULL
                ),

                model_calculation AS (
                    SELECT
                        m.origin_zone_id AS org_zone_id,
                        m.destination_zone_id AS dest_zone_id,
                        p.population AS total_population,
                        r.rent,
                        m.total_actual_trips AS total_trips,
                        
                        z_org.lat_rad as lat1, z_org.lon_rad as lon1,
                        z_dest.lat_rad as lat2, z_dest.lon_rad as lon2
                            
                    FROM od_pairs AS m
                    JOIN lakehouse.silver.metric_population AS p ON m.origin_zone_id = p.zone_id
                    JOIN unique_rent AS r ON m.destination_zone_id = r.zone_id
                    
                    JOIN geo_data as z_org ON m.origin_zone_id = z_org.zone_id
                    JOIN geo_data as z_dest ON m.destination_zone_id = z_dest.zone_id

                    WHERE p.population > 0 
                    AND r.rent > 0
                    AND m.origin_zone_id != m.destination_zone_id 
                    -- Ensure we have valid coordinates (Not 0, Not NULL)
                    AND z_org.raw_lat IS NOT NULL 
                    AND z_dest.raw_lat IS NOT NULL
                ),
                
                distance_calc AS (
                    SELECT 
                        *,
                        -- STEP 2: The Haversine Formula (Result in KM)
                        -- 6371 = Earth Radius in KM
                        -- Formula: 2 * R * asin(sqrt(a))
                        -- a = sin²(dlat/2) + cos(lat1)*cos(lat2)*sin²(dlon/2)
                        (
                            6371.0 * 2.0 * asin(
                                sqrt(
                                    power(sin((lat2 - lat1) / 2.0), 2) +
                                    cos(lat1) * cos(lat2) *
                                    power(sin((lon2 - lon1) / 2.0), 2)
                                )
                            )
                        ) AS calculated_dist_km
                    FROM model_calculation
                ),

                final_safe_check AS (
                    SELECT
                        *,
                        CASE 
                            -- Now we check the MANUAL result
                            WHEN calculated_dist_km IS NULL THEN 0.5
                            WHEN isnan(calculated_dist_km) THEN 0.5 
                            WHEN calculated_dist_km < 0.5 THEN 0.5 
                            ELSE calculated_dist_km 
                        END AS geographic_distance_km
                    FROM distance_calc
                )

                SELECT
                    org_zone_id,
                    dest_zone_id,
                    total_trips,
                    total_population,
                    rent,
                    geographic_distance_km,

                    -- Gravity Model
                    (1.0 * (CAST(total_population AS DOUBLE) * CAST(rent AS DOUBLE))) / 
                    (POW(geographic_distance_km, 2)) AS estimated_potential_trips,

                    -- Mismatch Ratio
                    total_trips / NULLIF(
                        (1.0 * (CAST(total_population AS DOUBLE) * CAST(rent AS DOUBLE))) / 
                        (POW(geographic_distance_km, 2)), 0
                    ) AS mismatch_ratio,

                    CURRENT_TIMESTAMP as processed_at
                FROM final_safe_check;
            """
            
            con.execute(full_script)
            logging.info("✅ Table created using Haversine Formula.")

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

    @task
    def classify_zone_functions():
        """
        Task: Functional Classification of Zones
        OPTIMIZATION: Aggregates raw mobility data first, then joins dimensions.
        """
        logging.info("--- 🏷️ Starting Table 3: Functional Zone Classification (Optimized) ---")

        con = get_connection()
        try:
            logging.info("-> Calculating Flow Asymmetry & Retention...")

            classification_script = """
                CREATE OR REPLACE TABLE lakehouse.gold.zone_functional_classification AS
                WITH pre_aggregated_flows AS (
                    -- 1. AGGREGATE FIRST (Massive Performance Boost)
                    -- Collapse millions of raw rows into unique OD pairs immediately.
                    SELECT 
                        origin_zone_id,
                        destination_zone_id,
                        SUM(trips) as total_trips
                    FROM lakehouse.silver.fact_mobility
                    GROUP BY 1, 2
                ),
                zone_flow_stats AS (
                    -- 2. Calculate In/Out/Internal using only IDs (No Joins yet)
                    SELECT 
                        -- We take the union of all zones appearing as origin or dest
                        coalesce(o.origin_zone_id, d.destination_zone_id) as zone_id,
                        
                        -- Internal: Origin = Dest
                        SUM(CASE 
                            WHEN o.origin_zone_id = o.destination_zone_id THEN o.total_trips 
                            ELSE 0 
                        END) as internal_trips,

                        -- Outflow: Trips starting here (excluding internal)
                        SUM(CASE 
                            WHEN o.origin_zone_id IS NOT NULL AND o.origin_zone_id != o.destination_zone_id THEN o.total_trips 
                            ELSE 0 
                        END) as outflow,

                        -- Inflow: Trips ending here (excluding internal)
                        SUM(CASE 
                            WHEN d.destination_zone_id IS NOT NULL AND d.origin_zone_id != d.destination_zone_id THEN d.total_trips 
                            ELSE 0 
                        END) as inflow

                    FROM pre_aggregated_flows o
                    FULL OUTER JOIN pre_aggregated_flows d ON o.origin_zone_id = d.destination_zone_id
                    GROUP BY 1
                ),
                metrics_calc AS (
                    -- 3. Calculate Ratios
                    SELECT 
                        zone_id,
                        internal_trips,
                        outflow,
                        inflow,
                        (outflow + internal_trips) AS total_generated,
                        
                        -- Net Flow Ratio
                        CASE 
                            WHEN (inflow + outflow) = 0 THEN 0 
                            ELSE (inflow - outflow) / (inflow + outflow) 
                        END AS net_flow_ratio,

                        -- Retention Rate
                        CASE 
                            WHEN (outflow + internal_trips) = 0 THEN 0
                            ELSE internal_trips / (outflow + internal_trips)
                        END AS retention_rate
                    FROM zone_flow_stats
                )
                -- 4. JOIN LAST: Attach Zone Names and Apply Labels
                SELECT 
                    m.zone_id,
                    z.zone_name,
                    m.internal_trips,
                    m.inflow,
                    m.outflow,
                    ROUND(m.net_flow_ratio, 3) as net_flow_ratio,
                    ROUND(m.retention_rate, 3) as retention_rate,
                    
                    CASE 
                        WHEN m.retention_rate > 0.6 THEN 'Self-Sustaining Cell'
                        WHEN m.net_flow_ratio > 0.15 THEN 'Activity Hub (Importer)'
                        WHEN m.net_flow_ratio < -0.15 THEN 'Bedroom Community (Exporter)'
                        ELSE 'Balanced / Transit Zone'
                    END AS functional_label,
                    
                    CURRENT_TIMESTAMP as processed_at
                FROM metrics_calc m
                JOIN lakehouse.silver.dim_zones z ON m.zone_id = z.zone_id
                WHERE (m.internal_trips + m.inflow + m.outflow) > 0;

                -- 5. Verification
                SELECT functional_label, COUNT(*) as zone_count 
                FROM lakehouse.gold.zone_functional_classification 
                GROUP BY functional_label 
                ORDER BY zone_count DESC;
            """

            verification_df = con.execute(classification_script).df()
            logging.info(f"✅ Classification Table Created.\nSummary:\n{verification_df.to_string(index=False)}")

        except Exception as e:
            logging.error(f"❌ Failed in Classification Task: {e}")
            raise e
        finally:
            con.close()


    @task
    def verify_zone_classification():
        """
        Verification: Functional Classification
        Fix: Corrected SQL syntax in Window Function and handled missing column.
        """
        logging.info("--- 🔎 Verification: Functional Zone Classification ---")
        con = get_connection()
        pd.set_option('display.max_colwidth', None)
        
        try:
            query_verify = """
                WITH ranked_examples AS (
                    SELECT 
                        functional_label,
                        zone_name,
                        net_flow_ratio,
                        retention_rate,
                        ROW_NUMBER() OVER (
                            PARTITION BY functional_label 
                            ORDER BY 
                                CASE 
                                    -- Hubs: Sort by High Positive Ratio (e.g., 0.8 is top)
                                    WHEN functional_label LIKE '%Hub%' THEN net_flow_ratio
                                    
                                    -- Bedrooms: Sort by "Deepest" Negative Ratio (e.g., -0.8).
                                    -- We negate it (-(-0.8) = 0.8) so it ranks higher than -0.1 in a DESC sort.
                                    WHEN functional_label LIKE '%Bedroom%' THEN -net_flow_ratio
                                    
                                    -- Self-Sustaining: Sort by High Retention
                                    WHEN functional_label LIKE '%Self-Sustaining%' THEN retention_rate
                                    
                                    -- Others: Sort by Volume (Re-calculated from available columns)
                                    ELSE (internal_trips + outflow)
                                END DESC
                        ) as rank
                    FROM lakehouse.gold.zone_functional_classification
                )
                SELECT 
                    functional_label,
                    COUNT(*) as total_zones,
                    LIST(zone_name ORDER BY rank) FILTER (WHERE rank <= 3) as top_3_examples
                FROM ranked_examples
                GROUP BY functional_label
                ORDER BY total_zones DESC;
            """
            
            logging.info("Summarizing Functional Roles...")
            df_verify = con.execute(query_verify).df()
            
            if df_verify.empty:
                logging.warning("⚠️ Verification returned no rows.")
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

    # Branch 3: Classification
    t_classify = classify_zone_functions()
    t_verify_class = verify_zone_classification()

    # Dependencies
    t1_clustering >> t3_validate
    t2_gaps >> t5_verify
    t_classify >> t_verify_class

mobility_03_gold_analytics()