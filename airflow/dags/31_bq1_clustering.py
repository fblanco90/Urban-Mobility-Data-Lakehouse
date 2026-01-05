import io
from airflow.sdk import dag, task, Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from pendulum import datetime
from utils_db import get_connection, run_batch_sql
import pandas as pd
from sklearn.cluster import KMeans
import logging
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt

DEFAULT_POLYGON = "POLYGON((715000 4365000, 735000 4365000, 735000 4385000, 715000 4385000, 715000 4365000))"

# --- CONFIGURATION ---
aws = BaseHook.get_connection("aws_s3_conn")
S3_BUCKET = aws.extra_dejson.get('bucket_name', 'ducklake-bdproject')
S3_KEY_PREFIX = "results/bq1/"

@dag(
    dag_id="31_bq1_clustering",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="YYYYMMDD"),
        "end_date": Param("20230101", type="string", description="YYYYMMDD"),
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string", title="Spatial Filter (WKT)"),
        "input_crs": Param(
            "EPSG:25830", 
            enum=["EPSG:25830", "OGC:CRS84", "EPSG:4326"], 
            title="Coordinate Reference System (CRS)",
            description="25830 (Meters), CRS84 (Lon/Lat), 4326 (Lat/Lon)"
        )
    },
    tags=['mobility', 'gold', 'analytics', 'aws_batch']
)

def gold_analytics():

    @task
    def prepare_profiles_locally(**context):
        params = context['params']
        polygon_wkt = params['polygon_wkt']

        sd_raw = str(params['start_date']) # "20230101"
        ed_raw = str(params['end_date'])   # "20231231"

        # Transformamos a formato YYYY-MM-DD (añadiendo los guiones)
        start_date = f"{sd_raw[:4]}-{sd_raw[4:6]}-{sd_raw[6:]}"
        end_date = f"{ed_raw[:4]}-{ed_raw[4:6]}-{ed_raw[6:]}"

        with get_connection() as con:
            con.execute(f"""
            CREATE OR REPLACE TABLE lakehouse.silver.tmp_gold_profiles_agg AS
            WITH municipios_in_polygon AS (
                SELECT zone_id 
                FROM lakehouse.silver.dim_zones
                -- Usamos Jinja para el polígono y el CRS si fuera necesario
                WHERE ST_Intersects(ST_GeomFromText('{polygon_wkt}'), polygon)
            ),
            filtered_trips AS (
                -- Seleccionamos viajes que nacen O mueren en el polígono
                SELECT 
                    partition_date, 
                    hour(period) as hour, 
                    trips
                FROM lakehouse.silver.fact_mobility f
                WHERE f.partition_date >= '{start_date}' 
                    AND f.partition_date <= '{end_date}'
                AND (
                    f.origin_zone_id IN (SELECT zone_id FROM municipios_in_polygon)
                    OR 
                    f.destination_zone_id IN (SELECT zone_id FROM municipios_in_polygon)
                )
            )
            -- Agrupamos todo al final una sola vez
            SELECT 
                partition_date, 
                hour, 
                SUM(trips) as total_trips
            FROM filtered_trips
            GROUP BY 1, 2;
            """)

            logging.info("✅ Temporary aggregated profiles table created: lakehouse.silver.tmp_gold_profiles_agg")

    # task_batch_profiles = run_batch_sql(
    #     task_id="batch_prepare_profiles", 
    #     sql_query=sql_profiles, 
    #     memory="12GB"
    # )


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

    # @task
    # def plot_heatmaps_to_s3(**context):
    #     import seaborn as sns
    #     con = get_connection()
    #     df = con.execute("SELECT * FROM lakehouse.gold.typical_od_matrices").df()
        
    #     s3_hook = S3Hook(aws_conn_id='aws_s3_conn')

    #     for cluster_id in df['cluster_id'].unique():
    #         # Filtramos datos del cluster
    #         cluster_data = df[df['cluster_id'] == cluster_id]
            
    #         # Pivotamos para formato Matriz (Filas=Origen, Columnas=Destino)
    #         matrix = cluster_data.pivot(index='origin', columns='destination', values='trips').fillna(0)
            
    #         # Dibujamos el Heatmap
    #         plt.figure(figsize=(10, 8))
    #         sns.heatmap(matrix, annot=True, cmap="YlGnBu", fmt='.1f')
    #         plt.title(f"Matriz de Origen-Destino - Patrón (Cluster) {cluster_id}")
    #         plt.xlabel("Destino")
    #         plt.ylabel("Origen")
    #         plt.tight_layout()

    #         # Guardar y subir
    #         img_buffer = io.BytesIO()
    #         plt.savefig(img_buffer, format='png')
    #         img_buffer.seek(0)
            
    #         file_key = f"{S3_KEY_PREFIX}heatmap_cluster_{cluster_id}.png"
    #         s3_hook.load_file_obj(img_buffer, key=file_key, bucket_name=S3_BUCKET, replace=True)
    #         plt.close()
    #         logging.info(f"✅ Heatmap cluster {cluster_id} subido a S3.")

    @task
    def cleanup():
        with get_connection() as con:            
            con.execute("DROP TABLE IF EXISTS silver.tmp_gold_profiles_agg;")
            con.execute("DROP TABLE IF EXISTS gold.dim_cluster_assignments;")

    @task
    def plot_to_s3(**context):
        """
        Task: Fetch mobility patterns from Gold layer and upload plot to S3.
        """
        logging.info("--- 🎨 Generating Visualization for S3 ---")
        
        params = context['params']
        start_dt = params['start_date']
        end_dt = params['end_date']
        
        con = get_connection()

        try:
            query = """
            SELECT 
                hour, 
                'Cluster ' || CAST(cluster_id AS VARCHAR) as pattern_name, 
                avg_trips
            FROM lakehouse.gold.typical_day_patterns
            ORDER BY hour, cluster_id;
            """
            
            logging.info("Fetching data from Gold table...")
            df = con.execute(query).df()

            if df.empty:
                logging.error("❌ No data found in typical_day_patterns table.")
                return

            # 3. Pivot and Plot
            pivot_df = df.pivot(index='hour', columns='pattern_name', values='avg_trips')
            
            logging.info(f"Generating plot for: {start_dt} to {end_dt}")
            fig, ax = plt.subplots(figsize=(12, 7))
            
            pivot_df.plot(kind='line', ax=ax, marker='o', markersize=4, linewidth=2)
            
            ax.set_title(f'Typical Daily Mobility Patterns\nPeriod: {start_dt} - {end_dt}', fontsize=15)
            ax.set_xlabel('Hour of Day', fontsize=12)
            ax.set_ylabel('Average Trips', fontsize=12)
            
            ax.set_xticks(range(0, 24))
            ax.set_xticklabels([f'{h:02d}:00' for h in range(24)], rotation=45)
            ax.grid(True, linestyle='--', alpha=0.6)
            ax.legend(title='Profiles', bbox_to_anchor=(1.05, 1), loc='upper left')
            
            plt.tight_layout()

            # 4. Save Plot to Memory Buffer
            img_buffer = io.BytesIO()
            plt.savefig(img_buffer, format='png', dpi=300)
            img_buffer.seek(0)

            # 5. Upload to Amazon S3
            s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
            file_key = f"{S3_KEY_PREFIX}mobility_report_{start_dt}_{end_dt}.png"
            
            logging.info(f"Uploading file to S3: s3://{S3_BUCKET}/{file_key}")
            
            s3_hook.load_file_obj(
                file_obj=img_buffer,
                key=file_key,
                bucket_name=S3_BUCKET,
                replace=True
            )
            
            logging.info("✅ Plot successfully uploaded to S3.")
            plt.close(fig)

        except Exception as e:
            logging.error(f"❌ Visualization failed: {str(e)}")
            raise e
        finally:
            con.close()

    # --- ORCHESTRATION ---
    prepare_profiles_locally() >> process_gold_patterns_locally() >> cleanup() >> plot_to_s3()

gold_analytics()