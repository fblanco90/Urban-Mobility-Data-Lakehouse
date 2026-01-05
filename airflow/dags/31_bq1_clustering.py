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
from matplotlib.colors import LogNorm
import seaborn as sns
import plotly.graph_objects as go
import numpy as np

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
        "target_hour": Param(8, type="integer", minimum=0, maximum=23, title="Heatmap Hour"),
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
                SELECT zone_id, zone_name 
                FROM lakehouse.silver.dim_zones
                WHERE ST_Intersects(ST_GeomFromText('{polygon_wkt}'), polygon)
            ),
            filtered_trips AS (
                -- Seleccionamos viajes que empiezan o acaban en el polígono
                SELECT 
                    partition_date, 
                    origin_zone_id, 
                    destination_zone_id,
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
            SELECT 
                t.partition_date, 
                t.hour, 
                zo.zone_name as origin_name,      -- Nombre del origen
                zd.zone_name as destination_name, -- Nombre del destino
                SUM(t.trips) as total_trips
            FROM filtered_trips t
            JOIN lakehouse.silver.dim_zones zo ON t.origin_zone_id = zo.zone_id
            JOIN lakehouse.silver.dim_zones zd ON t.destination_zone_id = zd.zone_id
            GROUP BY 1, 2, 3, 4;
            """)

            logging.info("✅ Temporary aggregated profiles table created: lakehouse.silver.tmp_gold_profiles_agg")

    # task_batch_profiles = run_batch_sql(
    #     task_id="batch_prepare_profiles", 
    #     sql_query=sql_profiles, 
    #     memory="12GB"
    # )


    @task
    def process_gold_patterns_locally():
        from sklearn.cluster import KMeans
        import pandas as pd
        
        with get_connection() as con:
            # df_raw tiene: [partition_date, hour, origin_zone_id, destination_zone_id, total_trips]
            df_raw = con.execute("SELECT * FROM lakehouse.silver.tmp_gold_profiles_agg").df()
        
        if df_raw.empty:
            return

        # Sumamos los viajes por día y hora (ignorando origen/destino)
        df_temporal = df_raw.groupby(['partition_date', 'hour'])['total_trips'].sum().reset_index()
        
        # Pivotamos para que cada fila sea un día y cada columna una de las 24 horas
        df_pivot = df_temporal.pivot(index='partition_date', columns='hour', values='total_trips').fillna(0)
        
        # Normalizamos
        norm_data = df_pivot.div(df_pivot.sum(axis=1).replace(0, 1), axis=0)
        
        # Entrenamos el K-Means
        kmeans = KMeans(n_clusters=3, random_state=42, n_init='auto').fit(norm_data)
        df_labels = pd.DataFrame({'partition_date': df_pivot.index, 'cluster_id': kmeans.labels_})

        # Unimos las etiquetas del clustering con los datos temporales
        df_gen = df_temporal.merge(df_labels, on='partition_date')
        gold_general_patterns = df_gen.groupby(['cluster_id', 'hour'])['total_trips'].mean().reset_index()

        # Unimos las etiquetas con los datos originales (que tienen origen/destino)
        df_geo = df_raw.merge(df_labels, on='partition_date')

        # Agrupamos por clúster, hora, origen y destino para obtener la matriz OD típica
        gold_od_matrix = df_geo.groupby(['cluster_id', 'hour', 'origin_name', 'destination_name'])['total_trips'].mean().reset_index()
        gold_od_matrix.columns = ['cluster_id', 'hour', 'origin', 'destination', 'trips']

        # Guardamos los resultados en Gold
        with get_connection() as con:
            # Para el plot de líneas (General)
            con.register('tmp_patterns', gold_general_patterns)
            con.execute("CREATE OR REPLACE TABLE lakehouse.gold.typical_day_patterns AS SELECT * FROM tmp_patterns")
            
            # Para los heatmaps (Geográfico)
            con.register('tmp_od', gold_od_matrix)
            con.execute("CREATE OR REPLACE TABLE lakehouse.gold.typical_od_matrices AS SELECT * FROM tmp_od")

    @task
    def plot_heatmaps_to_s3(**context):
        target_hour = context['params']['target_hour']

        with get_connection() as con:
            df = con.execute("SELECT * FROM lakehouse.gold.typical_od_matrices").df()
        
        if df.empty: return

        s3_hook = S3Hook(aws_conn_id='aws_s3_conn')

        df_hour = df[df['hour'] == target_hour]

        for cluster_id in df_hour['cluster_id'].unique():
            cluster_data = df_hour[df_hour['cluster_id'] == cluster_id]

            # --- Selección de Top Zonas  ---
            top_zones = cluster_data.groupby('origin')['trips'].sum().nlargest(10).index
            cluster_data = cluster_data[
                cluster_data['origin'].isin(top_zones) & 
                cluster_data['destination'].isin(top_zones)
            ]
            
            matrix = cluster_data.pivot(index='origin', columns='destination', values='trips').fillna(0)
            
            plt.figure(figsize=(12, 10))
            
            sns.heatmap(
                matrix, 
                annot=False, 
                cmap="YlGnBu", 
                norm=LogNorm() 
            )

            plt.title(f"Matriz OD (Top Zonas) - Cluster {cluster_id} a las {target_hour:02d}:00h\n(Log Scale)")

            img_buffer = io.BytesIO()
            plt.savefig(img_buffer, format='png', dpi=150)
            img_buffer.seek(0)
            
            file_key = f"{S3_KEY_PREFIX}heatmap_cluster_{cluster_id}_h{target_hour}.png"
            s3_hook.load_file_obj(img_buffer, key=file_key, bucket_name=S3_BUCKET, replace=True)
            plt.close()

    @task
    def cleanup():
        with get_connection() as con:            
            con.execute("DROP TABLE IF EXISTS silver.tmp_gold_profiles_agg;")
            con.execute("DROP TABLE IF EXISTS gold.typical_od_matrices;")

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
                total_trips
            FROM lakehouse.gold.typical_day_patterns
            ORDER BY hour, cluster_id;
            """
            
            logging.info("Fetching data from Gold table...")
            df = con.execute(query).df()

            if df.empty:
                logging.error("❌ No data found in typical_day_patterns table.")
                return

            # 3. Pivot and Plot
            pivot_df = df.pivot(index='hour', columns='pattern_name', values='total_trips')
            
            logging.info(f"Generating plot for: {start_dt} to {end_dt}")
            fig, ax = plt.subplots(figsize=(12, 7))
            
            pivot_df.plot(kind='line', ax=ax, marker='o', markersize=4, linewidth=2)
            
            ax.set_title(f'Typical Daily Mobility Patterns\nPeriod: {start_dt} - {end_dt}', fontsize=15)
            ax.set_xlabel('Hour of Day', fontsize=12)
            ax.set_ylabel('Total Trips', fontsize=12)
            
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
    prep = prepare_profiles_locally()
    proc = process_gold_patterns_locally()
    
    prep >> proc >> [plot_heatmaps_to_s3(), plot_to_s3()] >> cleanup()

gold_analytics()