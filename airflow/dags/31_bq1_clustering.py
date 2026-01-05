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

    sql_profiles = """
    CREATE OR REPLACE TABLE silver.tmp_gold_profiles_agg AS
    WITH municipios_in_polygon AS (
        SELECT zone_id, zone_name 
        FROM silver.dim_zones
        WHERE ST_Intersects(ST_GeomFromText('{{ params.polygon_wkt }}'), polygon)
    ),
    filtered_trips AS (
        SELECT 
            partition_date, 
            origin_zone_id, 
            destination_zone_id,
            hour(period) as hour, 
            trips
        FROM silver.fact_mobility f
        WHERE f.partition_date BETWEEN strptime('{{ params.start_date }}', '%Y%m%d')::DATE 
                             AND strptime('{{ params.end_date }}', '%Y%m%d')::DATE
        AND (
            f.origin_zone_id IN (SELECT zone_id FROM municipios_in_polygon)
            OR 
            f.destination_zone_id IN (SELECT zone_id FROM municipios_in_polygon)
        )
    )
    SELECT 
        t.partition_date, 
        t.hour, 
        zo.zone_name as origin_name,
        zd.zone_name as destination_name,
        SUM(t.trips) as total_trips
    FROM filtered_trips t
    JOIN silver.dim_zones zo ON t.origin_zone_id = zo.zone_id
    JOIN silver.dim_zones zd ON t.destination_zone_id = zd.zone_id
    GROUP BY 1, 2, 3, 4;
    """

    task_batch_profiles = run_batch_sql(
        task_id="batch_prepare_profiles", 
        sql_query=sql_profiles, 
        memory="12GB"
    )

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
    def plot_interactive_heatmap_html(**context):
        """
        Genera un único archivo HTML interactivo con un desplegable para seleccionar
        el heatmap del clúster deseado para una hora específica.
        Requiere la librería 'plotly' instalada en el entorno.
        """
        target_hour = context['params']['target_hour']
        
        logging.info(f"--- Iniciando generación de Heatmap Interactivo para la hora {target_hour} ---")

        # 1. Obtener datos
        with get_connection() as con:
            df = con.execute("SELECT * FROM lakehouse.gold.typical_od_matrices WHERE hour = ?", (target_hour,)).df()
        
        if df.empty:
            logging.warning("No hay datos para la hora seleccionada. No se genera HTML.")
            return

        clusters = sorted(df['cluster_id'].unique())
        n_clusters = len(clusters)
        
        # Inicializamos la figura y la lista de botones para el menú
        fig = go.Figure()
        buttons = []

        # 2. Iterar por cada clúster para crear los trazos (traces) y los botones
        for i, cluster_id in enumerate(clusters):
            logging.info(f"Procesando Cluster {cluster_id}...")
            cluster_data = df[df['cluster_id'] == cluster_id].copy()

            # Si hay pocas zonas, cogemos todas, si hay muchas, las top 15
            n_zones_to_keep = 15 
            if len(cluster_data['origin'].unique()) > n_zones_to_keep:
                top_origins = cluster_data.groupby('origin')['trips'].sum().nlargest(n_zones_to_keep).index
                top_dest = cluster_data.groupby('destination')['trips'].sum().nlargest(n_zones_to_keep).index
                cluster_data = cluster_data[
                    cluster_data['origin'].isin(top_origins) & 
                    cluster_data['destination'].isin(top_dest)
                ]
            
            if cluster_data.empty: continue

            # Pivotamos para tener la matriz
            matrix_df = cluster_data.pivot(index='origin', columns='destination', values='trips').fillna(0)
            
            # Preparar datos para Plotly
            z_data = matrix_df.values
            x_labels = matrix_df.columns.tolist()
            y_labels = matrix_df.index.tolist()
            
            # Transformamos los datos Z a log10 para el color.
            # Sumamos 1 para evitar log(0).
            z_log = np.log10(z_data + 1)

            # Creamos el trazo del heatmap
            trace = go.Heatmap(
                z=z_log,                # Usamos valores logarítmicos para la escala de color
                x=x_labels,             # Destinos en eje X
                y=y_labels,             # Orígenes en eje Y
                colorscale='YlGnBu',    # Misma paleta que usabas en Seaborn
                colorbar=dict(
                    title='Intensidad (Escala Log)',
                    tickvals=[0, 1, 2, 3, 4],
                    ticktext=['1 (10⁰)', '10 (10¹)', '100 (10²)', '1k (10³)', '10k (10⁴)']
                ),
                # Tooltip personalizado para mostrar el valor REAL, no el logarítmico
                hovertemplate='<b>Origen:</b> %{y}<br><b>Destino:</b> %{x}<br><b>Viajes (aprox):</b> %{customdata:.0f}<extra></extra>',
                customdata=z_data,      # Pasamos los datos reales para el tooltip
                visible=(i == 0)        # Solo el primer clúster es visible al inicio
            )
            fig.add_trace(trace)

            # --- Creamos la definición del botón para este clúster ---
            # Creamos una máscara booleana: [True, False, False] para el cluster 0, etc.
            visibility_mask = [False] * n_clusters
            visibility_mask[i] = True
            
            button = dict(
                label=f"Cluster {cluster_id}",
                method="update",
                args=[
                    {"visible": visibility_mask}, # Parte 1: Restyle (trazos)
                    {"title.text": f"Matriz OD Interactiva - Cluster {cluster_id} (Hora: {target_hour:02d}h)"} # Parte 2: Relayout (título)
                ]
            )
            buttons.append(button)

        # 3. Configurar el Layout (Diseño) final y añadir el menú
        fig.update_layout(
            title=f"Matriz OD Interactiva - Cluster {clusters[0]} (Hora: {target_hour:02d}h)",
            xaxis_title="Destino",
            yaxis_title="Origen",
            height=800, # Altura en píxeles
            margin=dict(
                l=200,  # Margen izquierdo fijo (ajusta este número si tus nombres son muy largos)
                r=50,   # Margen derecho
                t=100,  # Margen superior
                b=150   # Margen inferior para los nombres en vertical del eje X
            ),
            yaxis=dict(
                automargin=False,
                ticksuffix="  "   
            ),
            updatemenus=[
                dict(
                    type="dropdown",
                    direction="down",
                    # x=1.1 alinea el menú con el borde derecho de la leyenda
                    # y=1.15 lo sube por encima del gráfico y del título de la leyenda
                    x=1.1, 
                    y=1.15,
                    xanchor='right', # El punto x=1.1 es la esquina derecha del menú
                    yanchor='top',   # El punto y=1.15 es la esquina superior del menú
                    # -------------------------
                    showactive=True,
                    buttons=buttons
                )
            ]
            )

        logging.info("Generando archivo HTML...")
        # 4. Guardar a un buffer de texto (StringIO para HTML)
        html_buffer = io.StringIO()
        # include_plotlyjs='cdn' hace que el archivo sea más ligero al cargar la librería de internet
        fig.write_html(html_buffer, include_plotlyjs='cdn', full_html=True)
        html_buffer.seek(0)

        # 5. Subir a S3
        s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
        file_key = f"{S3_KEY_PREFIX}interactive_heatmap_h{target_hour}.html"
        
        logging.info(f"Subiendo HTML a S3: s3://{S3_BUCKET}/{file_key}")
        s3_hook.get_conn().put_object(
            Body=html_buffer.getvalue(),
            Bucket=S3_BUCKET,
            Key=file_key,
            ContentType='text/html'
        )
        logging.info("✅ Archivo HTML interactivo subido correctamente.")

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

    @task
    def generate_report_markdown(**context):
        """
        Generates a Markdown report in S3 summarizing BQ1 results.
        Uses the underlying boto3 client to avoid 'mimetype' keyword errors.
        """
        params = context['params']
        start_dt = params['start_date']
        end_dt = params['end_date']
        target_h = params['target_hour']
        
        # Define filenames
        png_filename = f"mobility_report_{start_dt}_{end_dt}.png"
        html_filename = f"interactive_heatmap_h{target_h}.html"
        md_filename = f"report_BQ1_{start_dt}.md"
        
        # S3 Public URL (or internal reference)
        s3_base_url = f"https://{S3_BUCKET}.s3.eu-central-1.amazonaws.com/{S3_KEY_PREFIX}"

        # Mejoramos el espaciado con saltos de línea extra (\n) para asegurar el renderizado
        markdown_content = f"""# Business Question 1: Typical Mobility Patterns (2023)

## 1. Execution Summary
This report analyzes mobility patterns in Spain using MITMA and INE public data.

* **Analysis Period:** {start_dt} to {end_dt}
* **Target Hour:** {target_h}:00h
* **Spatial Filter:** `{params['polygon_wkt']}`

---

## 2. Mobility Pattern Visualization
The **Gold layer** identified daily profiles via K-Means clustering.

![Mobility Patterns Plot]({s3_base_url}{png_filename})

*Figure 1: Mean hourly trips per cluster for the reference period.*

        ---

## 3. Interactive Origin-Destination Analysis
Access the interactive tool for zone-to-zone flows here:

👉 [**Open Interactive OD Matrix (HTML)**]({s3_base_url}{html_filename})

---
## 4. Technical Infrastructure
* **Tiers:** 3-tier Lakehouse (Bronze, Silver, Gold).
* **Engine:** DuckDB for SQL-based analytics.
* **Storage:** DuckLake for ACID storage.
"""

        s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
        s3_client = s3_hook.get_conn()
                
        s3_client.put_object(
            Body=markdown_content,
            Bucket=S3_BUCKET,
            Key=f"{S3_KEY_PREFIX}{md_filename}",
            ContentType='text/markdown'
        )
                
        logging.info("✅ Markdown report successfully uploaded with public access.")

    # --- ORCHESTRATION ---
    proc = process_gold_patterns_locally()
    t_md = generate_report_markdown()
    
    task_batch_profiles >> proc >> [plot_interactive_heatmap_html(), plot_to_s3()] >> t_md >> cleanup()

gold_analytics()