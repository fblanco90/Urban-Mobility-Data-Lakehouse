from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
import logging
import io
import os

# --- MATPLOTLIB HEADLESS SETUP ---
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt

# --- CONFIGURATION ---
S3_BUCKET = "ducklake-bdproject"
S3_KEY_PREFIX = "results/"

@dag(
    dag_id="mobility_04_visualization_to_s3",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="Start Date (YYYYMMDD)"),
        "end_date": Param("20231231", type="string", description="End Date (YYYYMMDD)"),
    },
    tags=['mobility', 's3', 'visualization']
)
def mobility_04_visualization_s3():

    @task
    def plot_to_s3(**context):
        """
        Task: Fetch mobility patterns from Gold layer and upload plot to S3.
        """
        logging.info("--- 🎨 Generating Visualization for S3 ---")
        
        # 1. Retrieve Parameters
        params = context['params']
        start_dt = params['start_date']
        end_dt = params['end_date']
        
        con = get_connection()

        try:
            # 2. Query Data from Gold Layer
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

    plot_to_s3()

mobility_04_visualization_s3()