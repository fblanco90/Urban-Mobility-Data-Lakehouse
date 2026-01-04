from airflow.sdk import dag
from pendulum import datetime
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.sdk.bases.hook import BaseHook
import logging

@dag(
    dag_id="0_aws_batch_test",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['mobility', 'aws_batch', 'test']
)
def aws_batch_test():
    try:
        neon = BaseHook.get_connection("neon_catalog_conn")
        aws = BaseHook.get_connection("aws_s3_conn")
        s3_bucket = aws.extra_dejson.get('bucket_name', 'ducklake-bdproject')
        s3_data_path = f"s3://{s3_bucket}/lakehouse/"

    except Exception as e:
        logging.error("❌ Connection 'neon_catalog_conn' not found.")
        raise e

    test_aws_sql = BatchOperator(
        task_id='test_aws_sql_connection',
        job_name='test_sql_job',
        job_definition='DuckJobDefinition', 
        job_queue='DuckJobQueue',                  
        region_name='eu-central-1',
        aws_conn_id='aws_s3_conn', 
        container_overrides={
            'command': [], 
            'environment': [
                {'name': 'memory', 'value': '2GB'},
                {'name': 'AWS_DEFAULT_REGION', 'value': 'eu-central-1'},
                {'name': 'CONTR_POSTGRES', 'value': neon.password},
                {'name': 'USUARIO_POSTGRES', 'value': neon.login},
                {'name': 'HOST_POSTGRES', 'value': neon.host},
                {'name': 'DATABASE_POSTGRES', 'value': 'neondb'}, 
                {'name': 'PUERTO_POSTGRES', 'value': '5432'},
                {'name': 'RUTA_S3_DUCKLAKE', 'value': s3_data_path},
                {
                    'name': 'SQL_QUERY', 
                    'value': "CREATE TABLE IF NOT EXISTS gold.test_on_aws AS (SELECT 1 as val); DROP TABLE gold.test_on_aws;"
                }
            ],
        }
    )

aws_batch_test()