import duckdb
import os
import logging
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.hooks.base import BaseHook

def get_connection():
    """
    Returns a DuckDB connection. 
    Detects if it's running in AWS Batch or locally in Airflow.
    """
    # 1. Start DuckDB
    con = duckdb.connect(database=':memory:')
    
    # 2. Load Extensions
    extensions = ['ducklake', 'spatial', 'httpfs', 'postgres_scanner']
    for ext in extensions:
        con.execute(f"INSTALL {ext}; LOAD {ext};")

    # 3. Detect Environment
    is_remote = os.getenv('AWS_BATCH_JOB_ID') is not None

    if is_remote:
        logging.info("☁️ Detected Remote Environment (AWS Batch)")
        # --- REMOTE CONFIGURATION ---
        
        # A. S3 Secret: Use IAM (No keys needed! Uses the Batch Job Role)
        con.execute("CREATE OR REPLACE SECRET secreto_s3 (TYPE S3, PROVIDER 'iam');")

        # B. Postgres Secret: Use Env Vars injected by Airflow BatchOperator
        host = os.getenv('NEON_HOST')
        user = os.getenv('NEON_USER')
        password = os.getenv('NEON_PASSWORD')
        port = os.getenv('NEON_PORT', '5432')
        schema = os.getenv('NEON_SCHEMA', 'neondb')
        s3_bucket = os.getenv('S3_BUCKET', 'ducklake-dbproject')
        region = os.getenv('AWS_REGION', 'eu-central-1')

    else:
        logging.info("💻 Detected Local Environment (Airflow/Astro)")
        from airflow.sdk.bases.hook import BaseHook
        
        aws = BaseHook.get_connection("aws_s3_conn")
        neon = BaseHook.get_connection("neon_catalog_conn")
        
        s3_bucket = aws.extra_dejson.get('bucket_name', 'ducklake-dbproject')
        region = aws.extra_dejson.get('region_name', 'eu-central-1')
        host, user, password, port, schema = neon.host, neon.login, neon.password, neon.port, neon.schema

        con.execute(f"""
            CREATE OR REPLACE SECRET secreto_s3 (
                TYPE S3,
                KEY_ID '{aws.login}',
                SECRET '{aws.password}',
                REGION '{region}'
            );
        """)

    # =========================================================================
    # 4. UNIVERSAL SECRETS & ATTACH (Same for both)
    # =========================================================================

    # Postgres Secret for Neon
    con.execute(f"""
        CREATE OR REPLACE SECRET secreto_postgres (
            TYPE POSTGRES,
            HOST '{host}',
            PORT {port},
            DATABASE '{schema}',
            USER '{user}',
            PASSWORD '{password}'
        );
    """)

    # DuckLake Bridge
    con.execute("""
        CREATE OR REPLACE SECRET secreto_ducklake (
            TYPE ducklake,
            METADATA_PATH '',
            METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'secreto_postgres'}
        );
    """)

    # Attach Lakehouse
    s3_data_path = f"s3://{s3_bucket}/lakehouse/"
    con.execute(f"ATTACH 'ducklake:secreto_ducklake' AS lakehouse (DATA_PATH '{s3_data_path}')")
    
    logging.info(f"✅ Connected to Lakehouse at s3://{s3_bucket}")
    return con

def run_batch_sql(task_id, sql_query, memory="12GB"):
    """
    Standardizes the creation of BatchOperators for the duckrunner image.
    """
    neon = BaseHook.get_connection("neon_catalog_conn")
    aws = BaseHook.get_connection("aws_s3_conn")
    s3_bucket = aws.extra_dejson.get('bucket_name', 'ducklake-dbproject')
    s3_data_path = f"s3://{s3_bucket}/lakehouse/"

    return BatchOperator(
        task_id=task_id,
        job_name=f"job_{task_id}",
        job_definition='DuckJobDefinition',
        job_queue='DuckJobQueue',
        region_name='eu-central-1',
        aws_conn_id='aws_s3_conn',
        container_overrides={
            'command': [],
            'environment': [
                {'name': 'memory', 'value': memory},
                {'name': 'AWS_DEFAULT_REGION', 'value': 'eu-central-1'},
                {'name': 'CONTR_POSTGRES', 'value': neon.password},
                {'name': 'USUARIO_POSTGRES', 'value': neon.login},
                {'name': 'HOST_POSTGRES', 'value': neon.host},
                {'name': 'DATABASE_POSTGRES', 'value': 'neondb'},
                {'name': 'PUERTO_POSTGRES', 'value': '5432'},
                {'name': 'RUTA_S3_DUCKLAKE', 'value': s3_data_path},
                {'name': 'SQL_QUERY', 'value': sql_query}
            ],
        }
    )
