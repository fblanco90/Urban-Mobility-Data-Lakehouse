import duckdb
import os
from airflow.sdk.bases.hook import BaseHook
import logging

def get_connection():
    """
    Returns a DuckDB connection configured for Neon (Metadata) + S3 (Storage)
    using the Secret-based DuckLake configuration.
    """
    # 1. Connect to In-Memory Compute
    con = duckdb.connect(database=':memory:')
    
    # 2. Load Extensions
    # We need postgres_scanner to handle the Postgres Secret type
    extensions = ['ducklake', 'spatial', 'httpfs', 'postgres_scanner']
    for ext in extensions:
        con.execute(f"INSTALL {ext}; LOAD {ext};")

    try:
        # Retrieve Connections from Airflow
        aws = BaseHook.get_connection("aws_s3_conn")
        neon = BaseHook.get_connection("neon_catalog_conn")
        
        # Get S3 Bucket from Extra, default to a sensible name if missing
        s3_bucket = aws.extra_dejson.get('bucket_name', 'ducklake-dbproject')
        region = aws.extra_dejson.get('region_name', 'eu-central-1')

        logging.info(f"⚙️ Configuring Lakehouse. Metadata: {neon.host}, Storage: s3://{s3_bucket}")

        # =========================================================================
        # 3. CONFIGURE SECRETS (Replicating your working example)
        # =========================================================================

        # A. S3 Secret (For Data Storage Access)
        # We use explicit keys from Airflow instead of 'credential_chain' to ensure
        # it works on workers without IAM roles.
        con.execute(f"""
            CREATE OR REPLACE SECRET secreto_s3 (
                TYPE S3,
                KEY_ID '{aws.login}',
                SECRET '{aws.password}',
                REGION '{region}'
            );
        """)

        # B. Postgres Secret (For Neon Metadata Access)
        # Maps Airflow connection fields to DuckDB Postgres Secret
        con.execute(f"""
            CREATE OR REPLACE SECRET secreto_postgres (
                TYPE POSTGRES,
                HOST '{neon.host}',
                PORT {neon.port or 5432},
                DATABASE '{neon.schema}',
                USER '{neon.login}',
                PASSWORD '{neon.password}'
            );
        """)
        # Note: Added SSL_MODE 'require' as Neon usually mandates it.

        # C. DuckLake Secret (The Bridge)
        # Links the DuckLake logic to the Postgres secret
        con.execute("""
            CREATE OR REPLACE SECRET secreto_ducklake (
                TYPE ducklake,
                METADATA_PATH '',
                METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'secreto_postgres'}
            );
        """)

        # =========================================================================
        # 4. ATTACH
        # =========================================================================
        
        # We attach using the secret we just created.
        # DATA_PATH points to where the Parquet files will live on S3.
        s3_data_path = f"s3://{s3_bucket}/lakehouse/"
        
        attach_query = f"""
            ATTACH 'ducklake:secreto_ducklake' AS lakehouse 
            (DATA_PATH '{s3_data_path}')
        """
        
        con.execute(attach_query)
        logging.info(f"✅ Lakehouse successfully attached! (Neon + S3)")

    except Exception as e:
        logging.error(f"❌ Critical Error connecting to Lakehouse: {e}")
        raise e
    
    return con

LAKEHOUSE_DIR = "/usr/local/airflow/include/lakehouse"
METADATA_PATH = os.path.join(LAKEHOUSE_DIR, "metadata.duckdb")

def get_connection_local():
    """
    Returns a DuckDB connection with DuckLake, Spatial, and HTTPFS loaded.
    """
    # 1. Ensure the directory exists
    os.makedirs(LAKEHOUSE_DIR, exist_ok=True)
    
    # 2. Connect to In-Memory Compute
    con = duckdb.connect(database=':memory:')
    
    # 3. Load Extensions
    # These allow us to read remote CSVs (httpfs), handle geometry (spatial), 
    # and manage the table catalog (ducklake).
    extensions = ['ducklake', 'spatial', 'httpfs']
    for ext in extensions:
        con.execute(f"INSTALL {ext}; LOAD {ext};")
        
    # 4. Attach the Lakehouse (Storage Layer)
    # The 'ducklake:' prefix tells DuckDB to manage this as a lakehouse
    con.execute(f"ATTACH 'ducklake:{METADATA_PATH}' AS lakehouse")
    
    return con
