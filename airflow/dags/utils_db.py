import duckdb
import os
from airflow.models import Variable

# Define the path for the DuckLake metadata
# In Astro/Docker, this maps to the 'include' folder which persists
LAKEHOUSE_DIR = "/usr/local/airflow/include/lakehouse"
METADATA_PATH = os.path.join(LAKEHOUSE_DIR, "metadata.duckdb")

def get_connection():
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