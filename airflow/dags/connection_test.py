from airflow.decorators import dag, task
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
from pyspainmobility import Zones
import logging

# --- CONFIGURATION ---
# Update these URLs if MITMA/INE changes the direct download links

SOURCES_CONFIG = [
    {
        'table_name': 'zoning_municipalities',
        'url': 'https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/nombres_municipios.csv',
        'sep': '|',
        'header': True
    },
    {
        'table_name': 'population_municipalities',
        'url': 'https://movilidad-opendata.mitma.es/zonificacion/zonificacion_municipios/poblacion_municipios.csv',
        'sep': '|',
        'header': False # Usually this file has no header or a weird one
    },
    {
        'table_name': 'mapping_ine_mitma',
        'url': 'https://movilidad-opendata.mitma.es/zonificacion/relacion_ine_zonificacionMitma.csv',
        'sep': '|',
        'header': True
    },
    {
        'table_name': 'work_calendars',
        'url': 'https://datos.madrid.es/egob/catalogo/300082-4-calendario_laboral.csv',
        'sep': ';',
        'header': True
    }
]

@dag(
    dag_id="connection_test",
    start_date=datetime(2023, 1, 1),
    schedule=None, # Triggered manually or by Master DAG
    catchup=False,
    tags=['mobility', 'setup', 'static']
)
def connection_test():

    @task
    def test():
        """Creates Bronze, Silver, Gold schemas if they don't exist."""
        con = get_connection()
        
        logging.info("✅ Connected.")

        con.close()

    
    
    # --- DAG FLOW ---
    # 1. Create Schema
    # 2. Parallel: Ingest Geo + Ingest CSVs
    # 3. Build Silver (Wait for all Bronze)
    
    init = test()
    


    # Parallel when not in local
    # init >> [geo, csvs] >> silver
    init

connection_test()
