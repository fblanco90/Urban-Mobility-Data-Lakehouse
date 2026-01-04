from airflow.sdk import dag, task
from pendulum import datetime
from utils_db import get_connection
import pandas as pd
from pyspainmobility import Zones
import logging


@dag(
    dag_id="00_connection_test",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test']
)
def connection_test():

    @task
    def test():
        """Creates Bronze, Silver, Gold schemas if they don't exist."""
        con = get_connection()
        
        logging.info("✅ Connected.")

        con.close()
    
    init = test()
    init

connection_test()
