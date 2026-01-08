from airflow.sdk import dag, task
from pendulum import datetime
from utils_db import get_connection
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
    def test() -> None:
        """
        Verifies the database connection integrity by attempting to establish 
        a session through the connection manager and logging a success message.
        """
        with get_connection() as con:
            logging.info("✅ Connected.")
        logging.info("✅ Connection verified.")

    init = test()
    init

connection_test()
