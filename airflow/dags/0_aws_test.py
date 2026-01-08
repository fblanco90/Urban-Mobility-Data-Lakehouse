from airflow.sdk import dag
from pendulum import datetime
from utils_db import run_batch_sql

@dag(
    dag_id="0_aws_batch_test",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['mobility', 'aws_batch', 'test']
)
def aws_batch_test():

    test_query = """
        CREATE TABLE IF NOT EXISTS gold.test_on_aws AS (SELECT 1 as val);
        DROP TABLE gold.test_on_aws;
    """

    task_test_sql = run_batch_sql(
        task_id='test_aws_sql_connection',
        sql_query=test_query,
        memory="2GB",
        vcpu=1
    )

aws_batch_test()
