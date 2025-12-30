from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param
from pendulum import datetime

@dag(
    dag_id="7_master_orchestrator",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="YYYYMMDD"),
        "end_date": Param("20230103", type="string", description="YYYYMMDD"),
    },
    tags=['orchestrator', 'mobility']
)
def master_orchestrator():

    # 1. Trigger Infrastructure (Always runs first)
    # This ensures schemas and static data exist before we try to load facts
    trigger_infra = TriggerDagRunOperator(
        task_id="trigger_infrastructure",
        trigger_dag_id="1_infrastructure_and_dimensions", 
        wait_for_completion=True,  # Block here until DAG 1 finishes successfully
        poke_interval=30,
        reset_dag_run=True         # Allow re-running even if previous run succeeded
    )

    # 2. Trigger Mobility Ingestion (Runs after Infra)
    trigger_mobility = TriggerDagRunOperator(
        task_id="trigger_mobility",
        trigger_dag_id="2_mobility_ingestion",
        wait_for_completion=True, # Block here until DAG 2 finishes
        poke_interval=30,
        reset_dag_run=True,
        # Pass the date parameters down to the child DAG
        conf={
            "start_date": "{{ params.start_date }}",
            "end_date": "{{ params.end_date }}"
        }
    )

    # 3. Trigger Gold Analytics (Future placeholder)
    # trigger_gold = TriggerDagRunOperator(...)

    # --- Dependencies ---
    # Strict Linear Execution
    trigger_infra >> trigger_mobility 
    # >> trigger_gold

master_orchestrator()