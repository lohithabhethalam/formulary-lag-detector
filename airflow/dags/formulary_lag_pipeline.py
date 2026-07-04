"""
airflow/dags/formulary_lag_pipeline.py

Weekly pipeline for formulary lag detection.
Scheduled for Tuesday 6am — one day after NADAC's
Monday weekly data refresh.

DAG order:
fetch_lag_batch → spark_lag → dbt_run → dbt_test
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"

default_args = {
    "owner": "lohitha",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="formulary_lag_pipeline",
    description="Measures lag between FDA generic approval and NADAC appearance",
    schedule_interval="0 6 * * 2",  # Tuesday 6am
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["formulary", "nadac", "openfda"],
) as dag:

    fetch_lag_batch = BashOperator(
        task_id="fetch_lag_batch",
        bash_command=f"cd {PROJECT_DIR} && python3 -m ingest.fetch_lag_batch",
    )

    spark_lag = BashOperator(
        task_id="spark_lag",
        bash_command=f"cd {PROJECT_DIR} && python3 spark_lag.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT_DIR}/dbt && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT_DIR}/dbt && dbt test",
    )

    fetch_lag_batch >> spark_lag >> dbt_run >> dbt_test