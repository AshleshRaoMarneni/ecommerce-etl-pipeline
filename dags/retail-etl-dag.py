from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="retail_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # Extract task
    extract = BashOperator(
        task_id="extract_sales",
        bash_command="python extract/extract_sales.py"
    )

    # Transform task
    transform = BashOperator(
        task_id="transform_sales",
        bash_command="python transform/transform_data.py"
    )

    # Load task
    load = BashOperator(
        task_id="load_sales",
        bash_command="python load/load_postgres.py"
    )

    # Set task order
    extract >> transform >> load

