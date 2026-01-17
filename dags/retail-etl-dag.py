from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import os

# Absolute path to project root
project_dir = "/Users/adityaraomarneni/ecommerce-etl-pipeline"

# DAG definition
dag = DAG(
    'retail_etl_pipeline',
    description='ETL pipeline for retail sales',
    schedule_interval=None,  # or set like '0 6 * * *' for 6 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False
)

# BashOperator for extract
extract_task = BashOperator(
    task_id='extract_sales',
    bash_command=f'python {os.path.join(project_dir, "extract/extract_sales.py")}',
    dag=dag
)

# BashOperator for transform
transform_task = BashOperator(
    task_id='transform_data',
    bash_command=f'python {os.path.join(project_dir, "transform/transform_data.py")}',
    dag=dag
)

# BashOperator for load
# Here we assume your load_postgres.py is updated to use Airflow connection named 'postgres_default'
load_task = BashOperator(
    task_id='load_postgres',
    bash_command=f'python {os.path.join(project_dir, "load/load_postgres.py")}',
    dag=dag
)

# Set task dependencies
extract_task >> transform_task >> load_task


