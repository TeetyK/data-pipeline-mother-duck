from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator 
from datetime import datetime
import os


with DAG(
    dag_id='etl_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule='@daily', 
    catchup=False,
    tags=['etl','gcs'],
    default_args={'owner':'TeetyK','retries':2}
) as dag:

    bronze_task = BashOperator(
        task_id='bronze',
        bash_command='uv run --with dbt-bigquery dbt build --select br_raw',
        cwd='F:\\data-pipeline-mother-duck\\transform'
    )
    silver_task = BashOperator(
        task_id='silver',
        bash_command='uv run --with dbt-bigquery dbt build --select sil_clean',
        cwd='F:\\data-pipeline-mother-duck\\transform'
    )
    gold_task = BashOperator(
        task_id='gold',
        bash_command='uv run --with dbt-bigquery dbt build --select gold_business',
        cwd='F:\\data-pipeline-mother-duck\\transform'
    )

    bronze_task >> silver_task >> gold_task