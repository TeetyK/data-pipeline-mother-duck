from airflow import DAG , task
from airflow.operators.python import PythonOperator
from datetime import datetime
import os


with DAG(
    dag_id='etl_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule='@daily', 
    catchup=False
) as dag:

    @task.bash
    def bronze():
        return """
            cd F:\\data-pipeline-mother-duck\\transform
            uv run --with dbt-bigquery dbt build --select br_raw
            """
    @task.bash
    def silver():
        return """
            cd F:\\data-pipeline-mother-duck\\transform
            uv run --with dbt-bigquery dbt build --select sil_clean
            """
    @task.bash
    def gold():
        return """
            cd F:\\data-pipeline-mother-duck\\transform
            uv run --with dbt-bigquery dbt build --select gold_business
            """
    t1 = bronze()
    t2 = silver()
    t3 = gold()
    t1 >> t2 >> t3