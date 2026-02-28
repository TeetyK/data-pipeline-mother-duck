import duckdb
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
load_dotenv()
def main():
    print("Data pipeline with dags file")


if __name__ == "__main__":
    main()
