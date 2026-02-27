import duckdb
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
load_dotenv()

def my_task():
    global md_token
    md_token = os.getenv("MOTHERDUCK_TOKEN")
def main():
    with DAG(dag_id="env_test_dag", start_date=datetime(2024, 1, 1), schedule=None) as dag:
        task = PythonOperator(
            task_id="print_env_task",
            python_callable=my_task)
    con = duckdb.connect(f"md:?motherduck_token={md_token}")

    con.sql("CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name VARCHAR)")
    con.sql("INSERT INTO test_table VALUES (1, 'Data Engineer Learner')")

    print(con.sql("SELECT * FROM test_table").df())


if __name__ == "__main__":
    main()
