from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import duckdb

def extract_data():
    print("กำลังดึงข้อมูลจาก API... (สมมติ)")
    return [{"id": 1, "name": "Teety", "status": "Learning DE"}]

def load_to_motherduck(ti):
    data = ti.xcom_pull(task_ids='extract_step')
    
    md_token = os.getenv("mother_duck_api")
    
    con = duckdb.connect(f"md:?motherduck_token={md_token}")
    
    con.execute("CREATE TABLE IF NOT EXISTS my_practice (id INT, name TEXT, status TEXT)")
    for row in data:
        con.execute(f"INSERT INTO my_practice VALUES ({row['id']}, '{row['name']}', '{row['status']}')")
    print("ส่งข้อมูลไป MotherDuck สำเร็จแล้ว!")

with DAG(
    dag_id='my_motherduck_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule='@daily', 
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_step',
        python_callable=extract_data
    )

    load_task = PythonOperator(
        task_id='load_step',
        python_callable=load_to_motherduck
    )

    extract_task >> load_task