from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import duckdb
import pandas as pd
import os

# --- Configuration ---
API_URL = "https://jsonplaceholder.typicode.com/posts"

# --- Functions ---

def extract_data(**context):

    print(f"🚀 Connecting to {API_URL}")
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        data = data[:2]
        
        print(f"✅ Extracted {len(data)} records")
        return data
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise

def load_to_duckdb(**context):
    print(f"📥 Loading to DuckDB:")
    
    ti = context['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    

    try:
        df = pd.DataFrame(data)
        
        md_token = os.getenv("mother_duck_api")
    
        con = duckdb.connect(f"md:?motherduck_token={md_token}")
        
        con.execute("CREATE TABLE IF NOT EXISTS json_holders (userId INT, id INT , title TEXT, body TEXT)")
        for row in data:
            con.execute(f"INSERT INTO json_holders VALUES ({row['userId']}, '{row['id']}', '{row['title']}', '{row['body']}')")
        
        print(f"✅ Success! ")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise

# --- DAG Definition ---

default_args = {
    'owner': 'practice',
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
}

with DAG(
    'practice_my_practice_db',
    default_args=default_args,
    schedule=None,  
    catchup=False,
    tags=['practice', 'duckdb'],
) as dag:

    t1 = PythonOperator(task_id='extract_data', python_callable=extract_data)
    t2 = PythonOperator(task_id='load_to_duckdb', python_callable=load_to_duckdb)
    
    t1 >> t2