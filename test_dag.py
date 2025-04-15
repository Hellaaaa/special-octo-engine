import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

def run_query():
    # Retrieve the connection details for 'b2b_sales'
    conn_obj = BaseHook.get_connection("b2b_sales")
    
    host = conn_obj.host
    port = conn_obj.port or 5432
    user = conn_obj.login
    password = conn_obj.password
    database = conn_obj.schema  # Note: in Airflow, the 'schema' usually represents the DB name

    # Connect to Postgres using psycopg2
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=database
    )
    
    try:
        cur = conn.cursor()
        # Execute the query; for instance, testing the connection with "SELECT 1;"
        cur.execute("SELECT 1;")
        result = cur.fetchone()
        print("Query result:", result)
        cur.close()
    finally:
        conn.close()

# Define the DAG
with DAG(
    dag_id='test_oltp_connection',
    start_date=datetime(2025, 4, 15),
    schedule_interval=None,
    catchup=False
) as dag:

    test_query = PythonOperator(
        task_id='test_query',
        python_callable=run_query
    )