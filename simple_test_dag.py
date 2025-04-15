from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print("Hello, world!")
    return "Hello, world!"

with DAG(
    dag_id='simple_test_dag',
    start_date=datetime(2025, 4, 15),
    schedule_interval='@once',  # Виконується один раз
    catchup=False,
    description='Простий DAG для тестування gitSync'
) as dag:
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=hello_world
    )