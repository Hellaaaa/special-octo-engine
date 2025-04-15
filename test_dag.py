from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id='test_oltp_connection',
    start_date=datetime(2025, 4, 15),
    schedule_interval=None,
    catchup=False
) as dag:
    test_query = PostgresOperator(
        task_id='test_query',
        postgres_conn_id='b2b_sales',
        sql="SELECT 1;"
    )