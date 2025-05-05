# /path/to/your/airflow/dags/b2b_aggregate_calculation_dag.py
import pendulum
import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator

# --- Configuration ---
OLAP_CONN_ID = "crm_system_olap"

# --- DAG Definition ---
@dag(
    dag_id='b2b_aggregate_calculation',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule='@daily',
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=15),
        'owner': 'airflow',
    },
    tags=['b2b_sales', 'aggregate'],
    doc_md="""
    ### B2B Sales Monthly Aggregate Calculation DAG

    **Завдання 3:** Обчислює місячні агрегати продажів на основі таблиці `FactSalesPerformance`
    і завантажує їх у `FactSalesMonthlyAggregate`.

    **Стратегія:** Truncate and Reload.

    **НЕЗАЛЕЖНО** від стану `b2b_incremental_load`.
    **ПОПЕРЕДЖЕННЯ:** Агрегати можуть бути обчислені на основі неповних даних,
    якщо цей DAG запуститься раніше, ніж `b2b_incremental_load` оновить `FactSalesPerformance`.
    """
)
def b2b_aggregate_calculation_dag():

    start = EmptyOperator(task_id='start_aggregate_calculation')

    @task
    def calculate_and_load_aggregates():
        """
        Truncates the aggregate table and re-calculates aggregates from FactSalesPerformance.
        Runs based on schedule, does not wait for upstream DAGs. No automatic retries on failure.
        """
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        sql_truncate = "TRUNCATE TABLE factsalesmonthlyaggregate;"
        logging.info(f"Truncating {sql_truncate}...")
        try:
            hook_olap.run(sql_truncate)
            logging.info("Table factsalesmonthlyaggregate truncated successfully.")
        except Exception as e:
            logging.error(f"Error truncating factsalesmonthlyaggregate: {e}")
            raise

        logging.info("Calculating and inserting aggregates into factsalesmonthlyaggregate...")
        sql_aggregate = """
        INSERT INTO factsalesmonthlyaggregate (
            year, month, region, dealstagekey,
            totaldealcount, totalclosevalue, avgdealvalue,
            avgdurationdays, expectedsuccessrate
        )
        SELECT
            d.year, d.month, COALESCE(sa.region, 'Unknown') AS region, f.dealstagekey,
            COUNT(*) AS totaldealcount, SUM(f.closevalue) AS totalclosevalue,
            AVG(f.closevalue) AS avgdealvalue, AVG(f.durationdays) AS avgdurationdays,
            AVG(f.expectedsuccessrate) AS expectedsuccessrate
        FROM factsalesperformance f
        JOIN dimdate d ON f.datekey = d.datekey
        JOIN dimsalesagent sa ON f.salesagentkey = sa.salesagentid
        GROUP BY d.year, d.month, COALESCE(sa.region, 'Unknown'), f.dealstagekey;
        """
        try:
            hook_olap.run(sql_aggregate)
            logging.info("Aggregates calculated and inserted successfully.")
        except Exception as e:
            logging.error(f"Error calculating/inserting aggregates: {e}")
            raise

    task_calculate = calculate_and_load_aggregates()

    end = EmptyOperator(task_id='end_aggregate_calculation')

    start >> task_calculate >> end

b2b_aggregate_calculation_dag()
