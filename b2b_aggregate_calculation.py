# /path/to/your/airflow/dags/b2b_aggregate_calculation_dag.py
import pendulum
import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
# Removed: from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator

# --- Configuration ---
OLAP_CONN_ID = "b2b_sales_olap" # Connection ID for your OLAP database
# Removed: SOURCE_INCREMENTAL_DAG_ID = "b2b_incremental_load"
# Removed: SOURCE_INCREMENTAL_TASK_ID = "end_incremental_load"

# --- DAG Definition ---
@dag(
    dag_id='b2b_aggregate_calculation_no_wait', # Changed ID slightly to avoid conflict
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), # Start date for scheduling
    schedule='@daily', # Schedule to run daily
    catchup=False, # Avoid running for past missed schedules
    default_args={
        'retries': 1, # Number of retries on failure
        'retry_delay': timedelta(minutes=5), # Delay between retries
        'owner': 'airflow', # Assign an owner
    },
    tags=['b2b_sales', 'aggregate', 'refined', 'no_wait'],
    doc_md="""
    ### B2B Sales Monthly Aggregate Calculation DAG (No Wait)

    **Завдання 3:** Обчислює місячні агрегати продажів на основі таблиці `FactSalesPerformance`
    і завантажує їх у `FactSalesMonthlyAggregate`.

    **Стратегія:** Truncate and Reload.

    **ЗАЛЕЖНІСТЬ ВИДАЛЕНО:** Цей DAG запускається за розкладом `@daily`
    **НЕЗАЛЕЖНО** від стану `b2b_incremental_load`.
    **ПОПЕРЕДЖЕННЯ:** Агрегати можуть бути обчислені на основі неповних даних,
    якщо цей DAG запуститься раніше, ніж `b2b_incremental_load` оновить `FactSalesPerformance`.
    """
)
def b2b_aggregate_calculation_no_wait_dag():

    start = EmptyOperator(task_id='start_aggregate_calculation')

    # REMOVED the ExternalTaskSensor definition

    @task
    def calculate_and_load_aggregates():
        """
        Truncates the aggregate table and re-calculates aggregates from FactSalesPerformance.
        Runs based on schedule, does not wait for upstream DAGs.
        """
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)

        # 1. Truncate the aggregate table
        sql_truncate = "TRUNCATE TABLE FactSalesMonthlyAggregate;"
        logging.info(f"Truncating {sql_truncate}...")
        try:
            hook_olap.run(sql_truncate)
            logging.info("Table FactSalesMonthlyAggregate truncated successfully.")
        except Exception as e:
            logging.error(f"Error truncating FactSalesMonthlyAggregate: {e}")
            raise # Fail the task if truncate fails

        # 2. Calculate and Insert Aggregates
        logging.info("Calculating and inserting aggregates into FactSalesMonthlyAggregate...")
        sql_aggregate = """
        INSERT INTO FactSalesMonthlyAggregate (
            Year, Month, Region, DealStageKey,
            TotalDealCount, TotalCloseValue, AvgDealValue,
            AvgDurationDays, ExpectedSuccessRate
        )
        SELECT
            d.Year, d.Month, COALESCE(sa.Region, 'Unknown') AS Region, f.DealStageKey,
            COUNT(*) AS TotalDealCount, SUM(f.CloseValue) AS TotalCloseValue,
            AVG(f.CloseValue) AS AvgDealValue, AVG(f.DurationDays) AS AvgDurationDays,
            AVG(f.ExpectedSuccessRate) AS ExpectedSuccessRate
        FROM FactSalesPerformance f
        JOIN DimDate d ON f.DateKey = d.DateKey
        JOIN DimSalesAgent sa ON f.SalesAgentKey = sa.SalesAgentID
        GROUP BY d.Year, d.Month, COALESCE(sa.Region, 'Unknown'), f.DealStageKey;
        """
        try:
            hook_olap.run(sql_aggregate)
            logging.info("Aggregates calculated and inserted successfully.")
        except Exception as e:
            logging.error(f"Error calculating/inserting aggregates: {e}")
            raise # Fail the task

    task_calculate = calculate_and_load_aggregates()

    end = EmptyOperator(task_id='end_aggregate_calculation')

    # Define dependencies - REMOVED wait_for_incremental
    start >> task_calculate >> end

# Instantiate the DAG
b2b_aggregate_calculation_no_wait_dag()
