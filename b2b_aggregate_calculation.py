# /path/to/your/airflow/dags/b2b_aggregate_calculation_dag.py
import pendulum
import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator

# --- Configuration ---
OLAP_CONN_ID = "b2b_sales_olap"

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
        sql_truncate = "TRUNCATE TABLE FactSalesMonthlyAggregate;"
        logging.info(f"Truncating {sql_truncate}...")
        try:
            hook_olap.run(sql_truncate)
            logging.info("Table FactSalesMonthlyAggregate truncated successfully.")
        except Exception as e:
            logging.error(f"Error truncating FactSalesMonthlyAggregate: {e}")
            raise

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
            raise

    task_calculate = calculate_and_load_aggregates()

    end = EmptyOperator(task_id='end_aggregate_calculation')

    start >> task_calculate >> end

b2b_aggregate_calculation_dag()
