# /path/to/your/airflow/dags/b2b_aggregate_calculation_dag.py
import pendulum
import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator

# --- Configuration ---
OLAP_CONN_ID = "b2b_sales_olap" # Connection ID for your OLAP database
SOURCE_INCREMENTAL_DAG_ID = "b2b_incremental_load" # The DAG ID this aggregate depends on
SOURCE_INCREMENTAL_TASK_ID = "end_incremental_load" # The final task in the source DAG to wait for

# --- DAG Definition ---
@dag(
    dag_id='b2b_aggregate_calculation', # Unique ID for this DAG
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), # Start date for scheduling
    schedule='@daily', # Schedule to run daily, adjust as needed (e.g., '0 1 * * *' for 1 AM UTC)
    catchup=False, # Avoid running for past missed schedules
    default_args={
        'retries': 1, # Number of retries on failure
        'retry_delay': timedelta(minutes=5), # Delay between retries
        'owner': 'airflow', # Assign an owner
    },
    tags=['b2b_sales', 'aggregate', 'refined'],
    doc_md="""
    ### B2B Sales Monthly Aggregate Calculation DAG

    **Завдання 3:** Обчислює місячні агрегати продажів на основі таблиці `FactSalesPerformance`
    і завантажує їх у `FactSalesMonthlyAggregate`.

    **Стратегія:** Truncate and Reload (Повне очищення та перезавантаження таблиці агрегатів).

    **Залежність:** Очікує на успішне завершення DAG `b2b_incremental_load`
    (конкретно завдання `end_incremental_load`) для тієї ж логічної дати перед початком обчислень.
    """
)
def b2b_aggregate_calculation_dag():

    start = EmptyOperator(task_id='start_aggregate_calculation')

    # Task to wait for the incremental load DAG to succeed for the same logical date
    # This ensures FactSalesPerformance is updated before aggregation starts.
    wait_for_incremental = ExternalTaskSensor(
        task_id='wait_for_incremental_load',
        external_dag_id=SOURCE_INCREMENTAL_DAG_ID,
        external_task_id=SOURCE_INCREMENTAL_TASK_ID, # Wait for the end task of the source DAG
        allowed_states=['success'], # Only proceed if the source task succeeded
        failed_states=['failed', 'skipped', 'upstream_failed'], # Fail this task if upstream failed/skipped
        execution_delta=timedelta(hours=0), # Check the run for the *same* logical date/interval
        check_existence=True, # Ensure the external DAG and task exist
        mode='poke', # Check periodically instead of occupying a worker slot constantly
        poke_interval=60, # Check every 60 seconds
        timeout=3600 # Fail if the upstream DAG doesn't succeed within 1 hour
    )

    @task
    def calculate_and_load_aggregates():
        """
        Truncates the aggregate table and re-calculates aggregates from FactSalesPerformance.
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
        # Fetches required attributes by joining FactSalesPerformance with dimensions.
        logging.info("Calculating and inserting aggregates into FactSalesMonthlyAggregate...")
        sql_aggregate = """
        INSERT INTO FactSalesMonthlyAggregate (
            Year, Month, Region, DealStageKey,
            TotalDealCount, TotalCloseValue, AvgDealValue,
            AvgDurationDays, ExpectedSuccessRate -- Note: AVG(ExpectedSuccessRate) might need business validation
        )
        SELECT
            d.Year,                                 -- Year from DimDate
            d.Month,                                -- Month from DimDate
            COALESCE(sa.Region, 'Unknown') AS Region, -- Region from DimSalesAgent, handle potential NULLs
            f.DealStageKey,                         -- DealStageKey directly from Fact table
            COUNT(*) AS TotalDealCount,             -- Count of opportunities in the group
            SUM(f.CloseValue) AS TotalCloseValue,   -- Sum of close values
            AVG(f.CloseValue) AS AvgDealValue,      -- Average close value
            AVG(f.DurationDays) AS AvgDurationDays, -- Average deal duration
            AVG(f.ExpectedSuccessRate) AS ExpectedSuccessRate -- Average success rate (interpret with care)
        FROM FactSalesPerformance f
        JOIN DimDate d ON f.DateKey = d.DateKey -- Join to get Year and Month
        JOIN DimSalesAgent sa ON f.SalesAgentKey = sa.SalesAgentID -- Join to get Region
        -- No need to join DimDealStage unless grouping by StageName is required
        -- Ensure fact table data integrity: assumes valid keys exist after incremental load
        GROUP BY
            d.Year,
            d.Month,
            COALESCE(sa.Region, 'Unknown'), -- Group by the same attributes being selected
            f.DealStageKey;
        """
        try:
            # Using hook.run which typically autocommits DML like INSERT by default
            # unless within an explicit transaction block managed elsewhere.
            hook_olap.run(sql_aggregate)
            # Optional: Add a row count check here if needed, e.g., using get_records
            # row_count = hook_olap.get_records("SELECT COUNT(*) FROM FactSalesMonthlyAggregate")[0][0]
            # logging.info(f"Successfully inserted aggregates. Row count: {row_count}")
            logging.info("Aggregates calculated and inserted successfully.")
        except Exception as e:
            logging.error(f"Error calculating/inserting aggregates: {e}")
            raise # Fail the task

    task_calculate = calculate_and_load_aggregates()

    end = EmptyOperator(task_id='end_aggregate_calculation')

    # Define dependencies
    start >> wait_for_incremental >> task_calculate >> end

# Instantiate the DAG
b2b_aggregate_calculation_dag()
