# /path/to/your/airflow/dags/b2b_aggregate_calculation_dag.py
import pendulum
import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

# --- Configuration ---
OLAP_CONN_ID = "b2b_sales_olap"

# --- DAG Definition ---
@dag(
    dag_id='b2b_aggregate_calculation',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule='@daily', # Should align with or run after incremental
    catchup=False,
    tags=['b2b_sales', 'aggregate'],
    doc_md="""
    ### B2B Sales Monthly Aggregate Calculation DAG

    Calculates monthly sales aggregates based on the `FactSalesPerformance` table
    and loads them into `FactSalesMonthlyAggregate`.

    Uses a **Truncate and Reload** strategy.

    Waits for the `b2b_incremental_load` DAG to complete for the same logical date.
    """
)
def b2b_aggregate_calculation_dag():

    # Wait for the incremental load DAG to succeed for the same logical date
    wait_for_incremental = ExternalTaskSensor(
        task_id='wait_for_incremental_load',
        external_dag_id='b2b_incremental_load',
        external_task_id='end_incremental_load', # Wait for the end task
        allowed_states=['success'],
        execution_delta=timedelta(hours=0), # Check same logical day's run
        check_existence=True,
        mode='poke',
        poke_interval=60, # Check every 60 seconds
        timeout=3600 # Fail if upstream doesn't succeed in 1 hour
    )

    @task
    def calculate_and_load_aggregates():
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)

        # 1. Truncate the aggregate table
        logging.info(f"Truncating FactSalesMonthlyAggregate...")
        sql_truncate = "TRUNCATE TABLE FactSalesMonthlyAggregate;"
        hook_olap.run(sql_truncate)
        logging.info("Table truncated.")

        # 2. Calculate and Insert Aggregates
        # Note: Region comes from DimSalesAgent. Need to join facts -> DimSalesAgent.
        logging.info("Calculating and inserting aggregates...")
        sql_aggregate = """
        INSERT INTO FactSalesMonthlyAggregate (
            Year, Month, Region, DealStageKey,
            TotalDealCount, TotalCloseValue, AvgDealValue,
            AvgDurationDays, ExpectedSuccessRate -- Note: AVG(ExpectedSuccessRate) might not be meaningful
        )
        SELECT
            d.Year,
            d.Month,
            COALESCE(sa.Region, 'Unknown') AS Region, -- Get region from Sales Agent Dim
            f.DealStageKey,
            COUNT(*) AS TotalDealCount,
            SUM(f.CloseValue) AS TotalCloseValue,
            AVG(f.CloseValue) AS AvgDealValue,
            AVG(f.DurationDays) AS AvgDurationDays,
            AVG(f.ExpectedSuccessRate) AS ExpectedSuccessRate -- Average of rates
        FROM FactSalesPerformance f
        JOIN DimDate d ON f.DateKey = d.DateKey
        JOIN DimSalesAgent sa ON f.SalesAgentKey = sa.SalesAgentID
        -- Add JOIN DimDealStage if StageName is needed for grouping/filtering, not needed for key
        GROUP BY
            d.Year,
            d.Month,
            COALESCE(sa.Region, 'Unknown'),
            f.DealStageKey;
        """
        hook_olap.run(sql_aggregate)
        logging.info("Aggregates inserted.")
        # Could add row count verification here if needed

    task_calculate = calculate_and_load_aggregates()

    # Define dependencies
    wait_for_incremental >> task_calculate

# Instantiate the DAG
b2b_aggregate_calculation_dag()