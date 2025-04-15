from datetime import datetime, timedelta
import os
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

def run_query(sql):
    conn_str = os.getenv('OLAP_B2B_SALES_CONN')
    if not conn_str:
        raise ValueError("OLAP_B2B_SALES_CONN environment variable is not set")
    conn = psycopg2.connect(conn_str)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
    finally:
        conn.close()

def detect_changed_data(**kwargs):
    # Placeholder: Insert changed record identifiers into a staging table from OLTP source.
    # For real implementation, you may extract data using other means.
    sql = """
    INSERT INTO staging_changed_data (RecordID, ChangeTime)
    SELECT RecordID, ChangeTime FROM OLTP_ChangeLog
    WHERE ChangeTime > (SELECT COALESCE(MAX(ProcessedTime), '2000-01-01') FROM staging_changed_data);
    """
    run_query(sql)

def update_dimensions_scd2(**kwargs):
    # Example SCD2 logic for DimProduct. Extend this logic for other dimensions as needed.
    sql = """
    UPDATE DimProduct d
    SET ValidTo = CURRENT_DATE, IsCurrent = FALSE
    WHERE d.ProductID IN (
        SELECT ProductID FROM staging_changed_data WHERE <change_condition>
    ) AND d.IsCurrent = TRUE;

    INSERT INTO DimProduct (ProductID, ProductName, SeriesName, Price, ValidFrom, ValidTo, IsCurrent)
    SELECT ProductID, ProductName, SeriesName, Price, CURRENT_DATE, NULL, TRUE
    FROM OLTP_Products
    WHERE ProductID IN (
        SELECT ProductID FROM staging_changed_data WHERE <new_or_changed_condition>
    );
    """
    run_query(sql)

def update_fact(**kwargs):
    sql = """
    INSERT INTO FactSalesPerformance 
       (DateKey, AccountKey, ProductKey, SalesAgentKey, DealStageKey, CloseValue, DurationDays, ExpectedSuccessRate)
    SELECT d.DateKey, a.AccountID, p.ProductID, s.SalesAgentID, ds.StageID,
           sp.CloseValue, sp.DurationDays, sp.ExpectedSuccessRate
    FROM OLTP_SalesPipeline sp
    JOIN DimDate d ON sp.Date = d.Date
    JOIN DimAccount a ON sp.AccountID = a.AccountID
    JOIN DimProduct p ON sp.ProductID = p.ProductID
    JOIN DimSalesAgent s ON sp.SalesAgentID = s.SalesAgentID
    JOIN DimDealStage ds ON sp.DealStage = ds.StageName
    WHERE sp.RecordID IN (SELECT RecordID FROM staging_changed_data);
    """
    run_query(sql)

def update_aggregate(**kwargs):
    sql = """
    DELETE FROM FactSalesMonthlyAggregate
    WHERE (Year, Month, Region, DealStageKey) IN (
       SELECT d.Year, d.Month, s.Region, ds.StageID
       FROM FactSalesPerformance sp
       JOIN DimDate d ON sp.DateKey = d.DateKey
       JOIN DimSalesAgent s ON sp.SalesAgentKey = s.SalesAgentID
       JOIN DimDealStage ds ON sp.DealStageKey = ds.StageID
       WHERE sp.RecordID IN (SELECT RecordID FROM staging_changed_data)
    );

    INSERT INTO FactSalesMonthlyAggregate 
       (Year, Month, Region, DealStageKey, TotalDealCount, TotalCloseValue, AvgDealValue, AvgDurationDays, ExpectedSuccessRate)
    SELECT d.Year, d.Month, s.Region, ds.StageID,
           COUNT(*),
           SUM(sp.CloseValue),
           AVG(sp.CloseValue),
           AVG(sp.DurationDays),
           AVG(sp.ExpectedSuccessRate)
    FROM FactSalesPerformance sp
    JOIN DimDate d ON sp.DateKey = d.DateKey
    JOIN DimSalesAgent s ON sp.SalesAgentKey = s.SalesAgentID
    JOIN DimDealStage ds ON sp.DealStageKey = ds.StageID
    WHERE sp.RecordID IN (SELECT RecordID FROM staging_changed_data)
    GROUP BY d.Year, d.Month, s.Region, ds.StageID;
    """
    run_query(sql)

def validate_incremental(**kwargs):
    conn_str = os.getenv('OLAP_B2B_SALES_CONN')
    conn = psycopg2.connect(conn_str)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM FactSalesPerformance;")
                result = cur.fetchone()
                if result[0] < 1:
                    raise ValueError("Incremental load validation failed: FactSalesPerformance is empty.")
                else:
                    print("Incremental load validation passed.")
    finally:
        conn.close()

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
}

with DAG(
    dag_id='dwh_incremental_load',
    default_args=default_args,
    description='Incremental update of the OLAP warehouse including SCD type 2 updates and fact/aggregate load',
    schedule_interval='@daily',
    start_date=datetime(2020, 1, 1),
    catchup=False
) as dag:

    t_detect = PythonOperator(
        task_id='detect_changed_data',
        python_callable=detect_changed_data
    )

    t_update_dimensions = PythonOperator(
        task_id='update_dimensions_scd2',
        python_callable=update_dimensions_scd2
    )

    t_update_fact = PythonOperator(
        task_id='update_fact',
        python_callable=update_fact
    )

    t_update_aggregate = PythonOperator(
        task_id='update_aggregate',
        python_callable=update_aggregate
    )

    t_validate = PythonOperator(
        task_id='validate_incremental',
        python_callable=validate_incremental,
        provide_context=True
    )

    t_detect >> t_update_dimensions >> t_update_fact >> t_update_aggregate >> t_validate