from datetime import datetime, timedelta
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

def run_query(conn_id: str, sql: str):
    conn_obj = BaseHook.get_connection(conn_id)
    conn_str = (
        f"postgresql://{conn_obj.login}:{conn_obj.password}@"
        f"{conn_obj.host}:{conn_obj.port}/{conn_obj.schema}"
    )
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()

def detect_changed_data(**kwargs):
    # This example assumes there is an OLTP change log table.
    sql = """
    INSERT INTO staging_changed_data (RecordID, ChangeTime)
    SELECT RecordID, ChangeTime FROM OLTP_ChangeLog
    WHERE ChangeTime > (SELECT COALESCE(MAX(ProcessedTime), '2000-01-01') FROM staging_changed_data);
    """
    # Using the OLTP connection (oltp_b2b_sales) for extraction
    run_query("oltp_b2b_sales", sql)

def update_dimensions_scd2(**kwargs):
    # Example SCD Type 2 update for DimProduct in OLAP.
    sql = """
    -- Close the current version if a change is detected.
    UPDATE DimProduct
    SET ValidTo = CURRENT_DATE, IsCurrent = FALSE
    WHERE ProductID IN (
        SELECT ProductID FROM staging_changed_data WHERE <change_condition>
    ) AND IsCurrent = TRUE;
    
    -- Insert new version for the changed records.
    INSERT INTO DimProduct (ProductID, ProductName, SeriesName, Price, ValidFrom, ValidTo, IsCurrent)
    SELECT ProductID, ProductName, SeriesName, Price, CURRENT_DATE, NULL, TRUE
    FROM OLTP_Products
    WHERE ProductID IN (
        SELECT ProductID FROM staging_changed_data WHERE <new_or_changed_condition>
    );
    """
    run_query("b2b_sales_olap", sql)

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
    run_query("b2b_sales_olap", sql)

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
    run_query("b2b_sales_olap", sql)

def validate_incremental(**kwargs):
    conn_obj = BaseHook.get_connection("b2b_sales_olap")
    conn_str = (
        f"postgresql://{conn_obj.login}:{conn_obj.password}@"
        f"{conn_obj.host}:{conn_obj.port}/{conn_obj.schema}"
    )
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM FactSalesPerformance;")
            result = cur.fetchone()
            if result[0] < 1:
                raise ValueError("Incremental load validation failed: FactSalesPerformance is empty.")
            else:
                print("Incremental load validation passed.")

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dwh_incremental_update',
    default_args=default_args,
    description='Incremental update from OLTP to OLAP (dimensions, fact, aggregate) with modifications',
    schedule_interval='@daily',
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=['incremental', 'olap_update']
) as dag:

    t_detect_changed = PythonOperator(
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

    t_detect_changed >> t_update_dimensions >> t_update_fact >> t_update_aggregate >> t_validate