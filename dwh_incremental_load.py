from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
}

def detect_changed_data(**kwargs):
    # Приклад: Витягуємо ID змінених/нових записів з OLTP
    hook = PostgresHook(postgres_conn_id='oltp_postgres')
    sql = """
    -- Заповнення staging-таблиці змінених даних з OLTP
    INSERT INTO staging_changed_data (RecordID, ChangeTime)
    SELECT RecordID, ChangeTime FROM OLTP_ChangeLog
    WHERE ChangeTime > (SELECT COALESCE(MAX(ProcessedTime), '2000-01-01') FROM staging_changed_data);
    """
    hook.run(sql)

def update_dimensions_scd2(**kwargs):
    hook = PostgresHook(postgres_conn_id='olap_b2b_sales')
    sql = """
    -- Приклад SCD2 для DimProduct:
    UPDATE DimProduct d
    SET ValidTo = CURRENT_DATE, IsCurrent = FALSE
    WHERE d.ProductID IN (
        SELECT ProductID FROM staging_changed_data WHERE <умова змін>
    ) AND d.IsCurrent = TRUE;

    INSERT INTO DimProduct (ProductID, ProductName, SeriesName, Price, ValidFrom, ValidTo, IsCurrent)
    SELECT ProductID, ProductName, SeriesName, Price, CURRENT_DATE, NULL, TRUE
    FROM OLTP_Products
    WHERE ProductID IN (
        SELECT ProductID FROM staging_changed_data WHERE <умова новизни або змін>
    );
    -- Аналогічно виконайте оновлення для інших вимірів.
    """
    hook.run(sql)

def update_fact(**kwargs):
    hook = PostgresHook(postgres_conn_id='olap_b2b_sales')
    sql = """
    INSERT INTO FactSalesPerformance (DateKey, AccountKey, ProductKey, SalesAgentKey, DealStageKey, CloseValue, DurationDays, ExpectedSuccessRate)
    SELECT d.DateKey, a.AccountID, p.ProductID, s.SalesAgentID, ds.StageID, sp.CloseValue, sp.DurationDays, sp.ExpectedSuccessRate
    FROM OLTP_SalesPipeline sp
    JOIN DimDate d ON sp.Date = d.Date
    JOIN DimAccount a ON sp.AccountID = a.AccountID
    JOIN DimProduct p ON sp.ProductID = p.ProductID
    JOIN DimSalesAgent s ON sp.SalesAgentID = s.SalesAgentID
    JOIN DimDealStage ds ON sp.DealStage = ds.StageName
    WHERE sp.RecordID IN (SELECT RecordID FROM staging_changed_data);
    """
    hook.run(sql)

def update_aggregate(**kwargs):
    hook = PostgresHook(postgres_conn_id='olap_b2b_sales')
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

    INSERT INTO FactSalesMonthlyAggregate (Year, Month, Region, DealStageKey, TotalDealCount, TotalCloseValue, AvgDealValue, AvgDurationDays, ExpectedSuccessRate)
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
    hook.run(sql)

def validate_incremental(**kwargs):
    hook = PostgresHook(postgres_conn_id='olap_b2b_sales')
    result = hook.get_records("SELECT COUNT(*) FROM FactSalesPerformance;")
    if result and result[0][0] < 1:
        raise ValueError("Incremental load validation failed: FactSalesPerformance appears empty.")
    else:
        print("Incremental load validation passed.")

with DAG(
    dag_id='dwh_incremental_load',
    default_args=default_args,
    description='Інкрементне оновлення OLAP-сховища (SCD2 для вимірів, інкрементне завантаження фактів та агрегованих таблиць)',
    schedule_interval='@daily',  # Або встановіть інтервал за потребою
    start_date=datetime(2020, 1, 1),
    catchup=False
) as dag:

    t_detect_changed_data = PythonOperator(
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

    t_validate_incremental = PythonOperator(
        task_id='validate_incremental',
        python_callable=validate_incremental,
        provide_context=True
    )

    t_detect_changed_data >> t_update_dimensions >> t_update_fact >> t_update_aggregate >> t_validate_incremental