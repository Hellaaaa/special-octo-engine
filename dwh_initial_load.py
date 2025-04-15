from datetime import datetime, timedelta
import os
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

# Utility function to run a SQL command using psycopg2
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

def create_tables(**kwargs):
    sql = """
    -- Create DimDate table
    CREATE TABLE IF NOT EXISTS DimDate (
        DateKey INT PRIMARY KEY,
        Date DATE NOT NULL,
        Day INT NOT NULL,
        Month INT NOT NULL,
        Quarter INT NOT NULL,
        Year INT NOT NULL
    );
    
    -- Create DimAccount table
    CREATE TABLE IF NOT EXISTS DimAccount (
        AccountID INT PRIMARY KEY,
        AccountName VARCHAR(255) NOT NULL,
        Sector VARCHAR(100),
        RevenueRange VARCHAR(50),
        ParentAccountID INT,
        FOREIGN KEY (ParentAccountID) REFERENCES DimAccount(AccountID)
    );
    
    -- Create DimProduct table (SCD Type 2)
    CREATE TABLE IF NOT EXISTS DimProduct (
        ProductID INT PRIMARY KEY,
        ProductName VARCHAR(255) NOT NULL,
        SeriesName VARCHAR(100),
        Price NUMERIC(12,2),
        PriceRange VARCHAR(50),
        ValidFrom DATE NOT NULL,
        ValidTo DATE,
        IsCurrent BOOLEAN NOT NULL DEFAULT TRUE
    );
    
    -- Create DimSalesAgent table
    CREATE TABLE IF NOT EXISTS DimSalesAgent (
        SalesAgentID INT PRIMARY KEY,
        SalesAgentName VARCHAR(255) NOT NULL,
        ManagerName VARCHAR(255),
        Region VARCHAR(100)
    );
    
    -- Create DimDealStage table
    CREATE TABLE IF NOT EXISTS DimDealStage (
        StageID INT PRIMARY KEY,
        StageName VARCHAR(50) NOT NULL
    );
    
    -- Create FactSalesPerformance table
    CREATE TABLE IF NOT EXISTS FactSalesPerformance (
        FactSalesPerformanceID SERIAL PRIMARY KEY,
        DateKey INT NOT NULL,
        AccountKey INT NOT NULL,
        ProductKey INT NOT NULL,
        SalesAgentKey INT NOT NULL,
        DealStageKey INT NOT NULL,
        CloseValue NUMERIC(12,2),
        DurationDays INT,
        ExpectedSuccessRate NUMERIC(5,2),
        FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
        FOREIGN KEY (AccountKey) REFERENCES DimAccount(AccountID),
        FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductID),
        FOREIGN KEY (SalesAgentKey) REFERENCES DimSalesAgent(SalesAgentID),
        FOREIGN KEY (DealStageKey) REFERENCES DimDealStage(StageID)
    );
    
    -- Create FactSalesMonthlyAggregate table
    CREATE TABLE IF NOT EXISTS FactSalesMonthlyAggregate (
        AggregateID SERIAL PRIMARY KEY,
        Year INT NOT NULL,
        Month INT NOT NULL,
        Region VARCHAR(100) NOT NULL,
        DealStageKey INT NOT NULL,
        TotalDealCount INT,
        TotalCloseValue DECIMAL(12,2),
        AvgDealValue DECIMAL(12,2),
        AvgDurationDays DECIMAL(8,2),
        ExpectedSuccessRate DECIMAL(5,2),
        FOREIGN KEY (DealStageKey) REFERENCES DimDealStage(StageID)
    );
    """
    run_query(sql)

def load_dimensions(**kwargs):
    # Placeholder SQL to load dimensions from the OLTP source.
    # Replace with your actual ETL logic.
    sql = """
    INSERT INTO DimAccount (AccountID, AccountName, Sector, RevenueRange, ParentAccountID)
    SELECT AccountID, AccountName, Sector, RevenueRange, ParentAccountID FROM OLTP_Accounts;
    """
    run_query(sql)

def load_fact(**kwargs):
    # Placeholder SQL to load data into FactSalesPerformance.
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
    JOIN DimDealStage ds ON sp.DealStage = ds.StageName;
    """
    run_query(sql)

def compute_aggregate(**kwargs):
    sql = """
    DELETE FROM FactSalesMonthlyAggregate;
    
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
    GROUP BY d.Year, d.Month, s.Region, ds.StageID;
    """
    run_query(sql)

def validate_initial_load(**kwargs):
    conn_str = os.getenv('OLAP_B2B_SALES_CONN')
    conn = psycopg2.connect(conn_str)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM FactSalesPerformance;")
                result = cur.fetchone()
                if result[0] < 1:
                    raise ValueError("Initial load validation failed: FactSalesPerformance is empty.")
                else:
                    print("Initial load validation passed.")
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
    dag_id='dwh_initial_load',
    default_args=default_args,
    description='Initial load into OLAP data warehouse using psycopg2 directly.',
    schedule_interval=None,
    start_date=datetime(2020, 1, 1),
    catchup=False
) as dag:

    t_create_tables = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables
    )

    t_load_dimensions = PythonOperator(
        task_id='load_dimensions',
        python_callable=load_dimensions
    )

    t_load_fact = PythonOperator(
        task_id='load_fact',
        python_callable=load_fact
    )

    t_compute_aggregate = PythonOperator(
        task_id='compute_aggregate',
        python_callable=compute_aggregate
    )

    t_validate = PythonOperator(
        task_id='validate_initial_load',
        python_callable=validate_initial_load,
        provide_context=True
    )

    t_create_tables >> t_load_dimensions >> t_load_fact >> t_compute_aggregate >> t_validate