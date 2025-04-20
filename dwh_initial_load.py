from datetime import datetime, timedelta
import logging
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from psycopg2 import sql
import json

# Configure logging
logger = logging.getLogger(__name__)

# Batch size for processing large datasets
BATCH_SIZE = 10000  # Adjust based on your system's capacity

def run_query(conn_id: str, sql_query: str, params=None):
    """
    Execute a SQL query using the specified connection ID.
    """
    conn_obj = BaseHook.get_connection(conn_id)
    conn_str = (
        f"postgresql://{conn_obj.login}:{conn_obj.password}@"
        f"{conn_obj.host}:{conn_obj.port}/{conn_obj.schema}"
    )
    
    try:
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query, params)
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        conn.rollback() if 'conn' in locals() and conn else None
        raise

def fetch_data(conn_id: str, sql_query: str, params=None):
    """
    Fetch data from database using the specified connection ID.
    """
    conn_obj = BaseHook.get_connection(conn_id)
    conn_str = (
        f"postgresql://{conn_obj.login}:{conn_obj.password}@"
        f"{conn_obj.host}:{conn_obj.port}/{conn_obj.schema}"
    )
    
    try:
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query, params)
                return cur.fetchall()
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise

def create_tables(**kwargs):
    """
    Create dimension and fact tables in the OLAP database.
    """
    # Create metadata table to track loading progress
    metadata_sql = """
    CREATE TABLE IF NOT EXISTS LoadingMetadata (
        table_name VARCHAR(100) PRIMARY KEY,
        last_loaded_id INT,
        last_loaded_timestamp TIMESTAMP,
        status VARCHAR(50)
    );
    
    -- Create error log table
    CREATE TABLE IF NOT EXISTS ErrorLog (
        error_id SERIAL PRIMARY KEY,
        table_name VARCHAR(100),
        record_id VARCHAR(100),
        error_message TEXT,
        error_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    run_query("b2b_sales_olap", metadata_sql)
    
    # Create dimension and fact tables
    tables_sql = """
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
        ValidFrom DATE NOT NULL,
        ValidTo DATE,
        IsCurrent BOOLEAN NOT NULL DEFAULT TRUE,
        FOREIGN KEY (ParentAccountID) REFERENCES DimAccount(AccountID)
    );
    
    -- Create DimProduct table (SCD Type 2)
    CREATE TABLE IF NOT EXISTS DimProduct (
        ProductID INT,
        SourceProductID INT NOT NULL,
        ProductName VARCHAR(255) NOT NULL,
        SeriesName VARCHAR(100),
        Price NUMERIC(12,2),
        PriceRange VARCHAR(50),
        ValidFrom DATE NOT NULL,
        ValidTo DATE,
        IsCurrent BOOLEAN NOT NULL DEFAULT TRUE,
        PRIMARY KEY (ProductID, ValidFrom)
    );
    
    -- Create DimSalesAgent table
    CREATE TABLE IF NOT EXISTS DimSalesAgent (
        SalesAgentID INT PRIMARY KEY,
        SalesAgentName VARCHAR(255) NOT NULL,
        ManagerName VARCHAR(255),
        Region VARCHAR(100),
        ValidFrom DATE NOT NULL,
        ValidTo DATE,
        IsCurrent BOOLEAN NOT NULL DEFAULT TRUE
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
        SourceTransactionID INT,
        FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
        FOREIGN KEY (AccountKey) REFERENCES DimAccount(AccountID),
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
        LastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (DealStageKey) REFERENCES DimDealStage(StageID)
    );
    """
    run_query("b2b_sales_olap", tables_sql)
    
    # Initialize metadata for each table
    init_metadata_sql = """
    INSERT INTO LoadingMetadata (table_name, last_loaded_id, last_loaded_timestamp, status)
    VALUES 
        ('DimDate', 0, '2000-01-01', 'pending'),
        ('DimAccount', 0, '2000-01-01', 'pending'),
        ('DimProduct', 0, '2000-01-01', 'pending'),
        ('DimSalesAgent', 0, '2000-01-01', 'pending'),
        ('DimDealStage', 0, '2000-01-01', 'pending'),
        ('FactSalesPerformance', 0, '2000-01-01', 'pending')
    ON CONFLICT (table_name) DO NOTHING;
    """
    run_query("b2b_sales_olap", init_metadata_sql)

def generate_date_dimension(**kwargs):
    """
    Generate and load date dimension for a range of years.
    """
    # Update metadata to 'in progress'
    run_query("b2b_sales_olap", 
              "UPDATE LoadingMetadata SET status = 'in_progress' WHERE table_name = 'DimDate'")
    
    try:
        # Generate dates for 10 years (adjust as needed)
        date_sql = """
        WITH RECURSIVE DateSeries AS (
            SELECT DATE '2015-01-01' AS date
            UNION ALL
            SELECT date + INTERVAL '1 day'
            FROM DateSeries
            WHERE date < DATE '2025-12-31'
        )
        SELECT 
            TO_CHAR(date, 'YYYYMMDD')::INT AS DateKey,
            date AS Date,
            EXTRACT(DAY FROM date)::INT AS Day,
            EXTRACT(MONTH FROM date)::INT AS Month,
            EXTRACT(QUARTER FROM date)::INT AS Quarter,
            EXTRACT(YEAR FROM date)::INT AS Year
        FROM DateSeries
        """
        
        dates = fetch_data("b2b_sales_olap", date_sql)
        
        # Process in batches
        for i in range(0, len(dates), BATCH_SIZE):
            batch = dates[i:i+BATCH_SIZE]
            
            # Prepare batch insert
            insert_sql = """
            INSERT INTO DimDate (DateKey, Date, Day, Month, Quarter, Year)
            VALUES %s
            ON CONFLICT (DateKey) DO NOTHING
            """
            
            # Convert to string format for executemany
            values = []
            for date_row in batch:
                values.append(f"({date_row[0]}, '{date_row[1]}', {date_row[2]}, {date_row[3]}, {date_row[4]}, {date_row[5]})")
            
            batch_sql = insert_sql % ",".join(values)
            run_query("b2b_sales_olap", batch_sql)
            
        # Update metadata to 'completed'
        run_query("b2b_sales_olap", 
                  "UPDATE LoadingMetadata SET status = 'completed', last_loaded_timestamp = CURRENT_TIMESTAMP WHERE table_name = 'DimDate'")
        
    except Exception as e:
        # Log error and update metadata
        run_query("b2b_sales_olap", 
                  "UPDATE LoadingMetadata SET status = 'failed' WHERE table_name = 'DimDate'")
        logger.error(f"Error generating date dimension: {e}")
        raise

def load_deal_stages(**kwargs):
    """
    Load deal stages dimension from OLTP to OLAP.
    """
    # Update metadata to 'in progress'
    run_query("b2b_sales_olap", 
              "UPDATE LoadingMetadata SET status = 'in_progress' WHERE table_name = 'DimDealStage'")
    
    try:
        # Get last loaded ID
        last_id_result = fetch_data("b2b_sales_olap", 
                                   "SELECT last_loaded_id FROM LoadingMetadata WHERE table_name = 'DimDealStage'")
        last_id = last_id_result[0][0] if last_id_result else 0
        
        # Get deal stages from OLTP
        stages = fetch_data("b2b_sales", 
                           f"SELECT stage_id, stage_name FROM deal_stages WHERE stage_id > {last_id} ORDER BY stage_id")
        
        if not stages:
            logger.info("No new deal stages to load")
            run_query("b2b_sales_olap", 
                     "UPDATE LoadingMetadata SET status = 'completed', last_loaded_timestamp = CURRENT_TIMESTAMP WHERE table_name = 'DimDealStage'")
            return
        
        # Process in batches
        for i in range(0, len(stages), BATCH_SIZE):
            batch = stages[i:i+BATCH_SIZE]
            
            # Prepare batch insert
            insert_sql = """
            INSERT INTO DimDealStage (StageID, StageName)
            VALUES %s
            ON CONFLICT (StageID) DO UPDATE SET StageName = EXCLUDED.StageName
            """
            
            # Convert to string format for executemany
            values = []
            for stage in batch:
                cleaned_stage_name = stage[1].replace("'", "''")
                values.append(f"({stage[0]}, '{cleaned_stage_name}')")
            
            batch_sql = insert_sql % ",".join(values)
            run_query("b2b_sales_olap", batch_sql)
        
        # Update metadata
        max_id = stages[-1][0]
        run_query("b2b_sales_olap", 
                 f"UPDATE LoadingMetadata SET last_loaded_id = {max_id}, status = 'completed', last_loaded_timestamp = CURRENT_TIMESTAMP WHERE table_name = 'DimDealStage'")
        
    except Exception as e:
        # Log error and update metadata
        run_query("b2b_sales_olap", 
                 "UPDATE LoadingMetadata SET status = 'failed' WHERE table_name = 'DimDealStage'")
        logger.error(f"Error loading deal stages: {e}")
        raise

def load_accounts(**kwargs):
    """
    Load account dimension from OLTP to OLAP.
    """
    # Update metadata to 'in progress'
    run_query("b2b_sales_olap", 
              "UPDATE LoadingMetadata SET status = 'in_progress' WHERE table_name = 'DimAccount'")
    
    try:
        # Get last loaded ID
        last_id_result = fetch_data("b2b_sales_olap", 
                                   "SELECT last_loaded_id FROM LoadingMetadata WHERE table_name = 'DimAccount'")
        last_id = last_id_result[0][0] if last_id_result else 0
        
        # Get accounts from OLTP
        accounts = fetch_data("b2b_sales", 
                             f"""
                             SELECT account_id, account_name, sector, revenue_range, parent_account_id 
                             FROM accounts 
                             WHERE account_id > {last_id} 
                             ORDER BY account_id
                             """)
        
        if not accounts:
            logger.info("No new accounts to load")
            run_query("b2b_sales_olap", 
                     "UPDATE LoadingMetadata SET status = 'completed', last_loaded_timestamp = CURRENT_TIMESTAMP WHERE table_name = 'DimAccount'")
            return
        
        # Process in batches
        for i in range(0, len(accounts), BATCH_SIZE):
            batch = accounts[i:i+BATCH_SIZE]
            
            # Prepare batch insert
            insert_sql = """
            INSERT INTO DimAccount (AccountID, AccountName, Sector, RevenueRange, ParentAccountID, ValidFrom, ValidTo, IsCurrent)
            VALUES %s
            ON CONFLICT (AccountID) DO UPDATE SET 
                AccountName = EXCLUDED.AccountName,
                Sector = EXCLUDED.Sector,
                RevenueRange = EXCLUDED.RevenueRange,
                ParentAccountID = EXCLUDED.ParentAccountID
            """
            
            # Convert to string format for executemany
            values = []
            for account in batch:
                parent_id = "NULL" if account[4] is None else str(account[4])
                account_name = account[1].replace("'", "''")
                sector = account[2].replace("'", "''")
                revenue_range = account[3].replace("'", "''")
                values.append(f"({account[0]}, '{account_name}', '{sector}', '{revenue_range}', {parent_id}, CURRENT_DATE, NULL, TRUE)")
            
            batch_sql = insert_sql % ",".join(values)
            run_query("b2b_sales_olap", batch_sql)
        
        # Update metadata
        max_id = accounts[-1][0]
        run_query("b2b_sales_olap", 
                 f"UPDATE LoadingMetadata SET last_loaded_id = {max_id}, status = 'completed', last_loaded_timestamp = CURRENT_TIMESTAMP WHERE table_name = 'DimAccount'")
        
    except Exception as e:
        # Log error and update metadata
        run_query("b2b_sales_olap", 
                 "UPDATE LoadingMetadata SET status = 'failed' WHERE table_name = 'DimAccount'")
        logger.error(f"Error loading accounts: {e}")
        raise

def load_products_scd2(**kwargs):
    """
    Load product dimension from OLTP to OLAP with SCD Type 2 handling.
    """
    # Update metadata to 'in progress'
    run_query("b2b_sales_olap", 
              "UPDATE LoadingMetadata SET status = 'in_progress' WHERE table_name = 'DimProduct'")
    
    try:
        # Get last loaded ID
        last_id_result = fetch_data("b2b_sales_olap", 
                                   "SELECT last_loaded_id FROM LoadingMetadata WHERE table_name = 'DimProduct'")
        last_id = last_id_result[0][0] if last_id_result else 0
        
        # Get products from OLTP
        products = fetch_data("b2b_sales", 
                             f"""
                             SELECT product_id, product_name, series_name, price, 
                                    CASE 
                                        WHEN price < 1000 THEN 'Low'
                                        WHEN price < 5000 THEN 'Medium'
                                        ELSE 'High'
                                    END as price_range
                             FROM products 
                             WHERE product_id > {last_id} 
                             ORDER BY product_id
                             """)
        
        if not products:
            logger.info("No new products to load")
            run_query("b2b_sales_olap", 
                     "UPDATE LoadingMetadata SET status = 'completed', last_loaded_timestamp = CURRENT_TIMESTAMP WHERE table_name = 'DimProduct'")
            return
        
        # Get the next available ProductID
        next_id_result = fetch_data("b2b_sales_olap", 
                                   "SELECT COALESCE(MAX(ProductID), 0) + 1 FROM DimProduct")
        next_id = next_id_result[0][0] if next_id_result else 1
        
        # Process in batches
        for i in range(0, len(products), BATCH_SIZE):
            batch = products[i:i+BATCH_SIZE]
            
            # Prepare batch insert
            insert_sql = """
            INSERT INTO DimProduct (ProductID, SourceProductID, ProductName, SeriesName, Price, PriceRange, ValidFrom, ValidTo, IsCurrent)
            VALUES %s
            """
            
            # Convert to string format for executemany
            values = []
            current_id = next_id
            for product in batch:
                product_name = product[1].replace("'", "''")
                series_name = product[2].replace("'", "''")
                price_range = product[4].replace("'", "''")
                values.append(f"({current_id}, {product[0]}, '{product_name}', '{series_name}', {product[3]}, '{price_range}', CURRENT_DATE, NULL, TRUE)")
                current_id += 1
            
            batch_sql = insert_sql % ",".join(values)
            run_query("b2b_sales_olap", batch_sql)
            
            next_id = current_id
        
        # Update metadata
        max_id = products[-1][0]
        run_query("b2b_sales_olap", 
                 f"UPDATE LoadingMetadata SET last_loaded_id = {max_id}, status = 'completed', last_loaded_timestamp = CURRENT_TIMESTAMP WHERE table_name = 'DimProduct'")
        
    except Exception as e:
        # Log error and update metadata
        run_query("b2b_sales_olap", 
                 "UPDATE LoadingMetadata SET status = 'failed' WHERE table_name = 'DimProduct'")
        logger.error(f"Error loading products: {e}")
        raise

def load_sales_agents(**kwargs):
    """
    Load sales agent dimension from OLTP to OLAP.
    """
    # Update metadata to 'in progress'
    run_query("b2b_sales_olap", 
              "UPDATE LoadingMetadata SET status = 'in_progress' WHERE table_name = 'DimSalesAgent'")
    
    try:
        # Get last loaded ID
        last_id_result = fetch_data("b2b_sales_olap", 
                                   "SELECT last_loaded_id FROM LoadingMetadata WHERE table_name = 'DimSalesAgent'")
        last_id = last_id_result[0][0] if last_id_result else 0
        
        # Get sales agents from OLTP
        agents = fetch_data("b2b_sales", 
                           f"""
                           SELECT agent_id, agent_name, manager_name, region 
                           FROM sales_agents 
                           WHERE agent_id > {last_id} 
                           ORDER BY agent_id
                           """)
        
        if not agents:
            logger.info("No new sales agents to load")
            run_query("b2b_sales_olap", 
                     "UPDATE LoadingMetadata SET status = 'completed', last_loaded_timestamp = CURRENT_TIMESTAMP WHERE table_name = 'DimSalesAgent'")
            return
        
        # Process in batches
        for i in range(0, len(agents), BATCH_SIZE):
            batch = agents[i:i+BATCH_SIZE]
            
            # Prepare batch insert
            insert_sql = """
            INSERT INTO DimSalesAgent (SalesAgentID, SalesAgentName, ManagerName, Region, ValidFrom, ValidTo, IsCurrent)
            VALUES %s
            ON CONFLICT (SalesAgentID) DO UPDATE SET 
                SalesAgentName = EXCLUDED.SalesAgentName,
                ManagerName = EXCLUDED.ManagerName,
                Region = EXCLUDED.Region
            """
            
            # Convert to string format for executemany
            values = []
            for agent in batch:
                if agent[2] is None:
                    manager = "NULL"
                else:
                    manager_name = agent[2].replace("'", "''")
                    manager = f"'{manager_name}'"
                agent_name = agent[1].replace("'", "''")
                region = agent[3].replace("'", "''")
                values.append(f"({agent[0]}, '{agent_name}', {manager}, '{region}', CURRENT_DATE, NULL, TRUE)")
            
            batch_sql = insert_sql % ",".join(values)
            run_query("b2b_sales_olap", batch_sql)
        
        # Update metadata
        max_id = agents[-1][0]
        run_query("b2b_sales_olap", 
                 f"UPDATE LoadingMetadata SET last_loaded_id = {max_id}, status = 'completed', last_loaded_timestamp = CURRENT_TIMESTAMP WHERE table_name = 'DimSalesAgent'")
        
    except Exception as e:
        # Log error and update metadata
        run_query("b2b_sales_olap", 
                 "UPDATE LoadingMetadata SET status = 'failed' WHERE table_name = 'DimSalesAgent'")
        logger.error(f"Error loading sales agents: {e}")
        raise

def load_fact_sales_performance(**kwargs):
    """
    Load fact sales performance data from OLTP to OLAP in batches.
    """
    # Update metadata to 'in progress'
    run_query("b2b_sales_olap", 
              "UPDATE LoadingMetadata SET status = 'in_progress' WHERE table_name = 'FactSalesPerformance'")
    
    try:
        # Get last loaded ID
        last_id_result = fetch_data("b2b_sales_olap", 
                                   "SELECT last_loaded_id FROM LoadingMetadata WHERE table_name = 'FactSalesPerformance'")
        last_id = last_id_result[0][0] if last_id_result else 0
        
        # Get total count for progress tracking
        total_count_result = fetch_data("b2b_sales", 
                                       f"SELECT COUNT(*) FROM sales_pipeline WHERE id > {last_id}")
        total_count = total_count_result[0][0] if total_count_result else 0
        
        if total_count == 0:
            logger.info("No new sales performance data to load")
            run_query("b2b_sales_olap", 
                     "UPDATE LoadingMetadata SET status = 'completed', last_loaded_timestamp = CURRENT_TIMESTAMP WHERE table_name = 'FactSalesPerformance'")
            return
        
        # Process in batches
        processed_count = 0
        max_id = last_id
        
        while processed_count < total_count:
            # Get batch of sales data
            sales_data = fetch_data("b2b_sales", 
                                   f"""
                                   SELECT id, deal_date, account_id, product_id, agent_id, stage_id, 
                                          close_value, duration_days, success_rate
                                   FROM sales_pipeline 
                                   WHERE id > {max_id} 
                                   ORDER BY id
                                   LIMIT {BATCH_SIZE}
                                   """)
            
            if not sales_data:
                break
                
            # Prepare data for insertion
            insert_values = []
            error_records = []
            
            for record in sales_data:
                try:
                    # Convert date to DateKey format (YYYYMMDD)
                    date_key = record[1].strftime('%Y%m%d')
                    
                    # Get product key from DimProduct (most current version)
                    product_key_result = fetch_data("b2b_sales_olap", 
                                                  f"""
                                                  SELECT ProductID FROM DimProduct 
                                                  WHERE SourceProductID = {record[3]} AND IsCurrent = TRUE
                                                  """)
                    
                    if not product_key_result:
                        raise ValueError(f"Product with SourceProductID {record[3]} not found in DimProduct")
                    
                    product_key = product_key_result[0][0]
                    
                    # Format values for insertion
                    insert_values.append(
                        f"({date_key}, {record[2]}, {product_key}, {record[4]}, {record[5]}, "
                        f"{record[6]}, {record[7]}, {record[8]}, {record[0]})"
                    )
                    
                    # Update max_id
                    max_id = max(max_id, record[0])
                    
                except Exception as e:
                    # Log error record
                    error_message = str(e).replace("'", "''")
                    error_records.append(f"('FactSalesPerformance', '{record[0]}', '{error_message}')")
            
            # Insert valid records
            if insert_values:
                insert_sql = f"""
                INSERT INTO FactSalesPerformance 
                    (DateKey, AccountKey, ProductKey, SalesAgentKey, DealStageKey, 
                     CloseValue, DurationDays, ExpectedSuccessRate, SourceTransactionID)
                VALUES {','.join(insert_values)}
                """
                run_query("b2b_sales_olap", insert_sql)
            
            # Log error records
            if error_records:
                error_sql = f"""
                INSERT INTO ErrorLog (table_name, record_id, error_message)
                VALUES {','.join(error_records)}
                """
                run_query("b2b_sales_olap", error_sql)
            
            # Update processed count
            processed_count += len(sales_data)
            
            # Update metadata with current progress
            run_query("b2b_sales_olap", 
                     f"UPDATE LoadingMetadata SET last_loaded_id = {max_id}, last_loaded_timestamp = CURRENT_TIMESTAMP WHERE table_name = 'FactSalesPerformance'")
        
        # Update metadata to 'completed'
        run_query("b2b_sales_olap", 
                 "UPDATE LoadingMetadata SET status = 'completed', last_loaded_timestamp = CURRENT_TIMESTAMP WHERE table_name = 'FactSalesPerformance'")
        
    except Exception as e:
        # Log error and update metadata
        run_query("b2b_sales_olap", 
                 "UPDATE LoadingMetadata SET status = 'failed' WHERE table_name = 'FactSalesPerformance'")
        logger.error(f"Error loading fact sales performance: {e}")
        raise

