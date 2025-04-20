from datetime import datetime, timedelta
import logging
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import json

# Configure logging
logger = logging.getLogger(__name__)

# Batch size for processing large datasets
BATCH_SIZE = 5000  # Adjust based on your system's capacity

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

def setup_change_tracking(**kwargs):
    """
    Set up change tracking tables and triggers in OLTP database.
    """
    try:
        # Create change tracking table in OLTP
        change_tracking_sql = """
        -- Create change tracking table if it doesn't exist
        CREATE TABLE IF NOT EXISTS change_tracking (
            change_id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            record_id INT NOT NULL,
            operation VARCHAR(10) NOT NULL,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create index on table_name and record_id
        CREATE INDEX IF NOT EXISTS idx_change_tracking_table_record 
        ON change_tracking(table_name, record_id);
        
        -- Create index on changed_at
        CREATE INDEX IF NOT EXISTS idx_change_tracking_changed_at 
        ON change_tracking(changed_at);
        """
        run_query("b2b_sales", change_tracking_sql)
        
        # Create triggers for each table to track changes
        tables = ['accounts', 'products', 'sales_agents', 'sales_pipeline']
        
        for table in tables:
            trigger_function_sql = f"""
            -- Create or replace trigger function for {table}
            CREATE OR REPLACE FUNCTION track_{table}_changes()
            RETURNS TRIGGER AS $$
            BEGIN
                IF (TG_OP = 'INSERT') THEN
                    INSERT INTO change_tracking (table_name, record_id, operation)
                    VALUES ('{table}', NEW.id, 'INSERT');
                    RETURN NEW;
                ELSIF (TG_OP = 'UPDATE') THEN
                    INSERT INTO change_tracking (table_name, record_id, operation)
                    VALUES ('{table}', NEW.id, 'UPDATE');
                    RETURN NEW;
                ELSIF (TG_OP = 'DELETE') THEN
                    INSERT INTO change_tracking (table_name, record_id, operation)
                    VALUES ('{table}', OLD.id, 'DELETE');
                    RETURN OLD;
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
            
            -- Drop trigger if it exists
            DROP TRIGGER IF EXISTS track_{table}_changes ON {table};
            
            -- Create trigger
            CREATE TRIGGER track_{table}_changes
            AFTER INSERT OR UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION track_{table}_changes();
            """
            
            run_query("b2b_sales", trigger_function_sql)
        
        # Create a table to track last processed change
        tracking_metadata_sql = """
        CREATE TABLE IF NOT EXISTS change_tracking_metadata (
            table_name VARCHAR(100) PRIMARY KEY,
            last_processed_id INT DEFAULT 0,
            last_processed_at TIMESTAMP DEFAULT '2000-01-01'
        );
        
        -- Initialize metadata for each table if not exists
        INSERT INTO change_tracking_metadata (table_name, last_processed_id, last_processed_at)
        VALUES 
            ('accounts', 0, '2000-01-01'),
            ('products', 0, '2000-01-01'),
            ('sales_agents', 0, '2000-01-01'),
            ('sales_pipeline', 0, '2000-01-01')
        ON CONFLICT (table_name) DO NOTHING;
        """
        run_query("b2b_sales", tracking_metadata_sql)
        
        logger.info("Change tracking setup completed successfully")
        
    except Exception as e:
        logger.error(f"Error setting up change tracking: {e}")
        raise

def detect_changed_data(**kwargs):
    """
    Detect changed data in OLTP and prepare for incremental update.
    """
    try:
        # Create staging table for changed data in OLAP
        staging_sql = """
        CREATE TABLE IF NOT EXISTS staging_changed_data (
            change_id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            record_id INT NOT NULL,
            operation VARCHAR(10) NOT NULL,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed BOOLEAN DEFAULT FALSE
        );
        
        -- Truncate staging table to prepare for new changes
        TRUNCATE TABLE staging_changed_data;
        """
        run_query("b2b_sales_olap", staging_sql)
        
        # Get last processed change IDs for each table
        tables = ['accounts', 'products', 'sales_agents', 'sales_pipeline']
        changes_detected = False
        
        for table in tables:
            # Get last processed change ID from OLTP
            last_id_result = fetch_data("b2b_sales", 
                                      f"SELECT last_processed_id FROM change_tracking_metadata WHERE table_name = '{table}'")
            last_id = last_id_result[0][0] if last_id_result else 0
            
            # Get new changes from OLTP
            changes = fetch_data("b2b_sales", 
                               f"""
                               SELECT change_id, record_id, operation, changed_at 
                               FROM change_tracking 
                               WHERE table_name = '{table}' AND change_id > {last_id}
                               ORDER BY change_id
                               """)
            
            if changes:
                changes_detected = True
                logger.info(f"Detected {len(changes)} changes for table {table}")
                
                # Insert changes into staging table in OLAP
                for i in range(0, len(changes), BATCH_SIZE):
                    batch = changes[i:i+BATCH_SIZE]
                    
                    # Prepare batch insert
                    insert_sql = """
                    INSERT INTO staging_changed_data (table_name, record_id, operation, changed_at)
                    VALUES %s
                    """
                    
                    # Convert to string format for executemany
                    values = []
                    for change in batch:
                        values.append(f"('{table}', {change[1]}, '{change[2]}', '{change[3]}')")
                    
                    batch_sql = insert_sql % ",".join(values)
                    run_query("b2b_sales_olap", batch_sql)
                
                # Update last processed ID in OLTP
                max_id = changes[-1][0]
                run_query("b2b_sales", 
                         f"UPDATE change_tracking_metadata SET last_processed_id = {max_id}, last_processed_at = CURRENT_TIMESTAMP WHERE table_name = '{table}'")
        
        # Push changes_detected flag to XCom for downstream tasks
        kwargs['ti'].xcom_push(key='changes_detected', value=changes_detected)
        
        if not changes_detected:
            logger.info("No changes detected in any table")
        
    except Exception as e:
        logger.error(f"Error detecting changed data: {e}")
        raise

def update_accounts_dimension(**kwargs):
    """
    Update account dimension based on detected changes.
    """
    # Check if changes were detected
    ti = kwargs['ti']
    changes_detected = ti.xcom_pull(task_ids='detect_changed_data', key='changes_detected')
    
    if not changes_detected:
        logger.info("No changes to process for accounts dimension")
        return
    
    try:
        # Get changed account IDs
        changed_accounts = fetch_data("b2b_sales_olap", 
                                    """
                                    SELECT DISTINCT record_id 
                                    FROM staging_changed_data 
                                    WHERE table_name = 'accounts' AND processed = FALSE
                                    """)
        
        if not changed_accounts:
            logger.info("No account changes to process")
            return
        
        account_ids = [str(account[0]) for account in changed_accounts]
        account_ids_str = ",".join(account_ids)
        
        # Get updated account data from OLTP
        account_data = fetch_data("b2b_sales", 
                                 f"""
                                 SELECT account_id, account_name, sector, revenue_range, parent_account_id 
                                 FROM accounts 
                                 WHERE account_id IN ({account_ids_str})
                                 """)
        
        # Process each account
        for account in account_data:
            # Check if account exists in OLAP
            exists_result = fetch_data("b2b_sales_olap", 
                                      f"SELECT COUNT(*) FROM DimAccount WHERE AccountID = {account[0]}")
            exists = exists_result[0][0] > 0 if exists_result else False
            
            parent_id = "NULL" if account[4] is None else account[4]
            
            if exists:
                # Update existing account (SCD Type 1 - overwrite)
                update_sql = f"""
                UPDATE DimAccount
                SET AccountName = '{account[1].replace("'", "''")}',
                    Sector = '{account[2].replace("'", "''")}',
                    RevenueRange = '{account[3].replace("'", "''")}',
                    ParentAccountID = {parent_id}
                WHERE AccountID = {account[0]}
                """
                run_query("b2b_sales_olap", update_sql)
            else:
                # Insert new account
                insert_sql = f"""
                INSERT INTO DimAccount (AccountID, AccountName, Sector, RevenueRange, ParentAccountID, ValidFrom, ValidTo, IsCurrent)
                VALUES ({account[0]}, '{account[1].replace("'", "''")}', '{account[2].replace("'", "''")}', 
                        '{account[3].replace("'", "''")}', {parent_id}, CURRENT_DATE, NULL, TRUE)
                """
                run_query("b2b_sales_olap", insert_sql)
        
        # Mark changes as processed
        run_query("b2b_sales_olap", 
                 f"UPDATE staging_changed_data SET processed = TRUE WHERE table_name = 'accounts' AND record_id IN ({account_ids_str})")
        
        logger.info(f"Processed {len(account_ids)} account changes")
        
    except Exception as e:
        logger.error(f"Error updating accounts dimension: {e}")
        raise

def update_products_dimension_scd2(**kwargs):
    """
    Update product dimension based on detected changes using SCD Type 2.
    """
    # Check if changes were detected
    ti = kwargs['ti']
    changes_detected = ti.xcom_pull(task_ids='detect_changed_data', key='changes_detected')
    
    if not changes_detected:
        logger.info("No changes to process for products dimension")
        return
    
    try:
        # Get changed product IDs
        changed_products = fetch_data("b2b_sales_olap", 
                                    """
                                    SELECT DISTINCT record_id 
                                    FROM staging_changed_data 
                                    WHERE table_name = 'products' AND processed = FALSE
                                    """)
        
        if not changed_products:
            logger.info("No product changes to process")
            return
        
        product_ids = [str(product[0]) for product in changed_products]
        product_ids_str = ",".join(product_ids)
        
        # Get updated product data from OLTP
        product_data = fetch_data("b2b_sales", 
                                 f"""
                                 SELECT product_id, product_name, series_name, price, 
                                        CASE 
                                            WHEN price < 1000 THEN 'Low'
                                            WHEN price < 5000 THEN 'Medium'
                                            ELSE 'High'
                                        END as price_range
                                 FROM products 
                                 WHERE product_id IN ({product_ids_str})
                                 """)
        
        # Process each product using SCD Type 2
        for product in product_data:
            # Check if product exists and get current version
            current_version = fetch_data("b2b_sales_olap", 
                                       f"""
                                       SELECT ProductID, ProductName, SeriesName, Price, PriceRange 
                                       FROM DimProduct 
                                       WHERE SourceProductID = {product[0]} AND IsCurrent = TRUE
                                       """)
            
            # Get the next available ProductID
            next_id_result = fetch_data("b2b_sales_olap", 
                                      "SELECT COALESCE(MAX(ProductID), 0) + 1 FROM DimProduct")
            next_id = next_id_result[0][0] if next_id_result else 1
            
            if current_version:
                # Check if any attributes have changed
                current = current_version[0]
                if (current[1] != product[1] or 
                    current[2] != product[2] or 
                    float(current[3]) != float(product[3]) or 
                    current[4] != product[4]):
                    
                    # Close current version
                    update_sql = f"""
                    UPDATE DimProduct
                    SET ValidTo = CURRENT_DATE - INTERVAL '1 day',
                        IsCurrent = FALSE
                    WHERE SourceProductID = {product[0]} AND IsCurrent = TRUE
                    """
                    run_query("b2b_sales_olap", update_sql)
                    
                    # Insert new version
                    insert_sql = f"""
                    INSERT INTO DimProduct 
                        (ProductID, SourceProductID, ProductName, SeriesName, Price, PriceRange, ValidFrom, ValidTo, IsCurrent)
                    VALUES 
                        ({next_id}, {product[0]}, '{product[1].replace("'", "''")}', '{product[2].replace("'", "''")}', 
                         {product[3]}, '{product[4]}', CURRENT_DATE, NULL, TRUE)
                    """
                    run_query("b2b_sales_olap", insert_sql)
            else:
                # Insert new product
                insert_sql = f"""
                INSERT INTO DimProduct 
                    (ProductID, SourceProductID, ProductName, SeriesName, Price, PriceRange, ValidFrom, ValidTo, IsCurrent)
                VALUES 
                    ({next_id}, {product[0]}, '{product[1].replace("'", "''")}', '{product[2].replace("'", "''")}', 
                     {product[3]}, '{product[4]}', CURRENT_DATE, NULL, TRUE)
                """
                run_query("b2b_sales_olap", insert_sql)
        
        # Mark changes as processed
        run_query("b2b_sales_olap", 
                 f"UPDATE staging_changed_data SET processed = TRUE WHERE table_name = 'products' AND record_id IN ({product_ids_str})")
        
        logger.info(f"Processed {len(product_ids)} product changes using SCD Type 2")
        
    except Exception as e:
        logger.error(f"Error updating products dimension: {e}")
        raise

def update_sales_agents_dimension(**kwargs):
    """
    Update sales agent dimension based on detected changes.
    """
    # Check if changes were detected
    ti = kwargs['ti']
    changes_detected = ti.xcom_pull(task_ids='detect_changed_data', key='changes_detected')
    
    if not changes_detected:
        logger.info("No changes to process for sales agents dimension")
        return
    
    try:
        # Get changed sales agent IDs
        changed_agents = fetch_data("b2b_sales_olap", 
                                  """
                                  SELECT DISTINCT record_id 
                                  FROM staging_changed_data 
                                  WHERE table_name = 'sales_agents' AND processed = FALSE
                                  """)
        
        if not changed_agents:
            logger.info("No sales agent changes to process")
            return
        
        agent_ids = [str(agent[0]) for agent in changed_agents]
        agent_ids_str = ",".join(agent_ids)
        
        # Get updated sales agent data from OLTP
        agent_data = fetch_data("b2b_sales", 
                               f"""
                               SELECT agent_id, agent_name, manager_name, region 
                               FROM sales_agents 
                               WHERE agent_id IN ({agent_ids_str})
                               """)
        
        # Process each sales agent
        for agent in agent_data:
            # Check if agent exists in OLAP
            exists_result = fetch_data("b2b_sales_olap", 
                                      f"SELECT COUNT(*) FROM DimSalesAgent WHERE SalesAgentID = {agent[0]}")
            exists = exists_result[0][0] > 0 if exists_result else False
            
            if agent[2] is None:
                manager = "NULL"
            else:
                manager = "'" + agent[2].replace("'", "''") + "'"
            
            if exists:
                # Update existing agent
                update_sql = f"""
                UPDATE DimSalesAgent
                SET SalesAgentName = '{agent[1].replace("'", "''")}',
                    ManagerName = {manager},
                    Region = '{agent[3].replace("'", "''")}'
                WHERE SalesAgentID = {agent[0]}
                """
                run_query("b2b_sales_olap", update_sql)
            else:
                # Insert new agent
                insert_sql = f"""
                INSERT INTO DimSalesAgent 
                    (SalesAgentID, SalesAgentName, ManagerName, Region, ValidFrom, ValidTo, IsCurrent)
                VALUES 
                    ({agent[0]}, '{agent[1].replace("'", "''")}', {manager}, 
                     '{agent[3].replace("'", "''")}', CURRENT_DATE, NULL, TRUE)
                """
                run_query("b2b_sales_olap", insert_sql)
        
        # Mark changes as processed
        run_query("b2b_sales_olap", 
                 f"UPDATE staging_changed_data SET processed = TRUE WHERE table_name = 'sales_agents' AND record_id IN ({agent_ids_str})")
        
        logger.info(f"Processed {len(agent_ids)} sales agent changes")    
    except Exception as e:
        logger.error(f"Error updating sales agents dimension: {e}")
        raise


def update_fact_sales_performance(**kwargs):
    """
    Update fact sales performance based on detected changes.
    """
    # Check if changes were detected
    ti = kwargs['ti']
    changes_detected = ti.xcom_pull(task_ids='detect_changed_data', key='changes_detected')
    
    if not changes_detected:
        logger.info("No changes to process for fact sales performance")
        return
    
    try:
        # Get changed sales pipeline IDs
        changed_sales = fetch_data("b2b_sales_olap", 
                                 """
                                 SELECT DISTINCT record_id, operation 
                                 FROM staging_changed_data 
                                 WHERE table_name = 'sales_pipeline' AND processed = FALSE
                                 """)
        
        if not changed_sales:
            logger.info("No sales performance changes to process")
            return
        
        # Process deletions first
        deletions = [str(record[0]) for record in changed_sales if record[1] == 'DELETE']
        if deletions:
            deletions_str = ",".join(deletions)
            delete_sql = f"""
            DELETE FROM FactSalesPerformance
            WHERE SourceTransactionID IN ({deletions_str})
            """
            run_query("b2b_sales_olap", delete_sql)
            logger.info(f"Deleted {len(deletions)} records from FactSalesPerformance")
        
        # Process inserts and updates
        upserts = [str(record[0]) for record in changed_sales if record[1] in ('INSERT', 'UPDATE')]
        if upserts:
            upserts_str = ",".join(upserts)
            
            # Get updated data from OLTP
            sales_data = fetch_data("b2b_sales", 
                                   f"""
                                   SELECT id, deal_date, account_id, product_id, agent_id, stage_id, 
                                          close_value, duration_days, success_rate
                                   FROM sales_pipeline 
                                   WHERE id IN ({upserts_str})
                                   """)
            
            # Process each record
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
                    
                    # Delete existing record if it's an update
                    delete_existing_sql = f"""
                    DELETE FROM FactSalesPerformance
                    WHERE SourceTransactionID = {record[0]}
                    """
                    run_query("b2b_sales_olap", delete_existing_sql)
                    
                    # Insert new/updated record
                    insert_sql = f"""
                    INSERT INTO FactSalesPerformance 
                        (DateKey, AccountKey, ProductKey, SalesAgentKey, DealStageKey, 
                         CloseValue, DurationDays, ExpectedSuccessRate, SourceTransactionID)
                    VALUES 
                        ({date_key}, {record[2]}, {product_key}, {record[4]}, {record[5]}, 
                         {record[6]}, {record[7]}, {record[8]}, {record[0]})
                    """
                    run_query("b2b_sales_olap", insert_sql)
                    
                except Exception as e:
                    # Log error record
                    error_message = str(e).replace("'", "''")
                    error_sql = f"""
                    INSERT INTO ErrorLog (table_name, record_id, error_message)
                    VALUES ('FactSalesPerformance', '{record[0]}', '{error_message}')
                    """
                    run_query("b2b_sales_olap", error_sql)
                    logger.error(f"Error processing sales record {record[0]}: {e}")
            
            logger.info(f"Processed {len(upserts)} inserts/updates for FactSalesPerformance")
        
        # Mark all changes as processed
        all_ids = [str(record[0]) for record in changed_sales]
        all_ids_str = ",".join(all_ids)
        run_query("b2b_sales_olap", 
                 f"UPDATE staging_changed_data SET processed = TRUE WHERE table_name = 'sales_pipeline' AND record_id IN ({all_ids_str})")
        
    except Exception as e:
        logger.error(f"Error updating fact sales performance: {e}")
        raise

def update_monthly_aggregates(**kwargs):
    """
    Update monthly sales aggregates after fact table changes.
    """
    # Check if changes were detected
    ti = kwargs['ti']
    changes_detected = ti.xcom_pull(task_ids='detect_changed_data', key='changes_detected')
    
    if not changes_detected:
        logger.info("No changes to process for monthly aggregates")
        return
    
    try:
        # Refresh the entire aggregate table
        # This is simpler than trying to update only affected months/regions
        run_query("b2b_sales_olap", "TRUNCATE TABLE FactSalesMonthlyAggregate")
        
        # Recompute aggregates
        aggregate_sql = """
        INSERT INTO FactSalesMonthlyAggregate
            (Year, Month, Region, DealStageKey, TotalDealCount, TotalCloseValue, 
             AvgDealValue, AvgDurationDays, ExpectedSuccessRate, LastUpdated)
        SELECT 
            d.Year, 
            d.Month, 
            sa.Region, 
            f.DealStageKey,
            COUNT(*) AS TotalDealCount,
            SUM(f.CloseValue) AS TotalCloseValue,
            AVG(f.CloseValue) AS AvgDealValue,
            AVG(f.DurationDays) AS AvgDurationDays,
            AVG(f.ExpectedSuccessRate) AS ExpectedSuccessRate,
            CURRENT_TIMESTAMP AS LastUpdated
        FROM FactSalesPerformance f
        JOIN DimDate d ON f.DateKey = d.DateKey
        JOIN DimSalesAgent sa ON f.SalesAgentKey = sa.SalesAgentID
        GROUP BY d.Year, d.Month, sa.Region, f.DealStageKey
        """
        
        run_query("b2b_sales_olap", aggregate_sql)
        logger.info("Monthly aggregates updated successfully")
        
    except Exception as e:
        logger.error(f"Error updating monthly aggregates: {e}")
        raise

def validate_incremental_update(**kwargs):
    """
    Validate the incremental update by checking data integrity.
    """
    # Check if changes were detected
    ti = kwargs['ti']
    changes_detected = ti.xcom_pull(task_ids='detect_changed_data', key='changes_detected')
    
    if not changes_detected:
        logger.info("No changes to validate")
        return
    
    validation_results = {}
    validation_passed = True
    
    try:
        # Check for unprocessed changes
        unprocessed_result = fetch_data("b2b_sales_olap", 
                                      "SELECT COUNT(*) FROM staging_changed_data WHERE processed = FALSE")
        unprocessed_count = unprocessed_result[0][0] if unprocessed_result else 0
        
        validation_results['UnprocessedChanges'] = {
            'count': unprocessed_count,
            'status': 'PASS' if unprocessed_count == 0 else 'FAIL'
        }
        
        if unprocessed_count > 0:
            validation_passed = False
            logger.error(f"Validation failed: {unprocessed_count} changes were not processed")
        
        # Check for orphaned records in fact table
        orphan_check_sql = """
        SELECT COUNT(*) FROM FactSalesPerformance f
        LEFT JOIN DimDate d ON f.DateKey = d.DateKey
        LEFT JOIN DimAccount a ON f.AccountKey = a.AccountID
        LEFT JOIN DimProduct p ON f.ProductKey = p.ProductID
        LEFT JOIN DimSalesAgent sa ON f.SalesAgentKey = sa.SalesAgentID
        LEFT JOIN DimDealStage ds ON f.DealStageKey = ds.StageID
        WHERE d.DateKey IS NULL OR a.AccountID IS NULL OR p.ProductID IS NULL 
              OR sa.SalesAgentID IS NULL OR ds.StageID IS NULL
        """
        
        orphan_result = fetch_data("b2b_sales_olap", orphan_check_sql)
        orphan_count = orphan_result[0][0] if orphan_result else 0
        
        validation_results['OrphanedRecords'] = {
            'count': orphan_count,
            'status': 'PASS' if orphan_count == 0 else 'FAIL'
        }
        
        if orphan_count > 0:
            validation_passed = False
            logger.error(f"Validation failed: Found {orphan_count} orphaned records in FactSalesPerformance")
        
        # Check for errors during update
        error_count_result = fetch_data("b2b_sales_olap", 
                                      "SELECT COUNT(*) FROM ErrorLog WHERE created_at > (CURRENT_TIMESTAMP - INTERVAL '1 hour')")
        error_count = error_count_result[0][0] if error_count_result else 0
        
        validation_results['RecentErrors'] = {
            'count': error_count,
            'status': 'WARNING' if error_count > 0 else 'PASS'
        }
        
        if error_count > 0:
            logger.warning(f"Validation warning: Found {error_count} recent errors in ErrorLog")
        
        # Store validation results as XCom for downstream tasks
        kwargs['ti'].xcom_push(key='validation_results', value=validation_results)
        kwargs['ti'].xcom_push(key='validation_passed', value=validation_passed)
        
        if not validation_passed:
            raise ValueError("Incremental update validation failed. Check logs for details.")
        
        logger.info("Incremental update validation passed successfully")
        
    except Exception as e:
        logger.error(f"Error during validation: {e}")
        raise

# Define default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
with DAG(
    dag_id='dwh_incremental_update',
    default_args=default_args,
    description='Incremental update of data from OLTP to OLAP data warehouse',
    schedule_interval='0 */3 * * *',  # Run every 3 hours
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['data_warehouse', 'incremental_update'],
) as dag:
    
    # Task to set up change tracking
    t_setup_change_tracking = PythonOperator(
        task_id='setup_change_tracking',
        python_callable=setup_change_tracking,
    )
    
    # Task to detect changed data
    t_detect_changed_data = PythonOperator(
        task_id='detect_changed_data',
        python_callable=detect_changed_data,
    )
    
    # Task to update accounts dimension
    t_update_accounts = PythonOperator(
        task_id='update_accounts',
        python_callable=update_accounts_dimension,
    )
    
    # Task to update products dimension
    t_update_products = PythonOperator(
        task_id='update_products',
        python_callable=update_products_dimension_scd2,
    )
    
    # Task to update sales agents dimension
    t_update_sales_agents = PythonOperator(
        task_id='update_sales_agents',
        python_callable=update_sales_agents_dimension,
    )
    
    # Task to update fact sales performance
    t_update_fact_sales = PythonOperator(
        task_id='update_fact_sales',
        python_callable=update_fact_sales_performance,
    )
    
    # Task to update monthly aggregates
    t_update_aggregates = PythonOperator(
        task_id='update_aggregates',
        python_callable=update_monthly_aggregates,
    )
    
    # Task to validate the incremental update
    t_validate_update = PythonOperator(
        task_id='validate_update',
        python_callable=validate_incremental_update,
    )
    
    # Define task dependencies
    t_setup_change_tracking >> t_detect_changed_data
    t_detect_changed_data >> [t_update_accounts, t_update_products, t_update_sales_agents]
    [t_update_accounts, t_update_products, t_update_sales_agents] >> t_update_fact_sales
    t_update_fact_sales >> t_update_aggregates >> t_validate_update
