# /path/to/your/airflow/dags/b2b_incremental_load_dag.py
import pendulum
import logging
from datetime import timedelta, datetime
# Ensure psycopg2 errors can be caught if needed, requires postgres provider installed
try:
    import psycopg2
except ImportError:
    psycopg2 = None # Allows code to run but specific DB error checks might fail

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowNotFoundException

# --- Configuration ---
OLTP_CONN_ID = "b2b_sales"
OLAP_CONN_ID = "b2b_sales_olap"
DEFAULT_START_TIME = '1970-01-01T00:00:00+00:00' # Default start for first run
WATERMARK_VAR_PREFIX = "b2b_incremental_watermark_" # Prefix for timestamp watermarks

# --- Helper: Watermark Management (Timestamp-based) ---
def get_watermark(table_key: str) -> str:
    """Gets the last watermark timestamp for a table key from Airflow Variable."""
    var_name = f"{WATERMARK_VAR_PREFIX}{table_key}"
    watermark = DEFAULT_START_TIME # Default value
    try:
        watermark = Variable.get(var_name, default_var=DEFAULT_START_TIME)
        logging.info(f"Retrieved watermark for {table_key}: {watermark}")
        pendulum.parse(watermark) # Validate format
        return watermark
    except AirflowNotFoundException:
        logging.info(f"Watermark Variable {var_name} not found. Using default: {DEFAULT_START_TIME}")
        return DEFAULT_START_TIME
    except Exception as e:
        # Log the problematic value if possible
        current_value = "N/A"
        try:
            current_value = Variable.get(var_name)
        except Exception:
            pass # Ignore error getting value if it doesn't exist or is invalid
        logging.warning(f"Error retrieving or parsing watermark {var_name} (current value: '{current_value}'): {e}. Using default: {DEFAULT_START_TIME}")
        return DEFAULT_START_TIME

def set_watermark(table_key: str, value: str):
    """Sets the new watermark timestamp for a table key in Airflow Variable."""
    var_name = f"{WATERMARK_VAR_PREFIX}{table_key}"
    try:
        pendulum.parse(value) # Validate format before setting
        Variable.set(var_name, value)
        logging.info(f"Set new watermark for {table_key}: {value}")
    except Exception as e:
        logging.error(f"Failed to set watermark {var_name} with value {value}: {e}")
        raise # Re-raise exception to fail the task if watermark is critical

# --- DAG Definition ---
@dag(
    dag_id='b2b_incremental_load',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule='@daily',
    catchup=False,
    # default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)}, # Commented out for testing
    default_args={'retry_delay': timedelta(minutes=5)},
    tags=['b2b_sales', 'incremental_load', 'refined', 'scd2', 'timestamp_watermark'],
    doc_md="""
    ### B2B Sales Incremental Load DAG (Timestamp Watermark & SCD2)

    Loads new and updated data from OLTP to OLAP using `updated_at` timestamps and watermarks.
    Implements **SCD Type 2** for `DimAccount`, `DimProduct`, `DimSalesAgent`.
    Uses **UPSERT** for `FactSalesPerformance` based on `OpportunityID`.

    **Assumptions:**
    - OLTP tables (`Accounts`, `Products`, `SalesAgents`, `SalesPipeline`) have a reliable `updated_at` column.
    - `FactSalesPerformance` in OLAP has an `OpportunityID` column with a `UNIQUE` constraint.

    **CDC/Watermarking:**
    - Reads last processed `updated_at` from `b2b_incremental_watermark_[table_key]` Variable.
    - Queries OLTP for records where `updated_at` > watermark and <= current processing time.
    - Updates watermark Variable upon successful task completion.

    **Tasks:**
    1. Update DimAccount (SCD Type 2).
    2. Update DimProduct (SCD Type 2).
    3. Update DimSalesAgent (SCD Type 2).
    4. Load/Update FactSalesPerformance (Upsert, with historical dimension key lookups).
    """
)
def b2b_incremental_load_dag():

    start = EmptyOperator(task_id='start_incremental_load')
    end = EmptyOperator(task_id='end_incremental_load')

    # --- Task 1: Dimension - Account (Incremental Update - SCD Type 2) ---
    @task
    def update_dim_account(**context):
        table_key = 'accounts' # Key for OLTP table and watermark variable
        start_time_str = get_watermark(table_key)
        end_time = pendulum.now('UTC') # Process up to now
        end_time_iso = end_time.isoformat()
        today_date = end_time.to_date_string()
        previous_day = end_time.subtract(days=1).to_date_string()

        logging.info(f"Checking for Account updates between {start_time_str} and {end_time_iso}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)

        # Fetch changed data from OLTP
        sql_extract = """
        SELECT
            a.AccountID, a.AccountName, s.SectorName,
            CASE
                WHEN a.Revenue < 1000000 THEN 'Under 1M'
                WHEN a.Revenue BETWEEN 1000000 AND 10000000 THEN '1M-10M'
                WHEN a.Revenue > 10000000 THEN 'Over 10M' ELSE 'Unknown'
            END AS RevenueRange,
            a.ParentAccountID
        FROM Accounts a
        LEFT JOIN Sectors s ON a.SectorID = s.SectorID
        WHERE a.updated_at > %s AND a.updated_at <= %s;
        """
        try:
            changed_data = hook_oltp.get_records(sql_extract, parameters=(start_time_str, end_time_iso))
        except Exception as e:
             logging.error(f"Failed to fetch data from OLTP table {table_key}: {e}")
             # Check if the error is UndefinedColumn, provide a specific hint
             if psycopg2 and isinstance(e, psycopg2.errors.UndefinedColumn) and "updated_at" in str(e):
                 logging.error(f"Hint: Ensure the column 'updated_at' exists in the OLTP table '{table_key}'. Run the SQL script from 'add_timestamps_sql'.")
             raise

        if not changed_data:
            logging.info("No Account changes detected.")
            set_watermark(table_key, end_time_iso)
            return

        logging.info(f"Found {len(changed_data)} changed/new accounts to process for SCD Type 2.")
        processed_count = 0

        try:
            with hook_olap.get_conn() as conn:
                with conn.cursor() as cur:
                    for row in changed_data:
                        account_id = row[0]
                        new_name = row[1]
                        new_sector = row[2]
                        new_revenue_range = row[3]
                        new_parent_id = row[4]

                        # Check current active record
                        cur.execute("""
                            SELECT AccountName, Sector, RevenueRange, ParentAccountID
                            FROM DimAccount
                            WHERE AccountID = %s AND IsCurrent = TRUE;
                        """, (account_id,))
                        current_active = cur.fetchone()

                        attributes_changed = False
                        if current_active:
                            old_name, old_sector, old_revenue_range, old_parent_id = current_active
                            # Compare relevant attributes (handle NULLs safely if necessary)
                            if (new_name != old_name or new_sector != old_sector or
                                new_revenue_range != old_revenue_range or new_parent_id != old_parent_id):
                                attributes_changed = True
                        else:
                            attributes_changed = True # No active record, needs insert

                        if attributes_changed:
                            logging.info(f"SCD Type 2: Change detected for AccountID: {account_id}. Updating history.")
                            # 1. Expire old record (if exists)
                            if current_active:
                                cur.execute("""
                                    UPDATE DimAccount
                                    SET ValidTo = %s, IsCurrent = FALSE
                                    WHERE AccountID = %s AND IsCurrent = TRUE;
                                """, (previous_day, account_id))

                            # 2. Insert new record
                            cur.execute("""
                                INSERT INTO DimAccount
                                    (AccountID, AccountName, Sector, RevenueRange, ParentAccountID, ValidFrom, ValidTo, IsCurrent)
                                VALUES (%s, %s, %s, %s, %s, %s, NULL, TRUE);
                            """, (account_id, new_name, new_sector, new_revenue_range, new_parent_id, today_date))
                            processed_count += 1
                        else:
                            logging.info(f"No significant change for existing AccountID: {account_id}")
                conn.commit()
            logging.info(f"SCD Type 2 processing complete for DimAccount. Processed records: {processed_count}")
            set_watermark(table_key, end_time_iso)
        except Exception as e:
            logging.error(f"Error during SCD Type 2 processing for DimAccount: {e}")
            raise

    # --- Task 2: Dimension - Product (Incremental Update - SCD Type 2) ---
    @task
    def update_dim_product(**context):
        table_key = 'products'
        start_time_str = get_watermark(table_key)
        end_time = pendulum.now('UTC')
        end_time_iso = end_time.isoformat()
        today_date = end_time.to_date_string()
        previous_day = end_time.subtract(days=1).to_date_string()

        logging.info(f"Checking for Product updates between {start_time_str} and {end_time_iso}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)

        sql_extract = """
        SELECT
            p.ProductID, p.ProductName, ps.SeriesName, p.SalesPrice,
            CASE
                WHEN p.SalesPrice < 100 THEN 'Low'
                WHEN p.SalesPrice BETWEEN 100 AND 1000 THEN 'Medium'
                WHEN p.SalesPrice > 1000 THEN 'High' ELSE 'Unknown'
            END AS PriceRange
        FROM Products p
        LEFT JOIN ProductSeries ps ON p.SeriesID = ps.SeriesID
        WHERE p.updated_at > %s AND p.updated_at <= %s;
        """
        try:
            changed_data = hook_oltp.get_records(sql_extract, parameters=(start_time_str, end_time_iso))
        except Exception as e:
             logging.error(f"Failed to fetch data from OLTP table {table_key}: {e}")
             if psycopg2 and isinstance(e, psycopg2.errors.UndefinedColumn) and "updated_at" in str(e):
                 logging.error(f"Hint: Ensure the column 'updated_at' exists in the OLTP table '{table_key}'. Run the SQL script from 'add_timestamps_sql'.")
             raise

        if not changed_data:
            logging.info("No Product changes detected.")
            set_watermark(table_key, end_time_iso)
            return

        logging.info(f"Found {len(changed_data)} changed/new products to process for SCD Type 2.")
        processed_count = 0

        try:
            with hook_olap.get_conn() as conn:
                with conn.cursor() as cur:
                    for row in changed_data:
                        product_id = row[0]
                        new_name = row[1]
                        new_series = row[2]
                        new_price = row[3]
                        new_price_range = row[4]

                        cur.execute("""
                            SELECT ProductName, SeriesName, Price, PriceRange
                            FROM DimProduct
                            WHERE ProductID = %s AND IsCurrent = TRUE;
                        """, (product_id,))
                        current_active = cur.fetchone()

                        attributes_changed = False
                        if current_active:
                            old_name, old_series, old_price, old_range = current_active
                            # Use decimal comparison carefully if needed
                            if (new_name != old_name or new_series != old_series or
                                new_price != old_price or new_price_range != old_range):
                                attributes_changed = True
                        else:
                            attributes_changed = True # No active record

                        if attributes_changed:
                            logging.info(f"SCD Type 2: Change detected for ProductID: {product_id}. Updating history.")
                            if current_active:
                                cur.execute("""
                                    UPDATE DimProduct
                                    SET ValidTo = %s, IsCurrent = FALSE
                                    WHERE ProductID = %s AND IsCurrent = TRUE;
                                """, (previous_day, product_id))
                            cur.execute("""
                                INSERT INTO DimProduct
                                    (ProductID, ProductName, SeriesName, Price, PriceRange, ValidFrom, ValidTo, IsCurrent)
                                VALUES (%s, %s, %s, %s, %s, %s, NULL, TRUE);
                            """, (product_id, new_name, new_series, new_price, new_price_range, today_date))
                            processed_count += 1
                        else:
                             logging.info(f"No significant change for existing ProductID: {product_id}")
                conn.commit()
            logging.info(f"SCD Type 2 processing complete for DimProduct. Processed records: {processed_count}")
            set_watermark(table_key, end_time_iso)
        except Exception as e:
            logging.error(f"Error during SCD Type 2 processing for DimProduct: {e}")
            raise

    # --- Task 3: Dimension - SalesAgent (Incremental Update - SCD Type 2) ---
    @task
    def update_dim_sales_agent(**context):
        table_key = 'salesagents'
        start_time_str = get_watermark(table_key)
        end_time = pendulum.now('UTC')
        end_time_iso = end_time.isoformat()
        today_date = end_time.to_date_string()
        previous_day = end_time.subtract(days=1).to_date_string()

        logging.info(f"Checking for SalesAgent updates between {start_time_str} and {end_time_iso}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)

        sql_extract = """
        SELECT
            sa.SalesAgentID, sa.SalesAgentName, sm.ManagerName, l.LocationName AS Region
        FROM SalesAgents sa
        LEFT JOIN SalesManagers sm ON sa.ManagerID = sm.ManagerID
        LEFT JOIN Locations l ON sa.RegionalOfficeID = l.LocationID
        WHERE sa.updated_at > %s AND sa.updated_at <= %s;
        """
        try:
            changed_data = hook_oltp.get_records(sql_extract, parameters=(start_time_str, end_time_iso))
        except Exception as e:
             logging.error(f"Failed to fetch data from OLTP table {table_key}: {e}")
             if psycopg2 and isinstance(e, psycopg2.errors.UndefinedColumn) and "updated_at" in str(e):
                 logging.error(f"Hint: Ensure the column 'updated_at' exists in the OLTP table '{table_key}'. Run the SQL script from 'add_timestamps_sql'.")
             raise

        if not changed_data:
            logging.info("No SalesAgent changes detected.")
            set_watermark(table_key, end_time_iso)
            return

        logging.info(f"Found {len(changed_data)} changed/new sales agents to process for SCD Type 2.")
        processed_count = 0

        try:
            with hook_olap.get_conn() as conn:
                with conn.cursor() as cur:
                    for row in changed_data:
                        agent_id = row[0]
                        new_name = row[1]
                        new_manager = row[2]
                        new_region = row[3]

                        cur.execute("""
                            SELECT SalesAgentName, ManagerName, Region
                            FROM DimSalesAgent
                            WHERE SalesAgentID = %s AND IsCurrent = TRUE;
                        """, (agent_id,))
                        current_active = cur.fetchone()

                        attributes_changed = False
                        if current_active:
                            old_name, old_manager, old_region = current_active
                            if (new_name != old_name or new_manager != old_manager or new_region != old_region):
                                attributes_changed = True
                        else:
                            attributes_changed = True # No active record

                        if attributes_changed:
                            logging.info(f"SCD Type 2: Change detected for SalesAgentID: {agent_id}. Updating history.")
                            if current_active:
                                cur.execute("""
                                    UPDATE DimSalesAgent
                                    SET ValidTo = %s, IsCurrent = FALSE
                                    WHERE SalesAgentID = %s AND IsCurrent = TRUE;
                                """, (previous_day, agent_id))
                            cur.execute("""
                                INSERT INTO DimSalesAgent
                                    (SalesAgentID, SalesAgentName, ManagerName, Region, ValidFrom, ValidTo, IsCurrent)
                                VALUES (%s, %s, %s, %s, %s, NULL, TRUE);
                            """, (agent_id, new_name, new_manager, new_region, today_date))
                            processed_count += 1
                        else:
                            logging.info(f"No significant change for existing SalesAgentID: {agent_id}")
                conn.commit()
            logging.info(f"SCD Type 2 processing complete for DimSalesAgent. Processed records: {processed_count}")
            set_watermark(table_key, end_time_iso)
        except Exception as e:
            logging.error(f"Error during SCD Type 2 processing for DimSalesAgent: {e}")
            raise

    # --- Task 4: Fact Table - SalesPerformance (Incremental Load/Update - UPSERT) ---
    @task
    def update_fact_sales_performance(**context):
        table_key = 'salespipeline'
        start_time_str = get_watermark(table_key)
        end_time = pendulum.now('UTC')
        end_time_iso = end_time.isoformat()

        logging.info(f"Checking for SalesPipeline updates between {start_time_str} and {end_time_iso}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)

        # Fetch changed data from OLTP
        sql_extract = """
        SELECT
            sp.OpportunityID, sp.SalesAgentID, sp.ProductID, sp.AccountID, sp.DealStageID,
            sp.EngageDate, sp.CloseDate, sp.CloseValue,
            CASE WHEN sp.CloseDate IS NOT NULL AND sp.EngageDate IS NOT NULL
                 THEN sp.CloseDate - sp.EngageDate ELSE NULL END AS DurationDays,
            CASE ds.StageName
               WHEN 'Won' THEN 100.0 WHEN 'Lost' THEN 0.0
               WHEN 'Engaging' THEN 75.0 WHEN 'Prospecting' THEN 25.0
               ELSE 10.0 END AS ExpectedSuccessRate,
            -- Event date for dimension lookups
            COALESCE(sp.CloseDate, sp.EngageDate) as EventDate
        FROM SalesPipeline sp
        LEFT JOIN DealStages ds ON sp.DealStageID = ds.DealStageID
        WHERE sp.updated_at > %s AND sp.updated_at <= %s
          AND sp.AccountID IS NOT NULL
          AND sp.ProductID IS NOT NULL
          AND sp.SalesAgentID IS NOT NULL
          AND sp.DealStageID IS NOT NULL
          AND COALESCE(sp.CloseDate, sp.EngageDate) IS NOT NULL;
        """
        try:
            changed_data = hook_oltp.get_records(sql_extract, parameters=(start_time_str, end_time_iso))
        except Exception as e:
             logging.error(f"Failed to fetch data from OLTP table {table_key}: {e}")
             if psycopg2 and isinstance(e, psycopg2.errors.UndefinedColumn) and "updated_at" in str(e):
                 logging.error(f"Hint: Ensure the column 'updated_at' exists in the OLTP table '{table_key}'. Run the SQL script from 'add_timestamps_sql'.")
             raise

        if not changed_data:
            logging.info("No SalesPipeline changes detected.")
            set_watermark(table_key, end_time_iso)
            return

        logging.info(f"Found {len(changed_data)} changed/new pipeline records.")

        # Prepare data for OLAP with dimension key lookups
        olap_data_to_upsert = []
        missing_keys_count = 0
        for row in changed_data:
            try:
                opportunity_id = row[0]
                oltp_sales_agent_id = row[1]
                oltp_product_id = row[2]
                oltp_account_id = row[3]
                oltp_deal_stage_id = row[4]
                event_date = row[10]
                close_value = row[7]
                duration_days = row[8]
                expected_success_rate = row[9]

                # Convert event_date to string if it's a date/datetime object
                event_date_str = event_date.strftime('%Y-%m-%d') if hasattr(event_date, 'strftime') else str(event_date)

                # --- Dimension Key Lookups (SCD2 Aware) ---
                # 1. DateKey
                date_key_sql = "SELECT DateKey FROM DimDate WHERE Date = %s"
                date_key_result = hook_olap.get_first(date_key_sql, parameters=(event_date_str,))
                date_key = date_key_result[0] if date_key_result else None

                # 2. AccountKey
                account_key_sql = """
                    SELECT AccountID FROM DimAccount
                    WHERE AccountID = %s AND %s >= ValidFrom AND (%s < ValidTo OR ValidTo IS NULL)
                """
                account_key_result = hook_olap.get_first(account_key_sql, parameters=(oltp_account_id, event_date_str, event_date_str))
                account_key = account_key_result[0] if account_key_result else None

                # 3. ProductKey
                product_key_sql = """
                    SELECT ProductID FROM DimProduct
                    WHERE ProductID = %s AND %s >= ValidFrom AND (%s < ValidTo OR ValidTo IS NULL)
                """
                product_key_result = hook_olap.get_first(product_key_sql, parameters=(oltp_product_id, event_date_str, event_date_str))
                product_key = product_key_result[0] if product_key_result else None

                # 4. SalesAgentKey
                agent_key_sql = """
                    SELECT SalesAgentID FROM DimSalesAgent
                    WHERE SalesAgentID = %s AND %s >= ValidFrom AND (%s < ValidTo OR ValidTo IS NULL)
                """
                agent_key_result = hook_olap.get_first(agent_key_sql, parameters=(oltp_sales_agent_id, event_date_str, event_date_str))
                agent_key = agent_key_result[0] if agent_key_result else None

                # 5. DealStageKey (Static dimension)
                stage_key_sql = "SELECT StageID FROM DimDealStage WHERE StageID = %s"
                stage_key_result = hook_olap.get_first(stage_key_sql, parameters=(oltp_deal_stage_id,))
                stage_key = stage_key_result[0] if stage_key_result else None

                # Check if all keys were found
                if all([date_key, account_key, product_key, agent_key, stage_key]):
                    olap_data_to_upsert.append((
                        opportunity_id, date_key, account_key, product_key, agent_key,
                        stage_key, close_value, duration_days, expected_success_rate
                    ))
                else:
                    missing_keys_count += 1
                    logging.warning(f"Skipping OpportunityID {opportunity_id} due to missing dimension key for EventDate {event_date_str}. "
                                    f"Found keys - Date: {date_key is not None}, Acc: {account_key is not None}, Prod: {product_key is not None}, "
                                    f"Agent: {agent_key is not None}, Stage: {stage_key is not None}")
            except Exception as lookup_ex:
                 missing_keys_count += 1
                 logging.error(f"Error during dimension lookup for OpportunityID {row[0]} (EventDate: {row[10]}): {lookup_ex}")


        if not olap_data_to_upsert:
             logging.warning("No data prepared for FactSalesPerformance UPSERT after dimension lookups.")
             set_watermark(table_key, end_time_iso)
             return

        logging.info(f"Prepared {len(olap_data_to_upsert)} records for UPSERT into FactSalesPerformance.")
        if missing_keys_count > 0:
             logging.warning(f"Skipped {missing_keys_count} fact records due to missing dimension keys.")

        # Perform UPSERT into OLAP Fact table
        try:
            with hook_olap.get_conn() as conn:
                with conn.cursor() as cur:
                    upsert_sql = """
                    INSERT INTO FactSalesPerformance (
                        OpportunityID, DateKey, AccountKey, ProductKey, SalesAgentKey,
                        DealStageKey, CloseValue, DurationDays, ExpectedSuccessRate
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (OpportunityID) DO UPDATE SET
                        DateKey = EXCLUDED.DateKey,
                        AccountKey = EXCLUDED.AccountKey,
                        ProductKey = EXCLUDED.ProductKey,
                        SalesAgentKey = EXCLUDED.SalesAgentKey,
                        DealStageKey = EXCLUDED.DealStageKey,
                        CloseValue = EXCLUDED.CloseValue,
                        DurationDays = EXCLUDED.DurationDays,
                        ExpectedSuccessRate = EXCLUDED.ExpectedSuccessRate;
                    """
                    cur.executemany(upsert_sql, olap_data_to_upsert)
                conn.commit()
            logging.info(f"Successfully UPSERTED {len(olap_data_to_upsert)} records into FactSalesPerformance.")
            set_watermark(table_key, end_time_iso)
        except Exception as e:
            logging.error(f"Error during UPSERT into FactSalesPerformance: {e}")
            if psycopg2 and isinstance(e, psycopg2.errors.UniqueViolation) and "OpportunityID" in str(e):
                 logging.error("Hint: Ensure OpportunityID column exists in FactSalesPerformance and has a UNIQUE constraint.")
            elif psycopg2 and isinstance(e, psycopg2.errors.ForeignKeyViolation):
                 logging.error("Hint: Check if all looked-up dimension keys actually exist in the dimension tables for the required dates.")
            raise

    # --- Task Dependencies ---
    task_update_account = update_dim_account()
    task_update_product = update_dim_product()
    task_update_agent = update_dim_sales_agent()
    task_update_facts = update_fact_sales_performance()

    # Dimension updates can run in parallel
    start >> [task_update_account, task_update_product, task_update_agent]

    # Fact update depends on dimensions being up-to-date for key lookups
    [task_update_account, task_update_product, task_update_agent] >> task_update_facts

    task_update_facts >> end

# Instantiate the DAG
b2b_incremental_load_dag()
