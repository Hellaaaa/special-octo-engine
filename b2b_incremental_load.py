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
        current_value = "N/A";
        try: current_value = Variable.get(var_name)
        except Exception: pass
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
    tags=['b2b_sales', 'incremental_load', 'refined', 'scd2_product_only', 'timestamp_watermark'],
    doc_md="""
    ### B2B Sales Incremental Load DAG (SCD2 for DimProduct Only - Corrected)

    Loads new and updated data from OLTP to OLAP using `updated_at` timestamps and watermarks.
    Implements **SCD Type 2** ONLY for `DimProduct`.
    Implements **SCD Type 1** (Upsert) for `DimAccount` and `DimSalesAgent`.
    Uses **UPSERT** for `FactSalesPerformance` based on `OpportunityID`.

    **Assumptions:**
    - OLTP tables (`Accounts`, `Products`, `SalesAgents`, `SalesPipeline`) have a reliable `updated_at` column.
    - `FactSalesPerformance` in OLAP has an `OpportunityID` column with a `UNIQUE` constraint.
    - `DimProduct` in OLAP has `ValidFrom`, `ValidTo`, `IsCurrent` columns.
    - `DimAccount`, `DimSalesAgent` in OLAP DO NOT have SCD2 columns.

    **CDC/Watermarking:** Uses `updated_at` and Airflow Variables.

    **Tasks:**
    1. Update DimAccount (SCD Type 1 - Upsert).
    2. Update DimProduct (SCD Type 2 - Expire/Insert).
    3. Update DimSalesAgent (SCD Type 1 - Upsert).
    4. Load/Update FactSalesPerformance (Upsert, with appropriate dimension key lookups).
    """
)
def b2b_incremental_load_dag():

    start = EmptyOperator(task_id='start_incremental_load')
    end = EmptyOperator(task_id='end_incremental_load')

    # --- Task 1: Dimension - Account (Incremental Update - SCD Type 1 - Upsert) ---
    @task
    def update_dim_account(**context):
        table_key = 'accounts' # Key for OLTP table and watermark
        start_time_str = get_watermark(table_key)
        end_time = pendulum.now('UTC')
        end_time_iso = end_time.isoformat()
        logging.info(f"Checking for Account updates (SCD1) between {start_time_str} and {end_time_iso}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        # Fetch changed/new data based on updated_at
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
             logging.error(f"Failed fetch from OLTP {table_key}: {e}")
             if psycopg2 and isinstance(e, psycopg2.errors.UndefinedColumn) and "updated_at" in str(e):
                 logging.error(f"Hint: Ensure 'updated_at' exists in OLTP '{table_key}'. Run SQL script 'add_timestamps_sql'.")
             raise

        if not changed_data:
            logging.info("No Account changes detected."); set_watermark(table_key, end_time_iso); return

        logging.info(f"Found {len(changed_data)} changed/new accounts for SCD1 upsert.")
        # Prepare data for direct upsert (SCD Type 1)
        olap_data = [(row[0], row[1], row[2], row[3], row[4]) for row in changed_data]
        target_fields = ["AccountID", "AccountName", "Sector", "RevenueRange", "ParentAccountID"]

        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        try:
            # Use insert_rows with replace=True for SCD Type 1 Upsert
            # This handles both inserts and updates based on the primary key (AccountID)
            hook_olap.insert_rows(
                table="DimAccount",
                rows=olap_data,
                target_fields=target_fields,
                commit_every=1000,
                replace=True,          # Key flag for SCD1 upsert
                replace_index="AccountID" # The column to check for conflicts
            )
            logging.info(f"Upserted {len(olap_data)} records into DimAccount (SCD1).")
            set_watermark(table_key, end_time_iso) # Update watermark on success
        except Exception as e:
            logging.error(f"Error during Upsert into DimAccount (SCD1): {e}")
            # Check if error is related to missing SCD2 columns (should not happen now)
            if psycopg2 and isinstance(e, psycopg2.errors.UndefinedColumn) and ("iscurrent" in str(e) or "validto" in str(e) or "validfrom" in str(e)):
                 logging.error(f"Hint: SCD2 columns should not be present or used in DimAccount for SCD1 logic.")
            raise # Fail task if upsert fails

    # --- Task 2: Dimension - Product (Incremental Update - SCD Type 2) ---
    @task
    def update_dim_product(**context):
        table_key = 'products'
        start_time_str = get_watermark(table_key)
        end_time = pendulum.now('UTC')
        end_time_iso = end_time.isoformat()
        today_date = end_time.to_date_string()
        previous_day = end_time.subtract(days=1).to_date_string()
        logging.info(f"Checking for Product updates (SCD2) between {start_time_str} and {end_time_iso}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        sql_extract = """
        SELECT p.ProductID, p.ProductName, ps.SeriesName, p.SalesPrice, CASE WHEN p.SalesPrice < 100 THEN 'Low' WHEN p.SalesPrice BETWEEN 100 AND 1000 THEN 'Medium' WHEN p.SalesPrice > 1000 THEN 'High' ELSE 'Unknown' END
        FROM Products p LEFT JOIN ProductSeries ps ON p.SeriesID = ps.SeriesID
        WHERE p.updated_at > %s AND p.updated_at <= %s;"""
        try:
            changed_data = hook_oltp.get_records(sql_extract, parameters=(start_time_str, end_time_iso))
        except Exception as e:
             logging.error(f"Failed fetch from OLTP {table_key}: {e}")
             if psycopg2 and isinstance(e, psycopg2.errors.UndefinedColumn) and "updated_at" in str(e): logging.error(f"Hint: Ensure 'updated_at' exists in OLTP '{table_key}'.")
             raise

        if not changed_data:
            logging.info("No Product changes detected."); set_watermark(table_key, end_time_iso); return

        logging.info(f"Found {len(changed_data)} changed/new products for SCD Type 2.")
        processed_count = 0
        try:
            with hook_olap.get_conn() as conn:
                with conn.cursor() as cur:
                    for row in changed_data:
                        product_id, new_name, new_series, new_price, new_price_range = row
                        # Check current active record in OLAP DimProduct
                        cur.execute("SELECT ProductName, SeriesName, Price, PriceRange FROM DimProduct WHERE ProductID = %s AND IsCurrent = TRUE;", (product_id,))
                        current_active = cur.fetchone()
                        attributes_changed = False
                        if current_active:
                            old_name, old_series, old_price, old_range = current_active
                            if (new_name != old_name or new_series != old_series or new_price != old_price or new_price_range != old_range): attributes_changed = True
                        else: attributes_changed = True # No active record, needs insert

                        if attributes_changed:
                            logging.info(f"SCD Type 2: Change detected for ProductID: {product_id}. Updating history.")
                            # 1. Expire old record (if exists)
                            if current_active:
                                cur.execute("UPDATE DimProduct SET ValidTo = %s, IsCurrent = FALSE WHERE ProductID = %s AND IsCurrent = TRUE;", (previous_day, product_id))
                            # 2. Insert new record
                            cur.execute("""INSERT INTO DimProduct (ProductID, ProductName, SeriesName, Price, PriceRange, ValidFrom, ValidTo, IsCurrent) VALUES (%s, %s, %s, %s, %s, %s, NULL, TRUE);""", (product_id, new_name, new_series, new_price, new_price_range, today_date))
                            processed_count += 1
                        else: logging.info(f"No significant change for existing ProductID: {product_id}")
                conn.commit()
            logging.info(f"SCD Type 2 processing complete for DimProduct. Processed: {processed_count}")
            set_watermark(table_key, end_time_iso)
        except Exception as e:
            # Check for specific error related to missing SCD2 columns
            if psycopg2 and isinstance(e, psycopg2.errors.UndefinedColumn) and ("iscurrent" in str(e) or "validto" in str(e) or "validfrom" in str(e)):
                 logging.error(f"Hint: Ensure columns 'ValidFrom', 'ValidTo', 'IsCurrent' exist in OLAP table 'DimProduct'. Check SQL script 'add_scd2_columns_olap_sql'.")
            logging.error(f"Error during SCD Type 2 for DimProduct: {e}"); raise

    # --- Task 3: Dimension - SalesAgent (Incremental Update - SCD Type 1 - Upsert) ---
    @task
    def update_dim_sales_agent(**context):
        table_key = 'salesagents'
        start_time_str = get_watermark(table_key)
        end_time = pendulum.now('UTC')
        end_time_iso = end_time.isoformat()
        logging.info(f"Checking for SalesAgent updates (SCD1) between {start_time_str} and {end_time_iso}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        sql_extract = """
        SELECT sa.SalesAgentID, sa.SalesAgentName, sm.ManagerName, l.LocationName AS Region
        FROM SalesAgents sa LEFT JOIN SalesManagers sm ON sa.ManagerID = sm.ManagerID LEFT JOIN Locations l ON sa.RegionalOfficeID = l.LocationID
        WHERE sa.updated_at > %s AND sa.updated_at <= %s;"""
        try:
            changed_data = hook_oltp.get_records(sql_extract, parameters=(start_time_str, end_time_iso))
        except Exception as e:
             logging.error(f"Failed fetch from OLTP {table_key}: {e}")
             if psycopg2 and isinstance(e, psycopg2.errors.UndefinedColumn) and "updated_at" in str(e): logging.error(f"Hint: Ensure 'updated_at' exists in OLTP '{table_key}'.")
             raise

        if not changed_data:
            logging.info("No SalesAgent changes detected."); set_watermark(table_key, end_time_iso); return

        logging.info(f"Found {len(changed_data)} changed/new sales agents for SCD1 upsert.")
        olap_data = [(row[0], row[1], row[2], row[3]) for row in changed_data]
        target_fields = ["SalesAgentID", "SalesAgentName", "ManagerName", "Region"]

        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        try:
             # Use insert_rows with replace=True for SCD Type 1 Upsert
            hook_olap.insert_rows(
                table="DimSalesAgent", rows=olap_data, target_fields=target_fields,
                commit_every=1000, replace=True, replace_index="SalesAgentID"
            )
            logging.info(f"Upserted {len(olap_data)} records into DimSalesAgent (SCD1).")
            set_watermark(table_key, end_time_iso)
        except Exception as e:
            logging.error(f"Error during Upsert into DimSalesAgent (SCD1): {e}")
            if psycopg2 and isinstance(e, psycopg2.errors.UndefinedColumn) and ("iscurrent" in str(e) or "validto" in str(e) or "validfrom" in str(e)):
                 logging.error(f"Hint: SCD2 columns should not be present or used in DimSalesAgent for SCD1 logic.")
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
        sql_extract = """
        SELECT sp.OpportunityID, sp.SalesAgentID, sp.ProductID, sp.AccountID, sp.DealStageID, sp.EngageDate, sp.CloseDate, sp.CloseValue, CASE WHEN sp.CloseDate IS NOT NULL AND sp.EngageDate IS NOT NULL THEN sp.CloseDate - sp.EngageDate ELSE NULL END, CASE ds.StageName WHEN 'Won' THEN 100.0 WHEN 'Lost' THEN 0.0 WHEN 'Engaging' THEN 75.0 WHEN 'Prospecting' THEN 25.0 ELSE 10.0 END, COALESCE(sp.CloseDate, sp.EngageDate) as EventDate
        FROM SalesPipeline sp LEFT JOIN DealStages ds ON sp.DealStageID = ds.DealStageID
        WHERE sp.updated_at > %s AND sp.updated_at <= %s AND sp.AccountID IS NOT NULL AND sp.ProductID IS NOT NULL AND sp.SalesAgentID IS NOT NULL AND sp.DealStageID IS NOT NULL AND COALESCE(sp.CloseDate, sp.EngageDate) IS NOT NULL;"""
        try:
            changed_data = hook_oltp.get_records(sql_extract, parameters=(start_time_str, end_time_iso))
        except Exception as e:
             logging.error(f"Failed fetch from OLTP {table_key}: {e}")
             if psycopg2 and isinstance(e, psycopg2.errors.UndefinedColumn) and "updated_at" in str(e): logging.error(f"Hint: Ensure 'updated_at' exists in OLTP '{table_key}'.")
             raise

        if not changed_data:
            logging.info("No SalesPipeline changes detected."); set_watermark(table_key, end_time_iso); return

        logging.info(f"Found {len(changed_data)} changed/new pipeline records.")
        olap_data_to_upsert = []; missing_keys_count = 0
        for row in changed_data:
            try:
                opportunity_id, oltp_sales_agent_id, oltp_product_id, oltp_account_id, oltp_deal_stage_id, _, _, close_value, duration_days, expected_success_rate, event_date = row
                event_date_str = event_date.strftime('%Y-%m-%d') if hasattr(event_date, 'strftime') else str(event_date)

                # --- Dimension Key Lookups ---
                date_key_result = hook_olap.get_first("SELECT DateKey FROM DimDate WHERE Date = %s", parameters=(event_date_str,))
                # AccountKey (SCD1 - direct lookup)
                account_key_result = hook_olap.get_first("SELECT AccountID FROM DimAccount WHERE AccountID = %s", parameters=(oltp_account_id,))
                # ProductKey (SCD2 - lookup based on event date)
                product_key_result = hook_olap.get_first("SELECT ProductID FROM DimProduct WHERE ProductID = %s AND %s >= ValidFrom AND (%s < ValidTo OR ValidTo IS NULL)", parameters=(oltp_product_id, event_date_str, event_date_str))
                # SalesAgentKey (SCD1 - direct lookup)
                agent_key_result = hook_olap.get_first("SELECT SalesAgentID FROM DimSalesAgent WHERE SalesAgentID = %s", parameters=(oltp_sales_agent_id,))
                # DealStageKey (Static)
                stage_key_result = hook_olap.get_first("SELECT StageID FROM DimDealStage WHERE StageID = %s", parameters=(oltp_deal_stage_id,))

                date_key = date_key_result[0] if date_key_result else None
                account_key = account_key_result[0] if account_key_result else None
                product_key = product_key_result[0] if product_key_result else None
                agent_key = agent_key_result[0] if agent_key_result else None
                stage_key = stage_key_result[0] if stage_key_result else None

                if all([date_key, account_key, product_key, agent_key, stage_key]):
                    olap_data_to_upsert.append((opportunity_id, date_key, account_key, product_key, agent_key, stage_key, close_value, duration_days, expected_success_rate))
                else:
                    missing_keys_count += 1; logging.warning(f"Skipping OppID {opportunity_id} due to missing dim key for EventDate {event_date_str}. Found: [D:{date_key is not None}, A:{account_key is not None}, P:{product_key is not None}, Ag:{agent_key is not None}, S:{stage_key is not None}]")
            except Exception as lookup_ex: missing_keys_count += 1; logging.error(f"Dim lookup error for OppID {row[0]} (EventDate: {row[10]}): {lookup_ex}")

        if not olap_data_to_upsert:
             logging.warning("No data for Fact UPSERT after lookups."); set_watermark(table_key, end_time_iso); return

        logging.info(f"Prepared {len(olap_data_to_upsert)} records for Fact UPSERT.");
        if missing_keys_count > 0: logging.warning(f"Skipped {missing_keys_count} fact records due to missing dim keys.")

        # Perform UPSERT
        try:
            with hook_olap.get_conn() as conn:
                with conn.cursor() as cur:
                    upsert_sql = """
                    INSERT INTO FactSalesPerformance (OpportunityID, DateKey, AccountKey, ProductKey, SalesAgentKey, DealStageKey, CloseValue, DurationDays, ExpectedSuccessRate)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (OpportunityID) DO UPDATE SET DateKey = EXCLUDED.DateKey, AccountKey = EXCLUDED.AccountKey, ProductKey = EXCLUDED.ProductKey, SalesAgentKey = EXCLUDED.SalesAgentKey, DealStageKey = EXCLUDED.DealStageKey, CloseValue = EXCLUDED.CloseValue, DurationDays = EXCLUDED.DurationDays, ExpectedSuccessRate = EXCLUDED.ExpectedSuccessRate;"""
                    cur.executemany(upsert_sql, olap_data_to_upsert)
                conn.commit()
            logging.info(f"Successfully UPSERTED {len(olap_data_to_upsert)} records into FactSalesPerformance.")
            set_watermark(table_key, end_time_iso)
        except Exception as e:
            logging.error(f"Error during UPSERT into FactSalesPerformance: {e}")
            if psycopg2 and isinstance(e, psycopg2.errors.UniqueViolation) and "OpportunityID" in str(e): logging.error("Hint: Ensure OpportunityID column exists in FactSalesPerformance and has a UNIQUE constraint.")
            elif psycopg2 and isinstance(e, psycopg2.errors.ForeignKeyViolation): logging.error("Hint: Check if looked-up dimension keys exist.")
            raise

    # --- Task Dependencies ---
    task_update_account = update_dim_account()
    task_update_product = update_dim_product()
    task_update_agent = update_dim_sales_agent()
    task_update_facts = update_fact_sales_performance()

    start >> [task_update_account, task_update_product, task_update_agent]
    [task_update_account, task_update_product, task_update_agent] >> task_update_facts
    task_update_facts >> end

# Instantiate the DAG
b2b_incremental_load_dag()
