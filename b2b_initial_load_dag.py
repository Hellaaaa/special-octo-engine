# /path/to/your/airflow/dags/b2b_initial_load_dag.py
import pendulum
import logging
from datetime import timedelta, date
# Ensure psycopg2 errors can be caught if needed, requires postgres provider installed
try:
    import psycopg2
except ImportError:
    psycopg2 = None # Allows code to run but specific DB error checks might fail


from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException, AirflowNotFoundException
from airflow.models.baseoperator import cross_downstream
from airflow.operators.empty import EmptyOperator

# --- Configuration ---
OLTP_CONN_ID = "b2b_sales" # Connection ID for your source OLTP database
OLAP_CONN_ID = "b2b_sales_olap" # Connection ID for your target OLAP database
DEFAULT_BATCH_SIZE = 1000 # Number of rows to process in each batch for large tables
DEFAULT_MIN_DATE = '2015-01-01' # Default start date for DimDate if source is empty
DEFAULT_MAX_DATE = '2030-12-31' # Default end date for DimDate if source is empty
# Default start date for SCD2 validity when history is unknown during initial load
DEFAULT_SCD2_VALID_FROM = '1900-01-01'
# Prefix for Airflow Variables used by the incremental DAG's watermarking
WATERMARK_VAR_PREFIX = "b2b_incremental_watermark_"
# Default start time for timestamp watermarks (used when resetting)
DEFAULT_START_TIME = '1970-01-01T00:00:00+00:00'

# --- Helper Functions ---
# (get/set/reset_batch_state remain the same)
def get_batch_state(variable_name, default_value=0):
    """Gets the starting offset for the next batch for this specific DAG run."""
    try:
        if Variable.get(variable_name, default_var=None) is None: return default_value
        return int(Variable.get(variable_name))
    except ValueError: logging.warning(f"Batch state variable {variable_name} has non-integer value, using default {default_value}"); return default_value
    except Exception as e: logging.error(f"Could not retrieve batch state variable {variable_name}: {e}"); logging.warning(f"Using default {default_value} due to error retrieving variable."); return default_value
def set_batch_state(variable_name, value):
    """Sets the starting offset for the next batch for this specific DAG run."""
    try: Variable.set(variable_name, str(value))
    except Exception as e: logging.error(f"Could not set batch state variable {variable_name}: {e}")
def reset_batch_state(variable_name):
     """Deletes the batch state variable at the start of a full load."""
     try: Variable.delete(variable_name); logging.info(f"Reset batch state variable {variable_name}.")
     except AirflowNotFoundException: logging.info(f"Batch state variable {variable_name} not found, no need to delete.")
     except Exception as e: logging.warning(f"Could not delete batch state variable {variable_name}: {e}")

# --- DAG Definition ---
@dag(
    dag_id='b2b_initial_load',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None, # This DAG is intended for manual trigger
    catchup=False,
    # default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)}, # Commented out retries for faster testing
    default_args={'retry_delay': timedelta(minutes=2)}, # Keep retry delay
    tags=['b2b_sales', 'initial_load', 'repopulatable', 'refined', 'scd2_product_only'],
    doc_md="""
    ### B2B Sales Initial Load DAG (Reverted to Quoted Identifiers)

    Performs the initial population of the OLAP database from the OLTP source.
    **TRUNCATES target tables (with CASCADE). Uses double-quoted identifiers for OLAP table/column names.**
    **Implements SCD Type 2 logic ONLY for DimProduct during initial load.**
    **DimAccount and DimSalesAgent are loaded as SCD Type 1.**
    Resets timestamp-based watermarks for the incremental load DAG at the end.
    **Looks up ProductSK for FactSalesPerformance.ProductKey.**

    **WARNING:** Running this DAG will WIPE existing data in the target OLAP tables. Ensure OLTP has `updated_at` columns and OLAP schema is correct (SCD2 cols for DimProduct, ProductSK as PK; OpportunityID in FactSalesPerformance).
    """
)
def b2b_initial_load_dag():

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # --- Task Group: Reset Batch State Variables ---
    @task
    def reset_all_batch_states():
        """Resets offset variables used for batching within this DAG run."""
        state_vars = ["initial_load_dim_account_offset", "initial_load_dim_product_offset", "initial_load_dim_sales_agent_offset", "initial_load_fact_sales_perf_offset"]
        logging.info("Resetting batch state variables for initial load...");
        for var_name in state_vars: reset_batch_state(var_name)
        logging.info("Batch state variables reset.")

    task_reset_states = reset_all_batch_states()

    # --- Task Group: Truncate Tables ---
    def _truncate_table(table_name: str, cascade: bool = False):
        """Helper Python function to truncate a table in the OLAP database."""
        # Use double quotes around table name passed as argument
        quoted_table_name = f'"{table_name}"'
        hook = PostgresHook(postgres_conn_id=OLAP_CONN_ID);
        sql = f"TRUNCATE TABLE {quoted_table_name}{' CASCADE' if cascade else ''};"
        logging.info(f"Running TRUNCATE command: {sql}")
        try: hook.run(sql); logging.info(f"Successfully truncated table {table_name}.")
        except Exception as e: logging.error(f"Failed to truncate table {table_name}: {e}"); raise

    # Pass table names exactly as defined in schema (likely mixed case)
    @task(task_id='truncate_fact_sales_performance')
    def python_truncate_fact_sales_performance(): _truncate_table("FactSalesPerformance", cascade=True)
    @task(task_id='truncate_fact_sales_monthly_aggregate')
    def python_truncate_fact_monthly_aggregate(): _truncate_table("FactSalesMonthlyAggregate", cascade=True)
    @task(task_id='truncate_dim_account')
    def python_truncate_dim_account(): _truncate_table("DimAccount", cascade=True)
    @task(task_id='truncate_dim_product')
    def python_truncate_dim_product(): _truncate_table("DimProduct", cascade=True)
    @task(task_id='truncate_dim_sales_agent')
    def python_truncate_dim_sales_agent(): _truncate_table("DimSalesAgent", cascade=True)
    @task(task_id='truncate_dim_deal_stage')
    def python_truncate_dim_deal_stage(): _truncate_table("DimDealStage", cascade=True)
    @task(task_id='truncate_dim_date')
    def python_truncate_dim_date(): _truncate_table("DimDate", cascade=True)

    truncate_fact_sales_perf_task = python_truncate_fact_sales_performance()
    truncate_fact_monthly_agg_task = python_truncate_fact_monthly_aggregate()
    truncate_dim_account_task = python_truncate_dim_account()
    truncate_dim_product_task = python_truncate_dim_product()
    truncate_dim_sales_agent_task = python_truncate_dim_sales_agent()
    truncate_dim_deal_stage_task = python_truncate_dim_deal_stage()
    truncate_dim_date_task = python_truncate_dim_date()
    truncate_facts = [truncate_fact_sales_perf_task, truncate_fact_monthly_agg_task]
    truncate_dims = [truncate_dim_account_task, truncate_dim_product_task, truncate_dim_sales_agent_task, truncate_dim_deal_stage_task, truncate_dim_date_task]

    # --- Dimension Load Tasks (Initial Load Logic) ---
    # Use quoted table and column names for OLAP interactions
    @task
    def load_dim_date():
        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID); hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        sql_get_range = "SELECT MIN(EngageDate), MAX(COALESCE(CloseDate, EngageDate)) FROM SalesPipeline;"
        logging.info("Querying OLTP for date range..."); result = hook_oltp.get_first(sql_get_range)
        min_date_str = DEFAULT_MIN_DATE; max_date_str = DEFAULT_MAX_DATE
        try:
            if result and result[0] and result[1]: min_dt, max_dt = result[0], result[1]; min_date_str = pendulum.instance(min_dt).to_date_string(); max_date_str = pendulum.instance(max_dt).to_date_string(); logging.info(f"Determined date range: {min_date_str} to {max_date_str}")
            else: logging.warning("Could not determine date range. Using defaults.")
        except Exception as e: logging.error(f"Error processing date range: {e}. Using defaults.")
        min_year_start = pendulum.parse(min_date_str).start_of('year').to_date_string(); max_year_end = pendulum.parse(max_date_str).end_of('year').to_date_string()
        # Use quoted identifiers for OLAP table/columns
        sql_insert_dates = f"""INSERT INTO "DimDate" ("DateKey", "Date", "Day", "Month", "Quarter", "Year") SELECT TO_CHAR(datum, 'YYYYMMDD')::INT, datum, EXTRACT(DAY FROM datum), EXTRACT(MONTH FROM datum), EXTRACT(QUARTER FROM datum), EXTRACT(YEAR FROM datum) FROM generate_series('{min_year_start}'::DATE, '{max_year_end}'::DATE, '1 day'::INTERVAL) datum;"""
        logging.info("Populating DimDate..."); hook_olap.run(sql_insert_dates); logging.info("DimDate populated.")
    @task
    def load_dim_deal_stage():
        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID); hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        stages = hook_oltp.get_records("SELECT DealStageID, StageName FROM DealStages")
        # Use quoted identifiers
        if stages: hook_olap.insert_rows(table='"DimDealStage"', rows=stages, target_fields=['"StageID"', '"StageName"'], commit_every=DEFAULT_BATCH_SIZE); logging.info(f"Loaded {len(stages)} rows into DimDealStage.")
    @task
    def load_dim_account(): # SCD1
        variable_name = "initial_load_dim_account_offset"; batch_size = DEFAULT_BATCH_SIZE; total_rows_processed = 0
        while True:
            current_offset = get_batch_state(variable_name, 0); hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            sql_extract = """SELECT a.AccountID, a.AccountName, s.SectorName, CASE WHEN a.Revenue < 1000000 THEN 'Under 1M' WHEN a.Revenue BETWEEN 1000000 AND 10000000 THEN '1M-10M' WHEN a.Revenue > 10000000 THEN 'Over 10M' ELSE 'Unknown' END, a.ParentAccountID FROM Accounts a LEFT JOIN Sectors s ON a.SectorID = s.SectorID ORDER BY a.AccountID LIMIT %s OFFSET %s;"""
            source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))
            if not source_data: break
            olap_data = [(row[0], row[1], row[2], row[3], row[4]) for row in source_data]; target_fields = ['"AccountID"', '"AccountName"', '"Sector"', '"RevenueRange"', '"ParentAccountID"']
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            try: hook_olap.insert_rows(table='"DimAccount"', rows=olap_data, target_fields=target_fields, commit_every=batch_size); rows_loaded = len(olap_data); total_rows_processed += rows_loaded; set_batch_state(variable_name, current_offset + rows_loaded);
            except Exception as e: logging.error(f"Failed DimAccount batch {current_offset}: {e}"); raise AirflowSkipException() from e
            if len(source_data) < batch_size: break
        logging.info(f"Finished loading DimAccount (SCD1). Total: {total_rows_processed}")
    @task
    def load_dim_product(): # SCD2
        variable_name = "initial_load_dim_product_offset"; batch_size = DEFAULT_BATCH_SIZE; total_rows_processed = 0
        initial_valid_from = DEFAULT_SCD2_VALID_FROM; logging.info(f"Initial load DimProduct (SCD2), ValidFrom={initial_valid_from}")
        while True:
            current_offset = get_batch_state(variable_name, 0); hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            sql_extract = """SELECT p.ProductID, p.ProductName, ps.SeriesName, p.SalesPrice, CASE WHEN p.SalesPrice < 100 THEN 'Low' WHEN p.SalesPrice BETWEEN 100 AND 1000 THEN 'Medium' WHEN p.SalesPrice > 1000 THEN 'High' ELSE 'Unknown' END FROM Products p LEFT JOIN ProductSeries ps ON p.SeriesID = ps.SeriesID ORDER BY p.ProductID LIMIT %s OFFSET %s;"""
            source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))
            if not source_data: break
            olap_data = [(row[0], row[1], row[2], row[3], row[4], initial_valid_from, None, True) for row in source_data]; target_fields = ['"ProductID"', '"ProductName"', '"SeriesName"', '"Price"', '"PriceRange"', '"ValidFrom"', '"ValidTo"', '"IsCurrent"']
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            try: hook_olap.insert_rows(table='"DimProduct"', rows=olap_data, target_fields=target_fields, commit_every=batch_size); rows_loaded = len(olap_data); total_rows_processed += rows_loaded; set_batch_state(variable_name, current_offset + rows_loaded);
            except Exception as e: logging.error(f"Failed DimProduct batch {current_offset}: {e}"); raise AirflowSkipException() from e
            if len(source_data) < batch_size: break
        logging.info(f"Finished loading DimProduct (SCD2). Total: {total_rows_processed}")
    @task
    def load_dim_sales_agent(): # SCD1
        variable_name = "initial_load_dim_sales_agent_offset"; batch_size = DEFAULT_BATCH_SIZE; total_rows_processed = 0
        while True:
            current_offset = get_batch_state(variable_name, 0); hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            sql_extract = """SELECT sa.SalesAgentID, sa.SalesAgentName, sm.ManagerName, l.LocationName AS Region FROM SalesAgents sa LEFT JOIN SalesManagers sm ON sa.ManagerID = sm.ManagerID LEFT JOIN Locations l ON sa.RegionalOfficeID = l.LocationID ORDER BY sa.SalesAgentID LIMIT %s OFFSET %s;"""
            source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))
            if not source_data: break
            olap_data = [(row[0], row[1], row[2], row[3]) for row in source_data]; target_fields = ['"SalesAgentID"', '"SalesAgentName"', '"ManagerName"', '"Region"']
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            try: hook_olap.insert_rows(table='"DimSalesAgent"', rows=olap_data, target_fields=target_fields, commit_every=batch_size); rows_loaded = len(olap_data); total_rows_processed += rows_loaded; set_batch_state(variable_name, current_offset + rows_loaded);
            except Exception as e: logging.error(f"Failed DimSalesAgent batch {current_offset}: {e}"); raise AirflowSkipException() from e
            if len(source_data) < batch_size: break
        logging.info(f"Finished loading DimSalesAgent (SCD1). Total: {total_rows_processed}")

    # --- Task: Fact Table - SalesPerformance (Initial Load) ---
    @task
    def load_fact_sales_performance():
        """Loads FactSalesPerformance, looking up appropriate dimension keys (using ProductSK)."""
        variable_name = "initial_load_fact_sales_perf_offset"; batch_size = DEFAULT_BATCH_SIZE; total_rows_processed = 0
        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID); hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        while True:
            current_offset = get_batch_state(variable_name, 0)
            sql_extract = """SELECT sp.OpportunityID, sp.SalesAgentID, sp.ProductID, sp.AccountID, sp.DealStageID, sp.EngageDate, sp.CloseDate, sp.CloseValue, CASE WHEN sp.CloseDate IS NOT NULL AND sp.EngageDate IS NOT NULL THEN sp.CloseDate - sp.EngageDate ELSE NULL END, CASE ds.StageName WHEN 'Won' THEN 100.0 WHEN 'Lost' THEN 0.0 WHEN 'Engaging' THEN 75.0 WHEN 'Prospecting' THEN 25.0 ELSE 10.0 END, COALESCE(sp.CloseDate, sp.EngageDate) as EventDate FROM SalesPipeline sp LEFT JOIN DealStages ds ON sp.DealStageID = ds.DealStageID WHERE sp.AccountID IS NOT NULL AND sp.ProductID IS NOT NULL AND sp.SalesAgentID IS NOT NULL AND sp.DealStageID IS NOT NULL AND COALESCE(sp.CloseDate, sp.EngageDate) IS NOT NULL ORDER BY sp.OpportunityID LIMIT %s OFFSET %s;"""
            try: source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))
            except Exception as e: logging.error(f"Failed fetch SalesPipeline offset {current_offset}: {e}"); raise AirflowSkipException() from e
            if not source_data: break
            olap_data_to_insert = []; missing_keys_count = 0
            for row in source_data:
                try:
                    opportunity_id, oltp_sales_agent_id, oltp_product_id, oltp_account_id, oltp_deal_stage_id, _, _, close_value, duration_days, expected_success_rate, event_date = row
                    event_date_str = pendulum.instance(event_date).to_date_string()
                    # Lookups: Use quoted identifiers for OLAP tables/columns
                    date_key_res = hook_olap.get_first('SELECT "DateKey" FROM "DimDate" WHERE "Date" = %s', parameters=(event_date_str,))
                    account_key_res = hook_olap.get_first('SELECT "AccountID" FROM "DimAccount" WHERE "AccountID" = %s', parameters=(oltp_account_id,))
                    product_sk_res = hook_olap.get_first('SELECT "ProductSK" FROM "DimProduct" WHERE "ProductID" = %s AND "IsCurrent" = TRUE', parameters=(oltp_product_id,)) # Lookup ProductSK
                    agent_key_res = hook_olap.get_first('SELECT "SalesAgentID" FROM "DimSalesAgent" WHERE "SalesAgentID" = %s', parameters=(oltp_sales_agent_id,))
                    stage_key_res = hook_olap.get_first('SELECT "StageID" FROM "DimDealStage" WHERE "StageID" = %s', parameters=(oltp_deal_stage_id,))
                    date_key = date_key_res[0] if date_key_res else None; account_key = account_key_res[0] if account_key_res else None
                    product_key = product_sk_res[0] if product_sk_res else None # This is ProductSK
                    agent_key = agent_key_res[0] if agent_key_res else None; stage_key = stage_key_res[0] if stage_key_res else None
                    if all([date_key, account_key, product_key, agent_key, stage_key]):
                         # ** Assuming OpportunityID column exists in factsalesperformance **
                         olap_data_to_insert.append((opportunity_id, date_key, account_key, product_key, agent_key, stage_key, close_value, duration_days, expected_success_rate))
                    else: missing_keys_count += 1; logging.warning(f"Initial Load: Skip OppID {opportunity_id} missing key for Event {event_date_str}. Found:[D:{date_key is not None},A:{account_key is not None},P:{product_key is not None},Ag:{agent_key is not None},S:{stage_key is not None}]")
                except Exception as lookup_ex: missing_keys_count += 1; logging.error(f"Initial Load: Dim lookup error OppID {row[0]}: {lookup_ex}")
            if olap_data_to_insert:
                try:
                    # Use quoted identifiers for target table and fields
                    # ** Assuming OpportunityID column exists in factsalesperformance **
                    target_fields = ['"OpportunityID"', '"DateKey"', '"AccountKey"', '"ProductKey"', '"SalesAgentKey"', '"DealStageKey"', '"CloseValue"', '"DurationDays"', '"ExpectedSuccessRate"']
                    hook_olap.insert_rows(table='"FactSalesPerformance"', rows=olap_data_to_insert, target_fields=target_fields, commit_every=batch_size)
                    rows_loaded = len(olap_data_to_insert); total_rows_processed += rows_loaded; set_batch_state(variable_name, current_offset + len(source_data))
                except Exception as e: logging.error(f"Failed fact insert batch {current_offset}: {e}"); raise AirflowSkipException() from e
            else: set_batch_state(variable_name, current_offset + len(source_data)); logging.warning(f"No valid fact data in batch {current_offset}")
            if len(source_data) < batch_size: break
        logging.info(f"Finished loading FactSalesPerformance. Inserted: {total_rows_processed}. Skipped: {missing_keys_count}")

    # --- Task: Reset Incremental Timestamp Watermarks ---
    @task
    def reset_incremental_watermarks():
        """Resets timestamp watermarks used by the incremental DAG to the default start time."""
        incremental_tables = ['accounts', 'products', 'salesagents', 'salespipeline']
        logging.info("Resetting timestamp watermarks..."); [Variable.set(f"{WATERMARK_VAR_PREFIX}{t}", DEFAULT_START_TIME) for t in incremental_tables]; logging.info("Watermarks reset.")

    # --- Instantiate Tasks & Define Flow ---
    task_load_date=load_dim_date(); task_load_stage=load_dim_deal_stage(); task_load_account=load_dim_account(); task_load_product=load_dim_product(); task_load_agent=load_dim_sales_agent(); task_load_facts=load_fact_sales_performance(); task_reset_watermarks=reset_incremental_watermarks()
    load_dims_tasks = [task_load_date, task_load_stage, task_load_account, task_load_product, task_load_agent]
    start >> task_reset_states >> truncate_facts; cross_downstream(truncate_facts, truncate_dims); cross_downstream(truncate_dims, load_dims_tasks); load_dims_tasks >> task_load_facts >> task_reset_watermarks >> end

b2b_initial_load_dag()
