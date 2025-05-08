# /path/to/your/airflow/dags/b2b_initial_load_dag.py
import pendulum
import logging
from datetime import timedelta, date, datetime # <-- Import datetime class
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
OLAP_SCHEMA = "public" # Explicitly define the schema for OLAP tables
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
    ### B2B Sales Initial Load DAG (Fact Load ON CONFLICT)

    Performs the initial population of the OLAP database from the OLTP source.
    **TRUNCATES target tables (with CASCADE). Uses lowercase unquoted identifiers qualified with the schema.**
    **Implements SCD Type 2 logic ONLY for dimproduct during initial load.**
    **dimaccount and dimsalesagent are loaded using explicit UPSERT (SCD Type 1).**
    **factsalesperformance loaded using INSERT ON CONFLICT DO NOTHING to handle potential source duplicates.**
    Resets timestamp-based watermarks for the incremental load DAG at the end.
    **Looks up productsk for factsalesperformance.productkey.**

    **WARNING:** Running this DAG will WIPE existing data in the target OLAP tables. Ensure OLTP has `updated_at` columns and OLAP schema is correct.
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
        qualified_table_name = f"{OLAP_SCHEMA}.{table_name}"
        hook = PostgresHook(postgres_conn_id=OLAP_CONN_ID);
        sql = f"TRUNCATE TABLE {qualified_table_name}{' CASCADE' if cascade else ''};"
        logging.info(f"Running TRUNCATE command: {sql}")
        try: hook.run(sql); logging.info(f"Successfully truncated table {qualified_table_name}.")
        except Exception as e:
            if psycopg2 and isinstance(e, psycopg2.errors.UndefinedTable): logging.warning(f"Table {qualified_table_name} does not exist, skipping truncate."); return
            logging.error(f"Failed to truncate table {qualified_table_name}: {e}"); raise

    @task(task_id='truncate_fact_sales_performance')
    def python_truncate_fact_sales_performance(): _truncate_table("factsalesperformance", cascade=True)
    @task(task_id='truncate_fact_sales_monthly_aggregate')
    def python_truncate_fact_monthly_aggregate(): _truncate_table("factsalesmonthlyaggregate", cascade=True)
    @task(task_id='truncate_dim_account')
    def python_truncate_dim_account(): _truncate_table("dimaccount", cascade=True)
    @task(task_id='truncate_dim_product')
    def python_truncate_dim_product(): _truncate_table("dimproduct", cascade=True)
    @task(task_id='truncate_dim_sales_agent')
    def python_truncate_dim_sales_agent(): _truncate_table("dimsalesagent", cascade=True)
    @task(task_id='truncate_dim_deal_stage')
    def python_truncate_dim_deal_stage(): _truncate_table("dimdealstage", cascade=True)
    @task(task_id='truncate_dim_date')
    def python_truncate_dim_date(): _truncate_table("dimdate", cascade=True)

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
    @task
    def load_dim_date():
        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID); hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        sql_get_range = "SELECT MIN(engage_date), MAX(COALESCE(close_date, engage_date)) FROM opportunities;"
        logging.info("Querying OLTP for date range..."); result = hook_oltp.get_first(sql_get_range)
        min_date_str = DEFAULT_MIN_DATE; max_date_str = DEFAULT_MAX_DATE
        try:
            if result and result[0] and result[1]: min_dt, max_dt = result[0], result[1]; min_date_str = pendulum.instance(min_dt).to_date_string(); max_date_str = pendulum.instance(max_dt).to_date_string(); logging.info(f"Determined date range: {min_date_str} to {max_date_str}")
            else: logging.warning("Could not determine date range. Using defaults.")
        except Exception as e: logging.error(f"Error processing date range: {e}. Using defaults.")
        min_year_start = pendulum.parse(min_date_str).start_of('year').to_date_string(); max_year_end = pendulum.parse(max_date_str).end_of('year').to_date_string()
        sql_insert_dates = f"""INSERT INTO {OLAP_SCHEMA}.dimdate (datekey, date, day, month, quarter, year) SELECT TO_CHAR(datum, 'YYYYMMDD')::INT, datum, EXTRACT(DAY FROM datum), EXTRACT(MONTH FROM datum), EXTRACT(QUARTER FROM datum), EXTRACT(YEAR FROM datum) FROM generate_series('{min_year_start}'::DATE, '{max_year_end}'::DATE, '1 day'::INTERVAL) datum;"""
        logging.info("Populating dimdate..."); hook_olap.run(sql_insert_dates); logging.info("dimdate populated.")
    @task
    def load_dim_deal_stage():
        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID); hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        stages = hook_oltp.get_records("SELECT stage_id, name FROM deal_stages")
        if stages:
            unique_stages = {sid: name for sid, name in stages}; stages_to_load = list(unique_stages.items())
            try:
                with hook_olap.get_conn() as conn:
                    with conn.cursor() as cur:
                        for sid, name in stages_to_load:
                            cur.execute(f"""INSERT INTO {OLAP_SCHEMA}.dimdealstage (stageid, stagename) VALUES (%s, %s) ON CONFLICT (stageid) DO UPDATE SET stagename = EXCLUDED.stagename;""", (sid, name))
                    conn.commit()
                logging.info(f"Upserted {len(stages_to_load)} rows into dimdealstage.")
            except Exception as e: logging.error(f"Failed to load dimdealstage: {e}"); raise
        else: logging.info("No stages found in OLTP.")
    @task
    def load_dim_account(): # SCD1 - Using UPSERT
        variable_name = "initial_load_dim_account_offset"; batch_size = DEFAULT_BATCH_SIZE; total_rows_processed = 0
        while True:
            current_offset = get_batch_state(variable_name, 0); hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            sql_extract = """SELECT p.project_id, p.name, s.name AS sector, 
                CASE WHEN p.revenue < 1000000 THEN 'Under 1M' 
                     WHEN p.revenue BETWEEN 1000000 AND 10000000 THEN '1M-10M' 
                     WHEN p.revenue > 10000000 THEN 'Over 10M' ELSE 'Unknown' END AS revenuerange,
                p.parent_project_id 
              FROM projects p 
              LEFT JOIN sectors s ON p.sector_id = s.sector_id 
              ORDER BY p.project_id LIMIT %s OFFSET %s;"""
            source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))
            if not source_data: break
            olap_data = [(row[0], row[1], row[2], row[3], row[4]) for row in source_data]
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            try:
                with hook_olap.get_conn() as conn:
                    with conn.cursor() as cur:
                        for record in olap_data: cur.execute(f"""INSERT INTO {OLAP_SCHEMA}.dimaccount (accountid, accountname, sector, revenuerange, parentaccountid) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (accountid) DO UPDATE SET accountname = EXCLUDED.accountname, sector = EXCLUDED.sector, revenuerange = EXCLUDED.revenuerange, parentaccountid = EXCLUDED.parentaccountid;""", record)
                    conn.commit()
                rows_loaded = len(olap_data); total_rows_processed += rows_loaded; set_batch_state(variable_name, current_offset + rows_loaded);
            except Exception as e: logging.error(f"Failed DimAccount batch {current_offset}: {e}"); raise AirflowSkipException() from e
            if len(source_data) < batch_size: break
        logging.info(f"Finished loading dimaccount (SCD1 - Upsert). Total: {total_rows_processed}")
    @task
    def load_dim_product(): # SCD2
        variable_name = "initial_load_dim_product_offset"; batch_size = DEFAULT_BATCH_SIZE; total_rows_processed = 0
        initial_valid_from = DEFAULT_SCD2_VALID_FROM; logging.info(f"Initial load dimproduct (SCD2), ValidFrom={initial_valid_from}")
        while True:
            current_offset = get_batch_state(variable_name, 0); hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            sql_extract = """SELECT p.product_id, p.name, ps.name AS seriesname, p.sales_price,
                CASE WHEN p.sales_price < 100 THEN 'Low' 
                     WHEN p.sales_price BETWEEN 100 AND 1000 THEN 'Medium' 
                     WHEN p.sales_price > 1000 THEN 'High' ELSE 'Unknown' END AS pricerange
              FROM products p 
              LEFT JOIN product_series ps ON p.series_id = ps.series_id 
              ORDER BY p.product_id LIMIT %s OFFSET %s;"""
            source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))
            if not source_data: break
            olap_data = [(row[0], row[1], row[2], row[3], row[4], initial_valid_from, None, True) for row in source_data]; target_fields = ["productid", "productname", "seriesname", "price", "pricerange", "validfrom", "validto", "iscurrent"]
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            try: hook_olap.insert_rows(table=f"{OLAP_SCHEMA}.dimproduct", rows=olap_data, target_fields=target_fields, commit_every=batch_size); rows_loaded = len(olap_data); total_rows_processed += rows_loaded; set_batch_state(variable_name, current_offset + rows_loaded);
            except Exception as e: logging.error(f"Failed dimproduct batch {current_offset}: {e}"); raise AirflowSkipException() from e
            if len(source_data) < batch_size: break
        logging.info(f"Finished loading dimproduct (SCD2). Total: {total_rows_processed}")
    @task
    def load_dim_sales_agent(): # SCD1 - Using UPSERT
        variable_name = "initial_load_dim_sales_agent_offset"; batch_size = DEFAULT_BATCH_SIZE; total_rows_processed = 0
        while True:
            current_offset = get_batch_state(variable_name, 0); hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            sql_extract = """SELECT sa.agent_id, sa.name AS salesagentname, sm.name AS managername, l.name AS region
                FROM sales_agents sa 
                LEFT JOIN sales_managers sm ON sa.manager_id = sm.manager_id 
                LEFT JOIN locations l ON sa.regional_office = l.location_id 
                ORDER BY sa.agent_id LIMIT %s OFFSET %s;"""
            source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))
            if not source_data: break
            olap_data = [(row[0], row[1], row[2], row[3]) for row in source_data]
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            try:
                with hook_olap.get_conn() as conn:
                    with conn.cursor() as cur:
                        for record in olap_data: cur.execute(f"""INSERT INTO {OLAP_SCHEMA}.dimsalesagent (salesagentid, salesagentname, managername, region) VALUES (%s, %s, %s, %s) ON CONFLICT (salesagentid) DO UPDATE SET salesagentname = EXCLUDED.salesagentname, managername = EXCLUDED.managername, region = EXCLUDED.region;""", record)
                    conn.commit()
                rows_loaded = len(olap_data); total_rows_processed += rows_loaded; set_batch_state(variable_name, current_offset + rows_loaded);
            except Exception as e: logging.error(f"Failed dimsalesagent batch {current_offset}: {e}"); raise AirflowSkipException() from e
            if len(source_data) < batch_size: break
        logging.info(f"Finished loading dimsalesagent (SCD1 - Upsert). Total: {total_rows_processed}")

    # --- Task: Fact Table - SalesPerformance (Initial Load) ---
    @task
    def load_fact_sales_performance():
        """Loads factsalesperformance using INSERT ON CONFLICT DO NOTHING."""
        variable_name = "initial_load_fact_sales_perf_offset"; batch_size = DEFAULT_BATCH_SIZE; total_rows_processed = 0
        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID); hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        while True:
            current_offset = get_batch_state(variable_name, 0)
            sql_extract = """SELECT sp.opportunity_id, sp.agent_id, sp.product_id, sp.project_id, sp.stage_id,
                sp.engage_date, sp.close_date, sp.close_value,
                CASE WHEN sp.close_date IS NOT NULL AND sp.engage_date IS NOT NULL THEN sp.close_date - sp.engage_date ELSE NULL END AS durationdays,
                CASE ds.name WHEN 'Won' THEN 100.0 WHEN 'Lost' THEN 0.0 WHEN 'Engaging' THEN 75.0 WHEN 'Prospecting' THEN 25.0 ELSE 10.0 END AS expectedsuccessrate,
                COALESCE(sp.close_date, sp.engage_date) AS event_date
              FROM opportunities sp 
              LEFT JOIN deal_stages ds ON sp.stage_id = ds.stage_id
              WHERE sp.project_id IS NOT NULL AND sp.product_id IS NOT NULL AND sp.agent_id IS NOT NULL 
                AND sp.stage_id IS NOT NULL AND COALESCE(sp.close_date, sp.engage_date) IS NOT NULL
              ORDER BY sp.opportunity_id LIMIT %s OFFSET %s;"""
            try: source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))
            except Exception as e: logging.error(f"Failed fetch SalesPipeline offset {current_offset}: {e}"); raise AirflowSkipException() from e
            if not source_data: break
            olap_data_to_insert = []; missing_keys_count = 0; processed_in_batch = 0
            for row in source_data:
                try:
                    opportunity_id, oltp_sales_agent_id, oltp_product_id, oltp_account_id, oltp_deal_stage_id, _, _, close_value, duration_days, expected_success_rate, event_date = row
                    if isinstance(event_date, (datetime, date)): event_date_str = event_date.strftime('%Y-%m-%d')
                    else: event_date_str = pendulum.parse(str(event_date)).to_date_string()
                    date_key_res = hook_olap.get_first(f'SELECT datekey FROM {OLAP_SCHEMA}.dimdate WHERE date = %s', parameters=(event_date_str,))
                    account_key_res = hook_olap.get_first(f'SELECT accountid FROM {OLAP_SCHEMA}.dimaccount WHERE accountid = %s', parameters=(oltp_account_id,))
                    product_sk_res = hook_olap.get_first(f'SELECT productsk FROM {OLAP_SCHEMA}.dimproduct WHERE productid = %s AND iscurrent = TRUE', parameters=(oltp_product_id,))
                    agent_key_res = hook_olap.get_first(f'SELECT salesagentid FROM {OLAP_SCHEMA}.dimsalesagent WHERE salesagentid = %s', parameters=(oltp_sales_agent_id,))
                    stage_key_res = hook_olap.get_first(f'SELECT stageid FROM {OLAP_SCHEMA}.dimdealstage WHERE stageid = %s', parameters=(oltp_deal_stage_id,))
                    date_key = date_key_res[0] if date_key_res else None; account_key = account_key_res[0] if account_key_res else None
                    product_key = product_sk_res[0] if product_sk_res else None # This is ProductSK
                    agent_key = agent_key_res[0] if agent_key_res else None; stage_key = stage_key_res[0] if stage_key_res else None
                    if all([date_key, account_key, product_key, agent_key, stage_key]):
                         olap_data_to_insert.append((opportunity_id, date_key, account_key, product_key, agent_key, stage_key, close_value, duration_days, expected_success_rate))
                    else: missing_keys_count += 1; # logging.warning(f"Initial Load: Skip OppID {opportunity_id} missing key for Event {event_date_str}.") # Too verbose
                except Exception as lookup_ex: missing_keys_count += 1; logging.error(f"Initial Load: Dim lookup error OppID {row[0]}: {lookup_ex}")
            if olap_data_to_insert:
                try:
                    # ** MODIFIED: Use INSERT ON CONFLICT DO NOTHING row by row **
                    with hook_olap.get_conn() as conn:
                        with conn.cursor() as cur:
                            insert_sql = f"""
                            INSERT INTO {OLAP_SCHEMA}.factsalesperformance
                            (opportunityid, datekey, accountkey, productkey, salesagentkey, dealstagekey, closevalue, durationdays, expectedsuccessrate)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (opportunityid) DO NOTHING;
                            """
                            # Execute one by one
                            for record in olap_data_to_insert:
                                cur.execute(insert_sql, record)
                                processed_in_batch += 1 # Count successful attempts
                        conn.commit()
                    total_rows_processed += processed_in_batch
                    set_batch_state(variable_name, current_offset + len(source_data)) # Advance offset by rows read
                    logging.info(f"Processed batch offset {current_offset}. Attempted inserts: {len(olap_data_to_insert)}. Actual inserts/ignored: {processed_in_batch}. Total inserted so far: {total_rows_processed}")
                except Exception as e: logging.error(f"Failed fact insert batch {current_offset}: {e}"); raise AirflowSkipException() from e
            else: set_batch_state(variable_name, current_offset + len(source_data)); logging.warning(f"No valid fact data in batch {current_offset}")
            if len(source_data) < batch_size: break
        logging.info(f"Finished loading factsalesperformance. Total rows processed/inserted: {total_rows_processed}. Skipped rows due to missing keys: {missing_keys_count}")

    # --- Task: Reset Incremental Timestamp Watermarks ---
    @task
    def reset_incremental_watermarks():
        """Resets timestamp watermarks used by the incremental DAG to the default start time."""
        incremental_tables = ['projects', 'products', 'sales_agents', 'opportunities']
        logging.info("Resetting timestamp watermarks..."); [Variable.set(f"{WATERMARK_VAR_PREFIX}{t}", DEFAULT_START_TIME) for t in incremental_tables]; logging.info("Watermarks reset.")

    # --- Instantiate Tasks & Define Flow ---
    task_load_date=load_dim_date(); task_load_stage=load_dim_deal_stage(); task_load_account=load_dim_account(); task_load_product=load_dim_product(); task_load_agent=load_dim_sales_agent(); task_load_facts=load_fact_sales_performance(); task_reset_watermarks=reset_incremental_watermarks()
    load_dims_tasks = [task_load_date, task_load_stage, task_load_account, task_load_product, task_load_agent]
    start >> task_reset_states >> truncate_facts; cross_downstream(truncate_facts, truncate_dims); cross_downstream(truncate_dims, load_dims_tasks); load_dims_tasks >> task_load_facts >> task_reset_watermarks >> end

b2b_initial_load_dag()
