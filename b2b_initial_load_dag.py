# /path/to/your/airflow/dags/b2b_initial_load_dag.py
import pendulum
import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
# Removed PostgresOperator import as it's no longer used for truncate
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import cross_downstream
from airflow.operators.empty import EmptyOperator

# --- Configuration ---
OLTP_CONN_ID = "b2b_sales"
OLAP_CONN_ID = "b2b_sales_olap"
DEFAULT_BATCH_SIZE = 1000  # Limit rows per transaction/command as requested

# --- Helper Functions ---
# Batch state functions remain useful if a single run is very long and fails mid-way,
# allowing potential manual continuation, although truncate makes full reruns clean.
def get_batch_state(variable_name, default_value=0):
    """Gets the starting offset for the next batch."""
    try:
        # Check if variable exists before getting
        if Variable.get(variable_name, default_var=None) is None:
             logging.info(f"Variable {variable_name} not found, starting from {default_value}")
             return default_value
        return int(Variable.get(variable_name))
    except ValueError:
        logging.warning(f"Variable {variable_name} has non-integer value, using default {default_value}")
        return default_value
    except Exception as e:
        logging.error(f"Could not retrieve variable {variable_name}: {e}")
        logging.warning(f"Using default {default_value} due to error retrieving variable.")
        return default_value

def set_batch_state(variable_name, value):
    """Sets the starting offset for the next batch."""
    try:
        Variable.set(variable_name, str(value))
    except Exception as e:
        logging.error(f"Could not set variable {variable_name}: {e}")

def reset_batch_state(variable_name):
     """Deletes the variable at the start of a full load."""
     try:
         Variable.delete(variable_name)
         logging.info(f"Reset batch state variable {variable_name}.")
     except Exception as e:
         logging.warning(f"Could not delete variable {variable_name} (may not exist): {e}")

# --- DAG Definition ---
@dag(
    dag_id='b2b_initial_load',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)},
    tags=['b2b_sales', 'initial_load', 'repopulatable'],
    doc_md="""
    ### B2B Sales Initial Load DAG (Repopulatable)

    Performs the initial population of the OLAP database from the OLTP source.
    **This version TRUNCATES target tables first using PythonOperators and CASCADE, allowing it to be re-run.**

    **WARNING:** Running this DAG will WIPE existing data in the target OLAP tables
    and potentially cascade to other tables if FKs are complex.

    **Tasks:**
    0.  Start.
    1.  Reset Batch State Variables.
    2.  Truncate OLAP Fact Tables (using CASCADE via PythonOperator).
    3.  Truncate OLAP Dimension Tables (using CASCADE via PythonOperator).
    4.  Load DimDate (pre-populate or generate).
    5.  Load DimDealStage.
    6.  Load DimAccount (batched).
    7.  Load DimProduct (batched, sets initial validity).
    8.  Load DimSalesAgent (batched).
    9.  Load FactSalesPerformance (batched).
    10. End.

    **Recovery:** Uses Airflow Variables (`initial_load_[table]_offset`)
    to track batch progress within a single run. These are reset at the start.
    """
)
def b2b_initial_load_dag():

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # --- Task Group: Reset Batch State Variables ---
    @task
    def reset_all_batch_states():
        state_vars = [
            "initial_load_dim_account_offset",
            "initial_load_dim_product_offset",
            "initial_load_dim_sales_agent_offset",
            "initial_load_fact_sales_perf_offset"
        ]
        for var_name in state_vars:
            reset_batch_state(var_name)

    task_reset_states = reset_all_batch_states()

    # --- Task Group: Truncate Tables using PythonOperator ---

    def _truncate_table(table_name: str, cascade: bool = False):
        """Helper Python function to truncate a table."""
        hook = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        sql = f"TRUNCATE TABLE {table_name}"
        if cascade:
            sql += " CASCADE"
        sql += ";"
        logging.info(f"Running TRUNCATE command: {sql}")
        hook.run(sql)
        logging.info(f"Successfully truncated table {table_name}.")

    # Truncate Fact Tables (depend on Dimensions) - Using CASCADE
    # WARNING: Using CASCADE can have unintended consequences if FKs are complex.
    @task(task_id='truncate_fact_sales_performance')
    def python_truncate_fact_sales_performance():
        _truncate_table("FactSalesPerformance", cascade=True)

    @task(task_id='truncate_fact_sales_monthly_aggregate')
    def python_truncate_fact_monthly_aggregate():
        _truncate_table("FactSalesMonthlyAggregate", cascade=True) # Assuming this might also be referenced

    # Truncate Dimension Tables (Referenced by Facts) - Using CASCADE now
    # WARNING: Using CASCADE here is necessary due to FK constraints but ensure
    # you understand which rows in referencing tables (like facts) will be deleted.
    @task(task_id='truncate_dim_account')
    def python_truncate_dim_account():
        _truncate_table("DimAccount", cascade=True) # Added CASCADE

    @task(task_id='truncate_dim_product')
    def python_truncate_dim_product():
        _truncate_table("DimProduct", cascade=True) # Added CASCADE

    @task(task_id='truncate_dim_sales_agent')
    def python_truncate_dim_sales_agent():
        _truncate_table("DimSalesAgent", cascade=True) # Added CASCADE

    @task(task_id='truncate_dim_deal_stage')
    def python_truncate_dim_deal_stage():
        _truncate_table("DimDealStage", cascade=True) # Added CASCADE

    @task(task_id='truncate_dim_date')
    def python_truncate_dim_date():
        _truncate_table("DimDate", cascade=True) # Added CASCADE

    # Instantiate Truncate Tasks
    truncate_fact_sales_perf_task = python_truncate_fact_sales_performance()
    truncate_fact_monthly_agg_task = python_truncate_fact_monthly_aggregate()
    truncate_dim_account_task = python_truncate_dim_account()
    truncate_dim_product_task = python_truncate_dim_product()
    truncate_dim_sales_agent_task = python_truncate_dim_sales_agent()
    truncate_dim_deal_stage_task = python_truncate_dim_deal_stage()
    truncate_dim_date_task = python_truncate_dim_date()

    # Group Truncate Tasks for dependency setting
    truncate_facts = [truncate_fact_sales_perf_task, truncate_fact_monthly_agg_task]
    truncate_dims = [
        truncate_dim_account_task,
        truncate_dim_product_task,
        truncate_dim_sales_agent_task,
        truncate_dim_deal_stage_task,
        truncate_dim_date_task
    ]

    # --- Task: Dimension - Date (Example: Generate if needed) ---
    @task
    def load_dim_date():
        # This task now runs *after* truncate_dim_date_task
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        sql = """
        INSERT INTO DimDate (DateKey, Date, Day, Month, Quarter, Year)
        SELECT
            TO_CHAR(datum, 'YYYYMMDD')::INT AS DateKey,
            datum AS Date,
            EXTRACT(DAY FROM datum) AS Day,
            EXTRACT(MONTH FROM datum) AS Month,
            EXTRACT(QUARTER FROM datum) AS Quarter,
            EXTRACT(YEAR FROM datum) AS Year
        FROM generate_series(
            '2020-01-01'::DATE,
            '2025-12-31'::DATE,
            '1 day'::INTERVAL
        ) datum;
        """
        hook_olap.run(sql)
        logging.info("DimDate populated.")

    # --- Task: Dimension - DealStage ---
    @task
    def load_dim_deal_stage():
        # This task now runs *after* truncate_dim_deal_stage_task
        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        stages = hook_oltp.get_records("SELECT DealStageID, StageName FROM DealStages")
        if stages:
            hook_olap.insert_rows(
                table="DimDealStage",
                rows=stages,
                target_fields=["StageID", "StageName"],
                commit_every=DEFAULT_BATCH_SIZE
            )
            logging.info(f"Loaded {len(stages)} rows into DimDealStage.")
        else:
            logging.info("No stages found in OLTP.")

    # --- Task: Dimension - Account (Batched) ---
    @task
    def load_dim_account():
        # This task now runs *after* truncate_dim_account_task
        variable_name = "initial_load_dim_account_offset"
        batch_size = DEFAULT_BATCH_SIZE
        total_rows_processed = 0

        while True:
            current_offset = get_batch_state(variable_name, 0)
            logging.info(f"Processing DimAccount batch starting from offset {current_offset}")

            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            sql_extract = """
            SELECT
                a.AccountID, a.AccountName, s.SectorName AS Sector,
                CASE
                    WHEN a.Revenue < 1000000 THEN 'Under 1M'
                    WHEN a.Revenue BETWEEN 1000000 AND 10000000 THEN '1M-10M'
                    WHEN a.Revenue > 10000000 THEN 'Over 10M' ELSE 'Unknown'
                END AS RevenueRange,
                a.ParentAccountID
            FROM Accounts a
            LEFT JOIN Sectors s ON a.SectorID = s.SectorID
            ORDER BY a.AccountID LIMIT %s OFFSET %s;
            """
            source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))

            if not source_data:
                logging.info("No more accounts to process.")
                break

            olap_data = [(row[0], row[1], row[2], row[3], row[4]) for row in source_data]
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            try:
                 hook_olap.insert_rows(
                     table="DimAccount",
                     rows=olap_data,
                     target_fields=["AccountID", "AccountName", "Sector", "RevenueRange", "ParentAccountID"],
                     commit_every=batch_size
                 )
                 rows_loaded = len(olap_data)
                 logging.info(f"Loaded batch of {rows_loaded} accounts.")
                 total_rows_processed += rows_loaded
                 set_batch_state(variable_name, current_offset + rows_loaded)
                 if len(source_data) < batch_size:
                    logging.info("Last batch processed.")
                    break
            except Exception as e:
                logging.error(f"Failed processing batch at offset {current_offset}: {e}")
                raise AirflowSkipException(f"Batch failed at offset {current_offset}, see logs.") from e

        logging.info(f"Finished loading DimAccount. Total rows processed: {total_rows_processed}")

    # --- Task: Dimension - Product (Batched, Initial Validity) ---
    @task
    def load_dim_product():
        # This task now runs *after* truncate_dim_product_task
        variable_name = "initial_load_dim_product_offset"
        batch_size = DEFAULT_BATCH_SIZE
        total_rows_processed = 0
        current_run_date = pendulum.now().to_date_string()

        while True:
            current_offset = get_batch_state(variable_name, 0)
            logging.info(f"Processing DimProduct batch starting from offset {current_offset}")

            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
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
            ORDER BY p.ProductID LIMIT %s OFFSET %s;
            """
            source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))

            if not source_data:
                logging.info("No more products to process.")
                break

            olap_data = [
                (row[0], row[1], row[2], row[3], row[4], current_run_date, None, True)
                for row in source_data
            ]
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            try:
                 hook_olap.insert_rows(
                     table="DimProduct",
                     rows=olap_data,
                     target_fields=["ProductID", "ProductName", "SeriesName", "Price", "PriceRange", "ValidFrom", "ValidTo", "IsCurrent"],
                     commit_every=batch_size
                 )
                 rows_loaded = len(olap_data)
                 logging.info(f"Loaded batch of {rows_loaded} products.")
                 total_rows_processed += rows_loaded
                 set_batch_state(variable_name, current_offset + rows_loaded)
                 if len(source_data) < batch_size:
                    logging.info("Last batch processed.")
                    break
            except Exception as e:
                logging.error(f"Failed processing batch at offset {current_offset}: {e}")
                raise AirflowSkipException(f"Batch failed at offset {current_offset}, see logs.") from e

        logging.info(f"Finished loading DimProduct. Total rows processed: {total_rows_processed}")

    # --- Task: Dimension - SalesAgent (Batched) ---
    @task
    def load_dim_sales_agent():
        # This task now runs *after* truncate_dim_sales_agent_task
        variable_name = "initial_load_dim_sales_agent_offset"
        batch_size = DEFAULT_BATCH_SIZE
        total_rows_processed = 0

        while True:
            current_offset = get_batch_state(variable_name, 0)
            logging.info(f"Processing DimSalesAgent batch starting from offset {current_offset}")

            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            sql_extract = """
             SELECT
                 sa.SalesAgentID, sa.SalesAgentName, sm.ManagerName, l.LocationName AS Region
             FROM SalesAgents sa
             LEFT JOIN SalesManagers sm ON sa.ManagerID = sm.ManagerID
             LEFT JOIN Locations l ON sa.RegionalOfficeID = l.LocationID
             ORDER BY sa.SalesAgentID LIMIT %s OFFSET %s;
             """
            source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))

            if not source_data:
                logging.info("No more sales agents to process.")
                break

            olap_data = [(row[0], row[1], row[2], row[3]) for row in source_data]
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            try:
                hook_olap.insert_rows(
                    table="DimSalesAgent",
                    rows=olap_data,
                    target_fields=["SalesAgentID", "SalesAgentName", "ManagerName", "Region"],
                    commit_every=batch_size
                )
                rows_loaded = len(olap_data)
                logging.info(f"Loaded batch of {rows_loaded} sales agents.")
                total_rows_processed += rows_loaded
                set_batch_state(variable_name, current_offset + rows_loaded)
                if len(source_data) < batch_size:
                    logging.info("Last batch processed.")
                    break
            except Exception as e:
                logging.error(f"Failed processing batch at offset {current_offset}: {e}")
                raise AirflowSkipException(f"Batch failed at offset {current_offset}, see logs.") from e

        logging.info(f"Finished loading DimSalesAgent. Total rows processed: {total_rows_processed}")

    # --- Task: Fact Table - SalesPerformance (Batched) ---
    @task
    def load_fact_sales_performance():
        # This task now runs *after* truncate_fact_sales_perf_task and after dims are loaded
        variable_name = "initial_load_fact_sales_perf_offset"
        batch_size = DEFAULT_BATCH_SIZE
        total_rows_processed = 0

        while True:
            current_offset = get_batch_state(variable_name, 0)
            logging.info(f"Processing FactSalesPerformance batch starting from offset {current_offset}")

            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
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
                TO_CHAR(COALESCE(sp.CloseDate, sp.EngageDate, CURRENT_DATE), 'YYYYMMDD')::INT AS DateKey
            FROM SalesPipeline sp
            LEFT JOIN DealStages ds ON sp.DealStageID = ds.DealStageID
            ORDER BY sp.OpportunityID LIMIT %s OFFSET %s;
            """
            source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))

            if not source_data:
                logging.info("No more sales pipeline data to process.")
                break

            olap_data = [
                (row[10], row[3], row[2], row[1], row[4], row[7], row[8], row[9])
                for row in source_data
            ]
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            try:
                hook_olap.insert_rows(
                    table="FactSalesPerformance",
                    rows=olap_data,
                    target_fields=[
                        "DateKey", "AccountKey", "ProductKey", "SalesAgentKey",
                        "DealStageKey", "CloseValue", "DurationDays", "ExpectedSuccessRate"
                    ],
                    commit_every=batch_size
                )
                rows_loaded = len(olap_data)
                logging.info(f"Loaded batch of {rows_loaded} fact records.")
                total_rows_processed += rows_loaded
                set_batch_state(variable_name, current_offset + rows_loaded)
                if len(source_data) < batch_size:
                    logging.info("Last fact batch processed.")
                    break
            except Exception as e:
                logging.error(f"Failed processing fact batch at offset {current_offset}: {e}")
                logging.error(f"Failed data sample (first record): {olap_data[0] if olap_data else 'N/A'}")
                raise AirflowSkipException(f"Fact Batch failed at offset {current_offset}, see logs.") from e

        logging.info(f"Finished loading FactSalesPerformance. Total rows processed: {total_rows_processed}")

    # --- Instantiate Load Tasks ---
    task_load_date = load_dim_date()
    task_load_stage = load_dim_deal_stage()
    task_load_account = load_dim_account()
    task_load_product = load_dim_product()
    task_load_agent = load_dim_sales_agent()
    task_load_facts = load_fact_sales_performance()

    load_dims_tasks = [
        task_load_date, task_load_stage, task_load_account,
        task_load_product, task_load_agent
    ]

    # --- Define Task Dependencies ---
    start >> task_reset_states # Reset offsets first

    # Truncate facts first (due to FKs from facts to dims), then truncate dims
    # Using CASCADE on fact tables simplifies this, otherwise truncate facts then dims.
    # Set dependency from single task to list
    task_reset_states >> truncate_facts

    # Set dependency from list of tasks to list of tasks using cross_downstream
    cross_downstream(truncate_facts, truncate_dims)

    # Load dimensions after truncation. They can run in parallel.
    # Set dependency from list of tasks to list of tasks using cross_downstream
    cross_downstream(truncate_dims, load_dims_tasks)

    # Load facts only after all dimensions are loaded
    # Set dependency from list of tasks to single task
    load_dims_tasks >> task_load_facts

    task_load_facts >> end

# Instantiate the DAG
b2b_initial_load_dag()
