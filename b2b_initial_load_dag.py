# /path/to/your/airflow/dags/b2b_initial_load_dag.py
import pendulum
import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import cross_downstream # Import added

# --- Configuration ---
OLTP_CONN_ID = "b2b_sales"
OLAP_CONN_ID = "b2b_sales_olap"
DEFAULT_BATCH_SIZE = 1000  # Limit rows per transaction/command as requested

# --- Helper Functions ---
def get_batch_state(variable_name, default_value=0):
    """Gets the starting offset for the next batch."""
    try:
        return int(Variable.get(variable_name, default_var=default_value))
    except ValueError:
        logging.warning(f"Variable {variable_name} not found or not an integer, using default {default_value}")
        return default_value
    except Exception as e:
        # Handle cases where Variable backend might be unavailable temporarily
        logging.error(f"Could not retrieve variable {variable_name}: {e}")
        # Depending on requirements, either fail or use default
        logging.warning(f"Using default {default_value} due to error retrieving variable.")
        return default_value


def set_batch_state(variable_name, value):
    """Sets the starting offset for the next batch."""
    try:
        Variable.set(variable_name, str(value))
    except Exception as e:
         # Log error but potentially allow DAG to continue if variable setting isn't critical path
         # Or re-raise depending on desired fault tolerance
        logging.error(f"Could not set variable {variable_name}: {e}")


def execute_load_batch(target_conn_id, sql, data):
    """Executes batch insert/upsert."""
    if not data:
        logging.info("No data to load in this batch.")
        return 0  # Return 0 rows loaded

    hook = PostgresHook(postgres_conn_id=target_conn_id)
    # Use execute for INSERT/UPDATE/DELETE or insert_rows for bulk inserts
    # Using execute allows for ON CONFLICT clauses easily
    # Note: Real batching might require psycopg2 executemany or copy_expert
    # For simplicity, we'll execute row by row in a single transaction here
    # but be aware this isn't optimal for *large* batches.
    # A better approach for bulk inserts is hook.insert_rows() or copy_expert

    rows_affected = 0
    conn = None # Initialize conn outside try
    try:
        # Example: hook.insert_rows(table='your_table', rows=data)
        # Using generic execute for flexibility with potential ON CONFLICT
        conn = hook.get_conn()
        cur = conn.cursor()
        for row in data:
             cur.execute(sql, row) # Pass row as parameters
             rows_affected += cur.rowcount # Note: rowcount might not be reliable for all statements/DBs
        conn.commit()
        cur.close()
        logging.info(f"Successfully loaded batch. Rows affected (approx): {rows_affected}")
        # Return len(data) as a more reliable indicator of processed batch items
        return len(data)
    except Exception as e:
        logging.error(f"Error loading batch: {e}")
        if conn: # Rollback if connection was established
            conn.rollback()
        raise # Re-raise the exception to fail the task
    finally:
        if conn: # Ensure connection is closed
             conn.close()


# --- DAG Definition ---
@dag(
    dag_id='b2b_initial_load',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)},
    tags=['b2b_sales', 'initial_load'],
    doc_md="""
    ### B2B Sales Initial Load DAG

    Performs the initial population of the OLAP database from the OLTP source.
    Handles data in batches to respect transaction limits and allows resuming.

    **Tasks:**
    1.  Load DimDate (pre-populate or generate).
    2.  Load DimDealStage.
    3.  Load DimAccount (batched).
    4.  Load DimProduct (batched, sets initial validity).
    5.  Load DimSalesAgent (batched).
    6.  Load FactSalesPerformance (batched).

    **Recovery:** Uses Airflow Variables (`initial_load_[table]_offset`)
    to track progress. If a task fails and restarts, it continues from the last
    successful batch offset. Clear these variables before the first run.
    """
)
def b2b_initial_load_dag():

    # --- Task 1: Dimension - Date (Example: Generate if needed) ---
    @task
    def load_dim_date():
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        # Simple example: ensure 2020-2025 exists. Adapt range as needed.
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
        ) datum
        ON CONFLICT (DateKey) DO NOTHING;
        """
        hook_olap.run(sql)
        logging.info("DimDate populated.")

    # --- Task 2: Dimension - DealStage ---
    @task
    def load_dim_deal_stage():
        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        stages = hook_oltp.get_records("SELECT DealStageID, StageName FROM DealStages")
        if stages:
            # Using insert_rows for efficiency
            hook_olap.insert_rows(
                table="DimDealStage",
                rows=stages,
                target_fields=["StageID", "StageName"],
                commit_every=DEFAULT_BATCH_SIZE # insert_rows handles batching
            )
            logging.info(f"Loaded {len(stages)} rows into DimDealStage.")
        else:
            logging.info("No stages found in OLTP.")

    # --- Task 3: Dimension - Account (Batched) ---
    @task
    def load_dim_account():
        variable_name = "initial_load_dim_account_offset"
        batch_size = DEFAULT_BATCH_SIZE
        total_rows_processed = 0

        while True:
            current_offset = get_batch_state(variable_name, 0)
            logging.info(f"Processing DimAccount batch starting from offset {current_offset}")

            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            sql_extract = """
            SELECT
                a.AccountID,
                a.AccountName,
                s.SectorName AS Sector,
                CASE -- Example Revenue Range Transformation
                    WHEN a.Revenue < 1000000 THEN 'Under 1M'
                    WHEN a.Revenue BETWEEN 1000000 AND 10000000 THEN '1M-10M'
                    WHEN a.Revenue > 10000000 THEN 'Over 10M'
                    ELSE 'Unknown'
                END AS RevenueRange,
                a.ParentAccountID -- Assumes ParentAccountID refers to AccountID in OLTP/OLAP
            FROM Accounts a
            LEFT JOIN Sectors s ON a.SectorID = s.SectorID
            ORDER BY a.AccountID -- Consistent ordering is vital for OFFSET
            LIMIT %s OFFSET %s;
            """
            source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))

            if not source_data:
                logging.info("No more accounts to process.")
                break # Exit loop

            # Prepare data for OLAP (matching DimAccount columns)
            olap_data = [
                (row[0], row[1], row[2], row[3], row[4]) for row in source_data
            ]

            # Load into OLAP
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            try:
                 # Using insert_rows is generally better for pure inserts
                 # It handles conflicts based on primary/unique keys if replace=True
                 # For initial load, ON CONFLICT might be safer if run multiple times
                 # Let's stick to insert_rows with replace=True for simplicity assuming
                 # we want the latest OLTP data if run again over existing data.
                 # If you need strict "insert only if not exists", use execute_load_batch with ON CONFLICT DO NOTHING
                 hook_olap.insert_rows(
                     table="DimAccount",
                     rows=olap_data,
                     target_fields=["AccountID", "AccountName", "Sector", "RevenueRange", "ParentAccountID"],
                     commit_every=batch_size,
                     replace=True, # Set to True to UPSERT based on PK
                     replace_index="AccountID" # Specify the primary key for conflict checking
                 )
                 rows_loaded = len(olap_data) # insert_rows doesn't return rows affected directly easily
                 logging.info(f"Loaded/Updated batch of {rows_loaded} accounts.")

                 total_rows_processed += rows_loaded
                 # Update offset based on rows processed in this batch
                 set_batch_state(variable_name, current_offset + rows_loaded)

                 # Optional: Check if fewer rows were returned than batch size asked for
                 if len(source_data) < batch_size:
                    logging.info("Last batch processed.")
                    break

            except Exception as e:
                logging.error(f"Failed processing batch at offset {current_offset}: {e}")
                # Do not update offset, let Airflow retry the task from the same offset
                raise AirflowSkipException(f"Batch failed at offset {current_offset}, see logs.") from e

        logging.info(f"Finished loading DimAccount. Total rows processed: {total_rows_processed}")
        # Optional: Reset offset variable at the end of successful full load
        # Variable.delete(variable_name)


    # --- Task 4: Dimension - Product (Batched, Initial Validity) ---
    @task
    def load_dim_product():
        variable_name = "initial_load_dim_product_offset"
        batch_size = DEFAULT_BATCH_SIZE
        total_rows_processed = 0
        current_run_date = pendulum.now().to_date_string() # Or use DAG run logical date

        while True:
            current_offset = get_batch_state(variable_name, 0)
            logging.info(f"Processing DimProduct batch starting from offset {current_offset}")

            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            sql_extract = """
            SELECT
                p.ProductID,
                p.ProductName,
                ps.SeriesName,
                p.SalesPrice,
                 CASE -- Example Price Range Transformation
                    WHEN p.SalesPrice < 100 THEN 'Low'
                    WHEN p.SalesPrice BETWEEN 100 AND 1000 THEN 'Medium'
                    WHEN p.SalesPrice > 1000 THEN 'High'
                    ELSE 'Unknown'
                END AS PriceRange
            FROM Products p
            LEFT JOIN ProductSeries ps ON p.SeriesID = ps.SeriesID
            ORDER BY p.ProductID
            LIMIT %s OFFSET %s;
            """
            source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))

            if not source_data:
                logging.info("No more products to process.")
                break

            # Prepare data for OLAP (matching DimProduct columns + SCD initial state)
            olap_data = [
                (
                    row[0], row[1], row[2], row[3], row[4],
                    current_run_date,  # ValidFrom - set to load date
                    None,              # ValidTo - NULL for current
                    True               # IsCurrent - TRUE
                 ) for row in source_data
            ]

            # Load into OLAP
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            try:
                 hook_olap.insert_rows(
                     table="DimProduct",
                     rows=olap_data,
                     target_fields=["ProductID", "ProductName", "SeriesName", "Price", "PriceRange", "ValidFrom", "ValidTo", "IsCurrent"],
                     commit_every=batch_size,
                     replace=True, # Use Upsert logic for initial load too
                     replace_index="ProductID"
                 )
                 rows_loaded = len(olap_data)
                 logging.info(f"Loaded/Updated batch of {rows_loaded} products.")

                 total_rows_processed += rows_loaded
                 set_batch_state(variable_name, current_offset + rows_loaded)

                 if len(source_data) < batch_size:
                    logging.info("Last batch processed.")
                    break

            except Exception as e:
                logging.error(f"Failed processing batch at offset {current_offset}: {e}")
                raise AirflowSkipException(f"Batch failed at offset {current_offset}, see logs.") from e

        logging.info(f"Finished loading DimProduct. Total rows processed: {total_rows_processed}")


    # --- Task 5: Dimension - SalesAgent (Batched) ---
    @task
    def load_dim_sales_agent():
        variable_name = "initial_load_dim_sales_agent_offset"
        batch_size = DEFAULT_BATCH_SIZE
        total_rows_processed = 0

        while True:
            current_offset = get_batch_state(variable_name, 0)
            logging.info(f"Processing DimSalesAgent batch starting from offset {current_offset}")

            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            sql_extract = """
             SELECT
                 sa.SalesAgentID,
                 sa.SalesAgentName,
                 sm.ManagerName,
                 l.LocationName AS Region -- Assuming RegionalOfficeID links to Locations for Region
             FROM SalesAgents sa
             LEFT JOIN SalesManagers sm ON sa.ManagerID = sm.ManagerID
             LEFT JOIN Locations l ON sa.RegionalOfficeID = l.LocationID
             ORDER BY sa.SalesAgentID
             LIMIT %s OFFSET %s;
             """
            source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))

            if not source_data:
                logging.info("No more sales agents to process.")
                break

            olap_data = [
                (row[0], row[1], row[2], row[3]) for row in source_data
            ]

            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            try:
                hook_olap.insert_rows(
                    table="DimSalesAgent",
                    rows=olap_data,
                    target_fields=["SalesAgentID", "SalesAgentName", "ManagerName", "Region"],
                    commit_every=batch_size,
                    replace=True, # Use Upsert logic
                    replace_index="SalesAgentID"
                )
                rows_loaded = len(olap_data)
                logging.info(f"Loaded/Updated batch of {rows_loaded} sales agents.")

                total_rows_processed += rows_loaded
                set_batch_state(variable_name, current_offset + rows_loaded)

                if len(source_data) < batch_size:
                    logging.info("Last batch processed.")
                    break

            except Exception as e:
                logging.error(f"Failed processing batch at offset {current_offset}: {e}")
                raise AirflowSkipException(f"Batch failed at offset {current_offset}, see logs.") from e

        logging.info(f"Finished loading DimSalesAgent. Total rows processed: {total_rows_processed}")

    # --- Task 6: Fact Table - SalesPerformance (Batched) ---
    @task
    def load_fact_sales_performance():
        variable_name = "initial_load_fact_sales_perf_offset"
        batch_size = DEFAULT_BATCH_SIZE
        total_rows_processed = 0

        while True:
            current_offset = get_batch_state(variable_name, 0)
            logging.info(f"Processing FactSalesPerformance batch starting from offset {current_offset}")

            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            # NOTE: Joining with OLTP dimensions here assumes initial load
            # happens sequentially. If dimensions could change *during* fact load,
            # lookups against OLAP dims would be safer.
            sql_extract = """
            SELECT
                sp.OpportunityID, -- Keep for potential reference/logging, not loaded directly to fact PK
                sp.SalesAgentID,
                sp.ProductID,
                sp.AccountID,
                sp.DealStageID,
                sp.EngageDate,
                sp.CloseDate,
                sp.CloseValue,
                -- Calculate DurationDays. Handle NULL dates.
                CASE
                    WHEN sp.CloseDate IS NOT NULL AND sp.EngageDate IS NOT NULL
                    THEN sp.CloseDate - sp.EngageDate
                    ELSE NULL
                END AS DurationDays,
                -- Placeholder for ExpectedSuccessRate - Needs logic or source field
                CASE ds.StageName -- Example logic based on stage
                   WHEN 'Won' THEN 100.0
                   WHEN 'Lost' THEN 0.0
                   WHEN 'Engaging' THEN 75.0
                   WHEN 'Prospecting' THEN 25.0
                   ELSE 10.0 -- Default for others
                END AS ExpectedSuccessRate,
                TO_CHAR(COALESCE(sp.CloseDate, sp.EngageDate, CURRENT_DATE), 'YYYYMMDD')::INT AS DateKey -- Use CloseDate, fallback EngageDate/Today
            FROM SalesPipeline sp
            LEFT JOIN DealStages ds ON sp.DealStageID = ds.DealStageID -- Needed for success rate example
            ORDER BY sp.OpportunityID -- Use a consistent, indexed column
            LIMIT %s OFFSET %s;
            """
            source_data = hook_oltp.get_records(sql_extract, parameters=(batch_size, current_offset))

            if not source_data:
                logging.info("No more sales pipeline data to process.")
                break

            # Prepare data for OLAP Fact table
            # Map source IDs directly to OLAP keys (assuming they match for initial load)
            # In incremental load, MUST lookup keys from OLAP dimensions.
            olap_data = [
                (
                    row[10], # DateKey
                    row[3],  # AccountKey (AccountID)
                    row[2],  # ProductKey (ProductID)
                    row[1],  # SalesAgentKey (SalesAgentID)
                    row[4],  # DealStageKey (DealStageID)
                    row[7],  # CloseValue
                    row[8],  # DurationDays
                    row[9]   # ExpectedSuccessRate
                ) for row in source_data
            ]

            # Load into OLAP Fact table (Append only for facts typically)
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
                # Basic error accumulation: Log the failing batch offset/details
                logging.error(f"Failed processing fact batch at offset {current_offset}: {e}")
                logging.error(f"Failed data sample (first record): {olap_data[0] if olap_data else 'N/A'}")
                # A more robust system might insert failed rows/keys into an error table
                raise AirflowSkipException(f"Fact Batch failed at offset {current_offset}, see logs.") from e


        logging.info(f"Finished loading FactSalesPerformance. Total rows processed: {total_rows_processed}")


    # --- Task Dependencies ---
    dim_date_task = load_dim_date()
    dim_stage_task = load_dim_deal_stage()
    dim_account_task = load_dim_account()
    dim_product_task = load_dim_product()
    dim_agent_task = load_dim_sales_agent()
    fact_sales_task = load_fact_sales_performance()

    # Dimensions can run in parallel after Date and Stage (simplest deps)
    # Use cross_downstream for list-to-list dependency
    cross_downstream([dim_date_task, dim_stage_task], [dim_account_task, dim_product_task, dim_agent_task])

    # Fact load depends on all dimensions being loaded (list-to-task dependency is fine)
    [dim_account_task, dim_product_task, dim_agent_task] >> fact_sales_task

# Instantiate the DAG
b2b_initial_load_dag()
