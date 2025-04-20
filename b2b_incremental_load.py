# /path/to/your/airflow/dags/b2b_incremental_load_dag.py
import pendulum
import logging
from datetime import timedelta, date

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

# --- Configuration ---
OLTP_CONN_ID = "b2b_sales"
OLAP_CONN_ID = "b2b_sales_olap"
# Using Airflow's data interval to track changes since last successful run
# Assumes OLTP tables have an 'updated_at' column tracking last modification time
# Use 'created_at' for tables that only have inserts.

# --- Helper: Get last successful run time ---
# This is a simplified way. Robust approach uses watermarking.
def get_last_success_time(**context):
    # Default to a very old date if no previous run exists or info unavailable
    default_start_time = '1970-01-01T00:00:00+00:00'
    last_success_date = context['prev_data_interval_end_success']
    if last_success_date:
        return last_success_date.isoformat()
    else:
        logging.warning("Could not determine last successful run time, using default.")
        # Fallback: try getting from Variable if manually set, or use default
        # return Variable.get("last_incremental_run_ts", default_var=default_start_time)
        return default_start_time


# --- DAG Definition ---
@dag(
    dag_id='b2b_incremental_load',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule='@daily', # Adjust as needed
    catchup=False, # Avoid backfilling unless intended
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},
    tags=['b2b_sales', 'incremental_load'],
    doc_md="""
    ### B2B Sales Incremental Load DAG

    Loads new and updated data from OLTP to OLAP since the last successful run.
    Uses `updated_at` timestamps for change detection (assumed to exist in OLTP).
    Handles Slowly Changing Dimensions (SCD Type '6-like' for DimProduct based on schema).

    **CDC Algorithm Description (Task 4):**
    - Reads the `data_interval_end` of the *previous successful* DAG run (`prev_data_interval_end_success`).
    - Queries OLTP tables for records where `updated_at` (or `created_at`) > `prev_data_interval_end_success`.
    - Processes these changed/new records.

    **Tasks:**
    1. Update DimAccount (SCD Type 1 - Upsert).
    2. Update DimProduct (SCD handling based on schema).
    3. Update DimSalesAgent (SCD Type 1 - Upsert).
    4. Load/Update FactSalesPerformance.
    """
)
def b2b_incremental_load_dag():

    start = EmptyOperator(task_id='start_incremental_load')
    end = EmptyOperator(task_id='end_incremental_load')

    # --- Task 1: Dimension - Account (Incremental Update - SCD Type 1: Overwrite) ---
    @task
    def update_dim_account(**context):
        last_run_ts = get_last_success_time(**context)
        current_run_ts = context['data_interval_end'].isoformat() # Use interval end
        logging.info(f"Checking for Account updates between {last_run_ts} and {current_run_ts}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        # Assume Accounts has updated_at. If not, use another strategy.
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
        -- WHERE a.accountid > (SELECT MAX(accountid) from dimaccount) -- Alternative if no timestamp
        """
        changed_data = hook_oltp.get_records(sql_extract, parameters=(last_run_ts, current_run_ts))

        if not changed_data:
            logging.info("No Account changes detected.")
            return

        logging.info(f"Found {len(changed_data)} changed/new accounts.")
        olap_data = [
            (row[0], row[1], row[2], row[3], row[4]) for row in changed_data
        ]

        # Upsert into OLAP
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        # Use insert_rows with replace=True for SCD Type 1
        hook_olap.insert_rows(
            table="DimAccount",
            rows=olap_data,
            target_fields=["AccountID", "AccountName", "Sector", "RevenueRange", "ParentAccountID"],
            commit_every=1000, # Batch size for commit
            replace=True,
            replace_index="AccountID" # Key to check for conflict
        )
        logging.info(f"Upserted {len(olap_data)} records into DimAccount.")

    # --- Task 2: Dimension - Product (Incremental Update - SCD based on schema) ---
    @task
    def update_dim_product(**context):
        last_run_ts = get_last_success_time(**context)
        current_run_ts = context['data_interval_end'].isoformat()
        today_date = pendulum.now().to_date_string() # For setting ValidTo/ValidFrom
        logging.info(f"Checking for Product updates between {last_run_ts} and {current_run_ts}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        # Assume Products has updated_at
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
        changed_data = hook_oltp.get_records(sql_extract, parameters=(last_run_ts, current_run_ts))

        if not changed_data:
            logging.info("No Product changes detected.")
            return

        logging.info(f"Found {len(changed_data)} changed/new products to process for SCD.")
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        updated_count = 0
        inserted_count = 0

        with hook_olap.get_conn() as conn:
            with conn.cursor() as cur:
                for row in changed_data:
                    product_id = row[0]
                    new_name = row[1]
                    new_series = row[2]
                    new_price = row[3]
                    new_price_range = row[4]

                    # Check if product exists and if relevant attributes changed
                    cur.execute("""
                        SELECT ProductName, SeriesName, Price, PriceRange, ValidTo, IsCurrent
                        FROM DimProduct
                        WHERE ProductID = %s
                    """, (product_id,))
                    existing_product = cur.fetchone()

                    if existing_product:
                        # Product exists, check for changes
                        old_name, old_series, old_price, old_range, old_valid_to, old_is_current = existing_product
                        # Define which attributes trigger an update
                        if (new_name != old_name or new_series != old_series or
                            new_price != old_price or new_price_range != old_range):

                            logging.info(f"SCD Update detected for ProductID: {product_id}")

                            # --- SCD Logic based on schema (Update validity on existing row) ---
                            # This is NOT standard SCD2. It modifies the existing record's validity.
                            # A true SCD2 would likely expire the old row and insert a new one
                            # with a new surrogate key if the schema supported it.

                            # Option 1: Simply update the existing row and its ValidFrom date
                            # This loses history of the *previous* state's validity period.
                            # cur.execute("""
                            #     UPDATE DimProduct
                            #     SET ProductName = %s, SeriesName = %s, Price = %s, PriceRange = %s,
                            #         ValidFrom = %s, ValidTo = NULL, IsCurrent = TRUE
                            #     WHERE ProductID = %s;
                            # """, (new_name, new_series, new_price, new_price_range, today_date, product_id))

                            # Option 2: Try to maintain some history by setting ValidTo on the old one
                            # This ONLY works if the change means the *old* version is no longer valid *at all*.
                            # If the old version might still apply to past facts, this is incorrect.
                            # ASSUMING a change invalidates the previous state immediately:
                            if old_is_current:
                                previous_day = (pendulum.parse(today_date).subtract(days=1)).to_date_string()
                                logging.info(f" Expiring old record for ProductID {product_id}, setting ValidTo={previous_day}")
                                cur.execute("""
                                    UPDATE DimProduct
                                    SET ValidTo = %s, IsCurrent = FALSE
                                    WHERE ProductID = %s AND IsCurrent = TRUE;
                                """, (previous_day, product_id))
                                updated_count += cur.rowcount # Count expiry update

                            # Now, Upsert the new version. If the previous step expired a row,
                            # this will insert. If no row was current (or exists), it will insert.
                            # If a row existed but wasn't current, this potentially updates it (check logic).
                            # Using ON CONFLICT to handle inserting the 'new' state or updating
                            # an existing placeholder if one exists (e.g., after expiry).
                            logging.info(f" Upserting new state for ProductID {product_id} as current.")
                            cur.execute("""
                                INSERT INTO DimProduct
                                    (ProductID, ProductName, SeriesName, Price, PriceRange, ValidFrom, ValidTo, IsCurrent)
                                VALUES (%s, %s, %s, %s, %s, %s, NULL, TRUE)
                                ON CONFLICT (ProductID) DO UPDATE SET
                                    ProductName = EXCLUDED.ProductName,
                                    SeriesName = EXCLUDED.SeriesName,
                                    Price = EXCLUDED.Price,
                                    PriceRange = EXCLUDED.PriceRange,
                                    ValidFrom = EXCLUDED.ValidFrom,
                                    ValidTo = EXCLUDED.ValidTo,
                                    IsCurrent = EXCLUDED.IsCurrent;
                             """, (product_id, new_name, new_series, new_price, new_price_range, today_date))
                            # This upsert logic needs careful thought depending on exact SCD requirement vs schema.
                            # The ON CONFLICT handles both inserting after expiry and updating if somehow needed.
                            inserted_count += cur.rowcount # Count upsert

                        else:
                            # No relevant attribute changed, potentially just timestamp updated
                            logging.info(f"No significant change detected for existing ProductID: {product_id}")

                    else:
                        # Product does not exist, insert as new current record
                        logging.info(f"Inserting new ProductID: {product_id}")
                        cur.execute("""
                             INSERT INTO DimProduct
                                 (ProductID, ProductName, SeriesName, Price, PriceRange, ValidFrom, ValidTo, IsCurrent)
                             VALUES (%s, %s, %s, %s, %s, %s, NULL, TRUE);
                         """, (product_id, new_name, new_series, new_price, new_price_range, today_date))
                        inserted_count += cur.rowcount

            conn.commit() # Commit transaction after processing all products in the batch

        logging.info(f"DimProduct SCD processing complete. Updated (Expired): {updated_count}, Inserted/Upserted: {inserted_count}")


    # --- Task 3: Dimension - SalesAgent (Incremental Update - SCD Type 1: Overwrite) ---
    @task
    def update_dim_sales_agent(**context):
        last_run_ts = get_last_success_time(**context)
        current_run_ts = context['data_interval_end'].isoformat()
        logging.info(f"Checking for SalesAgent updates between {last_run_ts} and {current_run_ts}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        # Assume SalesAgents has updated_at
        sql_extract = """
        SELECT
            sa.SalesAgentID, sa.SalesAgentName, sm.ManagerName, l.LocationName AS Region
        FROM SalesAgents sa
        LEFT JOIN SalesManagers sm ON sa.ManagerID = sm.ManagerID
        LEFT JOIN Locations l ON sa.RegionalOfficeID = l.LocationID
        WHERE sa.updated_at > %s AND sa.updated_at <= %s;
        """
        changed_data = hook_oltp.get_records(sql_extract, parameters=(last_run_ts, current_run_ts))

        if not changed_data:
            logging.info("No SalesAgent changes detected.")
            return

        logging.info(f"Found {len(changed_data)} changed/new sales agents.")
        olap_data = [
            (row[0], row[1], row[2], row[3]) for row in changed_data
        ]

        # Upsert into OLAP
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        hook_olap.insert_rows(
            table="DimSalesAgent",
            rows=olap_data,
            target_fields=["SalesAgentID", "SalesAgentName", "ManagerName", "Region"],
            commit_every=1000,
            replace=True,
            replace_index="SalesAgentID"
        )
        logging.info(f"Upserted {len(olap_data)} records into DimSalesAgent.")


    # --- Task 4: Fact Table - SalesPerformance (Incremental Load/Update) ---
    @task
    def update_fact_sales_performance(**context):
        last_run_ts = get_last_success_time(**context)
        current_run_ts = context['data_interval_end'].isoformat()
        logging.info(f"Checking for SalesPipeline updates between {last_run_ts} and {current_run_ts}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        # Crucial: Need updated_at on SalesPipeline to catch changes (stage, value, dates)
        sql_extract = """
        SELECT
            sp.OpportunityID, -- Needed for potential updates/deduplication
            sp.SalesAgentID, sp.ProductID, sp.AccountID, sp.DealStageID,
            sp.EngageDate, sp.CloseDate, sp.CloseValue,
            CASE WHEN sp.CloseDate IS NOT NULL AND sp.EngageDate IS NOT NULL
                 THEN sp.CloseDate - sp.EngageDate ELSE NULL END AS DurationDays,
            CASE ds.StageName -- Example logic
               WHEN 'Won' THEN 100.0 WHEN 'Lost' THEN 0.0
               WHEN 'Engaging' THEN 75.0 WHEN 'Prospecting' THEN 25.0
               ELSE 10.0 END AS ExpectedSuccessRate,
            TO_CHAR(COALESCE(sp.CloseDate, sp.EngageDate, CURRENT_DATE), 'YYYYMMDD')::INT AS DateKey
        FROM SalesPipeline sp
        LEFT JOIN DealStages ds ON sp.DealStageID = ds.DealStageID
        WHERE sp.updated_at > %s AND sp.updated_at <= %s;
        """
        changed_data = hook_oltp.get_records(sql_extract, parameters=(last_run_ts, current_run_ts))

        if not changed_data:
            logging.info("No SalesPipeline changes detected.")
            return

        logging.info(f"Found {len(changed_data)} changed/new pipeline records.")

        # Prepare data for OLAP Fact table - Lookup CURRENT dimension keys
        # This requires joining/querying OLAP dimension tables now.
        # For simplicity here, we assume keys match. A robust version MUST lookup keys.
        # E.g., ProductKey = SELECT ProductID FROM DimProduct WHERE OltpProductID = sp.ProductID AND IsCurrent = TRUE
        olap_data = []
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID) # Needed for lookups
        for row in changed_data:
            # --- WARNING: Simplified Key Mapping ---
            # Real implementation needs to look up the correct surrogate keys in OLAP dims
            # Example lookup (inefficient - better to batch lookups):
            # product_key = hook_olap.get_first("SELECT ProductID FROM DimProduct WHERE ProductID = %s AND IsCurrent = TRUE", parameters=(row[2],))[0]
            # account_key = hook_olap.get_first("SELECT AccountID FROM DimAccount WHERE AccountID = %s", parameters=(row[3],))[0]
            # agent_key = hook_olap.get_first("SELECT SalesAgentID FROM DimSalesAgent WHERE SalesAgentID = %s", parameters=(row[1],))[0]
            # stage_key = hook_olap.get_first("SELECT StageID FROM DimDealStage WHERE StageID = %s", parameters=(row[4],))[0]
            # Using direct IDs assuming they match for this example
            product_key = row[2]
            account_key = row[3]
            agent_key = row[1]
            stage_key = row[4]
            # ------------------------------------

            olap_data.append((
                row[10], # DateKey
                account_key,
                product_key,
                agent_key,
                stage_key,
                row[7],  # CloseValue
                row[8],  # DurationDays
                row[9],  # ExpectedSuccessRate
                row[0]   # Include OpportunityID for potential UPSERT logic
            ))


        # Load into OLAP Fact table - Decide Insert vs Upsert
        # Option A: Pure Insert (if each change creates a new fact record - less common)
        # Option B: Upsert based on a business key (e.g., OpportunityID if it uniquely identifies a deal over time)
        # Let's assume we need to UPSERT based on OpportunityID if it can be updated.
        # FactSalesPerformance has its own SERIAL PK, so we need a different approach than insert_rows with replace=True.
        # We need INSERT ... ON CONFLICT (unique_constraint_on_business_key) DO UPDATE SET ...

        # Create a unique constraint on the logical key columns in FactSalesPerformance if needed for upsert
        # Example: ALTER TABLE FactSalesPerformance ADD CONSTRAINT fact_logical_key UNIQUE (DateKey, AccountKey, ProductKey, SalesAgentKey); -- Choose appropriate columns
        # OR, if OpportunityID is the link back to the deal source:
        # We might need an OpportunityID column in the fact table itself for robust upserting.
        # Assuming OpportunityID is NOT in the fact table per the schema:
        # Treat as append-only fact table. Insert all changes. Analysis layer handles duplicates/latest state if needed.

        logging.info(f"Inserting {len(olap_data)} new/changed records into FactSalesPerformance (append-only).")
        hook_olap.insert_rows(
            table="FactSalesPerformance",
            rows=[row[:-1] for row in olap_data], # Exclude OpportunityID if not in target table
            target_fields=[
                "DateKey", "AccountKey", "ProductKey", "SalesAgentKey",
                "DealStageKey", "CloseValue", "DurationDays", "ExpectedSuccessRate"
            ],
            commit_every=1000
        )
        logging.info(f"Inserted {len(olap_data)} records into FactSalesPerformance.")

        # --- IF UPSERT based on OpportunityID was needed AND OpportunityID was in Fact Table ---
        # with hook_olap.get_conn() as conn:
        #     with conn.cursor() as cur:
        #         upsert_sql = """
        #         INSERT INTO FactSalesPerformance (
        #             OpportunityID, DateKey, AccountKey, ProductKey, SalesAgentKey,
        #             DealStageKey, CloseValue, DurationDays, ExpectedSuccessRate
        #         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        #         ON CONFLICT (OpportunityID) DO UPDATE SET -- Requires UNIQUE constraint on OpportunityID
        #             DateKey = EXCLUDED.DateKey,
        #             AccountKey = EXCLUDED.AccountKey,
        #             ProductKey = EXCLUDED.ProductKey,
        #             SalesAgentKey = EXCLUDED.SalesAgentKey,
        #             DealStageKey = EXCLUDED.DealStageKey,
        #             CloseValue = EXCLUDED.CloseValue,
        #             DurationDays = EXCLUDED.DurationDays,
        #             ExpectedSuccessRate = EXCLUDED.ExpectedSuccessRate;
        #         """
        #         # Note: Reorder olap_data elements to match INSERT sequence if OpportunityID is first
        #         # Example: (row[0], row[10], account_key, ...)
        #         cur.executemany(upsert_sql, olap_data) # Use executemany for better performance
        #     conn.commit()
        # logging.info(f"Upserted {len(olap_data)} records into FactSalesPerformance.")
        # ------------------------------------------------------------------------------------


    # --- Task Dependencies ---
    task_update_account = update_dim_account()
    task_update_product = update_dim_product()
    task_update_agent = update_dim_sales_agent()
    task_update_facts = update_fact_sales_performance()

    # Dimensions can update in parallel
    start >> [task_update_account, task_update_product, task_update_agent]

    # Fact update depends on dimensions being up-to-date for key lookups
    [task_update_account, task_update_product, task_update_agent] >> task_update_facts

    task_update_facts >> end

# Instantiate the DAG
b2b_incremental_load_dag()