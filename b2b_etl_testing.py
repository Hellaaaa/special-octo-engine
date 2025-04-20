# /path/to/your/airflow/dags/b2b_etl_testing_dag.py
import pendulum
import logging
import time
from datetime import timedelta

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException
from airflow.models import DagRun
from airflow.utils.state import State
from airflow.utils.types import DagRunType

# --- Configuration ---
OLTP_CONN_ID = "b2b_sales"
OLAP_CONN_ID = "b2b_sales_olap"
TARGET_INCREMENTAL_DAG_ID = "b2b_incremental_load"

# --- Test Data IDs ---
# Use specific IDs unlikely to exist or easy to clean up
TEST_OPPORTUNITY_ID = 9999999
TEST_PRODUCT_ID = 9999998
TEST_ACCOUNT_ID = 9999997
TEST_AGENT_ID = 9999996
TEST_STAGE_ID = 1 # Assuming stage ID 1 exists

# --- Helper Functions ---
def check_dag_run_status(target_dag_id, run_id):
    """Polls for the status of a triggered DAG run."""
    max_attempts = 30 # 30 attempts * 60s interval = 30 mins timeout
    poll_interval = 60 # seconds

    for attempt in range(max_attempts):
        logging.info(f"Checking status of {target_dag_id} run_id {run_id} (Attempt {attempt + 1}/{max_attempts})")
        dag_run = DagRun.find(dag_id=target_dag_id, run_id=run_id)
        if not dag_run:
            logging.warning(f"DAG run {run_id} not found yet.")
            time.sleep(poll_interval)
            continue

        current_state = dag_run[0].state
        logging.info(f" Current state: {current_state}")
        if current_state == State.SUCCESS:
            logging.info(f"DAG run {run_id} completed successfully.")
            return True
        elif current_state in (State.FAILED, State.UPSTREAM_FAILED):
            logging.error(f"DAG run {run_id} failed with state {current_state}.")
            raise AirflowFailException(f"Triggered DAG {target_dag_id} run {run_id} failed.")
        elif current_state == State.QUEUED or current_state == State.RUNNING:
            time.sleep(poll_interval)
        else:
             logging.warning(f"DAG run {run_id} in unexpected state: {current_state}")
             time.sleep(poll_interval)

    raise AirflowFailException(f"Timeout waiting for triggered DAG {target_dag_id} run {run_id} to complete.")


# --- DAG Definition ---
@dag(
    dag_id='b2b_etl_testing',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None, # Manual trigger
    catchup=False,
    tags=['b2b_sales', 'testing'],
    doc_md="""
    ### B2B Sales ETL Testing DAG

    Provides scenarios to test the incremental ETL process.

    **Scenarios:**
    1.  **Test Incremental Fact Load:** Inserts a new sales pipeline record, triggers incremental load, verifies fact exists in OLAP.
    2.  **Test SCD Product Update:** Updates an existing product, triggers incremental load, verifies SCD logic in DimProduct.

    **Cleanup:** Includes tasks to remove test data from OLTP and OLAP. Run cleanup even if verification fails.
    """
)
def b2b_etl_testing_dag():

    # --- Test Group 1: Incremental Fact Load ---
    @task_group(group_id='test_incremental_fact_load')
    def tg1_test_incremental_fact():

        @task
        def setup_oltp_fact_data():
            """Insert test data into OLTP SalesPipeline."""
            logging.info(f"Inserting test opportunity {TEST_OPPORTUNITY_ID} into OLTP.")
            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            # Ensure dependent records exist or handle FK constraints
            # Minimal insert:
            hook_oltp.run(
                """
                -- Ensure dummy dependent records exist first (or use existing ones)
                INSERT INTO Sectors (SectorID, SectorName) VALUES (999, 'Test Sector') ON CONFLICT DO NOTHING;
                INSERT INTO Locations (LocationID, LocationName) VALUES (999, 'Test Location') ON CONFLICT DO NOTHING;
                INSERT INTO Accounts (AccountID, AccountName, SectorID, HeadquartersID) VALUES (%s, 'Test Account Inc', 999, 999) ON CONFLICT DO NOTHING;
                INSERT INTO ProductSeries (SeriesID, SeriesName) VALUES (999, 'Test Series') ON CONFLICT DO NOTHING;
                INSERT INTO Products (ProductID, ProductName, SeriesID, SalesPrice) VALUES (%s, 'Test Product Fact', 999, 500) ON CONFLICT DO NOTHING;
                INSERT INTO SalesManagers (ManagerID, ManagerName) VALUES (999, 'Test Manager') ON CONFLICT DO NOTHING;
                INSERT INTO SalesAgents (SalesAgentID, SalesAgentName, ManagerID, RegionalOfficeID) VALUES (%s, 'Test Agent Fact', 999, 999) ON CONFLICT DO NOTHING;
                INSERT INTO DealStages (DealStageID, StageName) VALUES (%s, 'Prospecting') ON CONFLICT DO NOTHING;

                -- Insert the actual test pipeline record
                INSERT INTO SalesPipeline (OpportunityID, SalesAgentID, ProductID, AccountID, DealStageID, EngageDate, CloseDate, CloseValue, updated_at)
                VALUES (%s, %s, %s, %s, %s, CURRENT_DATE - INTERVAL '5 days', NULL, 10000, NOW())
                ON CONFLICT (OpportunityID) DO UPDATE SET updated_at = NOW(); -- Ensure updated_at is recent
                """,
                parameters=(
                    TEST_ACCOUNT_ID, TEST_PRODUCT_ID, TEST_AGENT_ID, TEST_STAGE_ID, # For FKs
                    TEST_OPPORTUNITY_ID, TEST_AGENT_ID, TEST_PRODUCT_ID, TEST_ACCOUNT_ID, TEST_STAGE_ID # For pipeline
                )
            )

        trigger_incremental_dag = TriggerDagRunOperator(
            task_id="trigger_incremental_load_for_fact_test",
            trigger_dag_id=TARGET_INCREMENTAL_DAG_ID,
            conf={'test_run': True, 'scenario': 'fact_load'}, # Pass context if needed
            wait_for_completion=True, # Wait for the triggered DAG
            poke_interval=30, # Check every 30s
            timeout=timedelta(minutes=20).total_seconds(), # Timeout
            failed_states=[State.FAILED, State.UPSTREAM_FAILED]
        )

        @task
        def verify_fact_in_olap():
            """Check if the corresponding fact record exists in OLAP."""
            logging.info(f"Verifying fact data for OLTP OpportunityID {TEST_OPPORTUNITY_ID} in OLAP.")
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            # Query based on the keys expected from the test data
            result = hook_olap.get_records(
                """
                SELECT COUNT(*) FROM FactSalesPerformance
                WHERE AccountKey = %s AND ProductKey = %s AND SalesAgentKey = %s AND DealStageKey = %s;
                -- Add more checks like CloseValue if needed
                """,
                parameters=(TEST_ACCOUNT_ID, TEST_PRODUCT_ID, TEST_AGENT_ID, TEST_STAGE_ID)
            )
            if not result or result[0][0] == 0:
                 raise AirflowFailException(f"Test fact record not found in OLAP for keys ({TEST_ACCOUNT_ID}, {TEST_PRODUCT_ID}, {TEST_AGENT_ID}, {TEST_STAGE_ID}).")
            else:
                 logging.info(f"Verification successful: Found {result[0][0]} matching fact record(s).")


        @task(trigger_rule='all_done') # Run cleanup even if verification fails
        def cleanup_oltp_fact_data():
            """Remove test data from OLTP."""
            logging.info(f"Cleaning up test data (OpportunityID {TEST_OPPORTUNITY_ID}) from OLTP.")
            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            # Delete in reverse order of insertion or handle FKs appropriately
            hook_oltp.run("DELETE FROM SalesPipeline WHERE OpportunityID = %s;", parameters=(TEST_OPPORTUNITY_ID,))
            # Optionally delete other test setup records if exclusively for this test
            # hook_oltp.run("DELETE FROM Products WHERE ProductID = %s;", parameters=(TEST_PRODUCT_ID,))
            # ... etc ...


        @task(trigger_rule='all_done')
        def cleanup_olap_fact_data():
            """Remove test fact data from OLAP."""
            logging.info(f"Cleaning up test fact data related to test keys from OLAP.")
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            hook_olap.run(
                 """DELETE FROM FactSalesPerformance
                 WHERE AccountKey = %s AND ProductKey = %s AND SalesAgentKey = %s;""",
                 parameters=(TEST_ACCOUNT_ID, TEST_PRODUCT_ID, TEST_AGENT_ID)
            )

        # Task dependencies within the group
        setup_task = setup_oltp_fact_data()
        verify_task = verify_fact_in_olap()
        cleanup_oltp_task = cleanup_oltp_fact_data()
        cleanup_olap_task = cleanup_olap_fact_data()

        setup_task >> trigger_incremental_dag >> verify_task
        verify_task >> [cleanup_oltp_task, cleanup_olap_task]


    # --- Test Group 2: SCD Product Update ---
    @task_group(group_id='test_scd_product_update')
    def tg2_test_scd_product():

        PRODUCT_ORIGINAL_PRICE = 777.00
        PRODUCT_UPDATED_PRICE = 888.00

        @task
        def setup_oltp_product_data():
            """Insert/Reset test product in OLTP."""
            logging.info(f"Ensuring test product {TEST_PRODUCT_ID} exists in OLTP with price {PRODUCT_ORIGINAL_PRICE}.")
            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
             # Ensure dummy series exists
            hook_oltp.run("INSERT INTO ProductSeries (SeriesID, SeriesName) VALUES (998, 'SCD Test Series') ON CONFLICT DO NOTHING;")
            # Insert or reset the product
            hook_oltp.run(
                """
                INSERT INTO Products (ProductID, ProductName, SeriesID, SalesPrice, updated_at)
                VALUES (%s, 'Test SCD Product', 998, %s, NOW() - interval '1 day') -- Ensure old updated_at initially
                ON CONFLICT (ProductID) DO UPDATE SET
                    ProductName = EXCLUDED.ProductName,
                    SeriesID = EXCLUDED.SeriesID,
                    SalesPrice = EXCLUDED.SalesPrice,
                    updated_at = NOW() - interval '1 day';
                """,
                parameters=(TEST_PRODUCT_ID, PRODUCT_ORIGINAL_PRICE)
            )
            # Run initial incremental load to get the product into OLAP in its initial state
            # This relies on the trigger operator finding the DAG run ID
            context = get_current_context()
            run_id = f"test_setup_scd_product_{context['ts_nodash']}"
            TriggerDagRunOperator.partial(
                task_id="trigger_incremental_load_for_scd_setup",
                trigger_dag_id=TARGET_INCREMENTAL_DAG_ID,
                conf={'test_run': True, 'scenario': 'scd_setup', 'run_id': run_id}
                # wait_for_completion=False here, check status manually below
            ).execute(context=context)

            time.sleep(10) # Give scheduler time to create run
            check_dag_run_status(TARGET_INCREMENTAL_DAG_ID, run_id)
            logging.info("Initial state loaded into OLAP.")


        @task
        def update_oltp_product():
            """Update the test product's price in OLTP."""
            logging.info(f"Updating test product {TEST_PRODUCT_ID} price to {PRODUCT_UPDATED_PRICE} in OLTP.")
            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            hook_oltp.run(
                "UPDATE Products SET SalesPrice = %s, updated_at = NOW() WHERE ProductID = %s;",
                parameters=(PRODUCT_UPDATED_PRICE, TEST_PRODUCT_ID)
            )

        trigger_incremental_dag = TriggerDagRunOperator(
            task_id="trigger_incremental_load_for_scd_test",
            trigger_dag_id=TARGET_INCREMENTAL_DAG_ID,
            conf={'test_run': True, 'scenario': 'scd_update'},
            wait_for_completion=True,
            poke_interval=30,
            timeout=timedelta(minutes=20).total_seconds(),
            failed_states=[State.FAILED, State.UPSTREAM_FAILED]
        )

        @task
        def verify_scd_product_in_olap():
            """Check DimProduct for correct SCD handling based on the schema."""
            logging.info(f"Verifying SCD changes for ProductID {TEST_PRODUCT_ID} in OLAP.")
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            # Retrieve all records for the test ProductID
            # The verification depends HEAVILY on the SCD implementation in the incremental DAG
            # Based on the 'update validity' logic in b2b_incremental_load:
            # We expect ONE record, with the NEW price, and ValidFrom set to today (or run date)
            result = hook_olap.get_records(
                """
                SELECT Price, ValidFrom, ValidTo, IsCurrent
                FROM DimProduct
                WHERE ProductID = %s;
                """,
                parameters=(TEST_PRODUCT_ID,)
            )

            if not result:
                raise AirflowFailException(f"Test product {TEST_PRODUCT_ID} not found in DimProduct after update.")

            if len(result) > 1:
                 # This would only happen if the SCD logic was changed to true row versioning AND used the same ProductID
                 logging.warning(f"Found {len(result)} rows for ProductID {TEST_PRODUCT_ID}. Verifying the current one.")
                 current_row = [row for row in result if row[3] is True]
                 if not current_row:
                      raise AirflowFailException(f"Test product {TEST_PRODUCT_ID} has multiple rows but none are current.")
                 row = current_row[0]
            else:
                row = result[0] # Only one row expected

            # Check the single row's state
            olap_price, olap_valid_from, olap_valid_to, olap_is_current = row

            # Price should be updated
            # Note: Decimal comparison needs care, cast or use tolerance
            if abs(float(olap_price) - PRODUCT_UPDATED_PRICE) > 0.01:
                 raise AirflowFailException(f"SCD Verification Failed: Price mismatch. Expected {PRODUCT_UPDATED_PRICE}, got {olap_price}.")

            # Should be current
            if not olap_is_current:
                 raise AirflowFailException(f"SCD Verification Failed: Record is not current (IsCurrent=False).")

            # ValidTo should be NULL
            if olap_valid_to is not None:
                 raise AirflowFailException(f"SCD Verification Failed: ValidTo should be NULL, got {olap_valid_to}.")

            # ValidFrom should be recent (e.g., today's date or DAG run date)
            if olap_valid_from < date.today() - timedelta(days=1): # Allow for yesterday if run just after midnight
                 raise AirflowFailException(f"SCD Verification Failed: ValidFrom date ({olap_valid_from}) is older than expected.")

            logging.info("SCD Verification Successful for Product Update.")


        @task(trigger_rule='all_done')
        def cleanup_oltp_product_data():
            """Remove test product from OLTP."""
            logging.info(f"Cleaning up test product {TEST_PRODUCT_ID} from OLTP.")
            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            hook_oltp.run("DELETE FROM Products WHERE ProductID = %s;", parameters=(TEST_PRODUCT_ID,))
            # hook_oltp.run("DELETE FROM ProductSeries WHERE SeriesID = 998;") # Optional

        @task(trigger_rule='all_done')
        def cleanup_olap_product_data():
            """Remove test product data from OLAP."""
            logging.info(f"Cleaning up test product {TEST_PRODUCT_ID} from DimProduct.")
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            hook_olap.run("DELETE FROM DimProduct WHERE ProductID = %s;", parameters=(TEST_PRODUCT_ID,))

        # Task dependencies within the group
        setup_task = setup_oltp_product_data()
        update_task = update_oltp_product()
        verify_task = verify_scd_product_in_olap()
        cleanup_oltp_task = cleanup_oltp_product_data()
        cleanup_olap_task = cleanup_olap_product_data()

        setup_task >> update_task >> trigger_incremental_dag >> verify_task
        verify_task >> [cleanup_oltp_task, cleanup_olap_task]

    # Instantiate task groups
    test_fact_group = tg1_test_incremental_fact()
    test_scd_group = tg2_test_scd_product()

    # Could add dependencies between test groups if needed, but usually run independently
    # setup_all >> [test_fact_group, test_scd_group] >> cleanup_all


# Instantiate the DAG
b2b_etl_testing_dag()