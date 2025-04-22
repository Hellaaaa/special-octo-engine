# /path/to/your/airflow/dags/b2b_etl_test_scenarios_dag.py
import pendulum
import logging
from datetime import timedelta
import json # To handle configuration passing for TriggerDagRunOperator

# Import necessary Airflow components
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State # Required for allowed_states/failed_states
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException

# --- Configuration ---
OLTP_CONN_ID = "b2b_sales" # Connection ID for your source OLTP database
OLAP_CONN_ID = "b2b_sales_olap" # Connection ID for your target OLAP database
INCREMENTAL_DAG_ID = "b2b_incremental_load" # The DAG ID to trigger and wait for

# Test Data IDs (ensure these exist in your OLTP, except for the new ones)
TEST_ACC_ID_S1_NEW = 99999 # Explicit ID for the new test account
TEST_ACC_NAME_S1 = f'Test New Account Inc - ID {TEST_ACC_ID_S1_NEW}' # Include ID in name
TEST_AGENT_ID_S1 = 5
# TEST_AGENT_REGION_S1_ORIG = 'Original Region S1' # Placeholder - Not used in current logic
TEST_AGENT_MANAGER_ID_S1_ORIG = 1 # Placeholder - Get the actual original value if needed for complex revert
TEST_AGENT_MANAGER_ID_S1_NEW = 5 # As per scenario
# Generate unique Opp ID based on the test DAG run's ID
TEST_OPP_ID_S1_NEW = f'TEST_INC_NEW_{{{{ dag_run.id }}}}'
TEST_OPP_ID_S1_UPDATE = 'SBCR987L' # Existing OpportunityID to update
TEST_OPP_STAGE_S1_ORIG = 1 # Placeholder - Get the actual original value if needed for complex revert
TEST_OPP_STAGE_S1_NEW = 4 # As per scenario
TEST_OPP_VALUE_S1_ORIG = 590.00 # Placeholder - Get the actual original value if needed for complex revert
TEST_OPP_VALUE_S1_NEW = 650.00 # As per scenario

TEST_PROD_ID_S2 = 3
TEST_PROD_NAME_S2_SUFFIX = ' (Updated Scenario 2)'
TEST_PROD_PRICE_MULTIPLIER_S2 = 1.1

# --- DAG Definition ---
@dag(
    dag_id='b2b_etl_test_scenarios',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None, # Manual trigger only
    catchup=False,
    default_args={'owner': 'airflow', 'retries': 0}, # No retries for tests
    tags=['b2b_sales', 'test', 'etl'],
    doc_md="""
    ### ETL Test Scenarios DAG (Fixed State Args)

    Automates running test scenarios described previously.
    Triggers the incremental load DAG and waits for it to complete.

    **WARNING:** Modifies data in the OLTP database.
    Run in a controlled environment. Assumes specific data IDs exist.
    Verification steps log results but do not automatically fail the DAG on assertion errors.
    Cleanup steps attempt to revert OLTP changes.
    """
)
def b2b_etl_test_scenarios_dag():

    start = EmptyOperator(task_id='start_tests')
    end = EmptyOperator(task_id='end_tests', trigger_rule=TriggerRule.ALL_DONE) # Ensure end runs

    # --- Scenario 1 Group ---
    @task_group(group_id='scenario_1_incremental_update')
    def scenario_1_group():
        @task
        def modify_oltp_s1(**context) -> dict: # Added context to access ti
            """Applies data modifications in OLTP for Scenario 1."""
            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            original_agent_data = None
            original_opp_data = None
            # Generate unique Opp ID for this run using Airflow context
            ti = context['ti'] # Get task instance from context
            # Use a simpler unique ID for testing if dag_run.id is complex/long
            new_opp_id_s1 = f'TEST_S1_{ti.run_id[-8:]}' # Use last 8 chars of run_id

            logging.info("--- Scenario 1: Modifying OLTP Data ---")
            try:
                # Get original data for cleanup later (optional but good practice)
                original_agent_data = hook_oltp.get_first(f"SELECT ManagerID FROM SalesAgents WHERE SalesAgentID = {TEST_AGENT_ID_S1}")
                original_opp_data = hook_oltp.get_first(f"SELECT DealStageID, CloseValue FROM SalesPipeline WHERE OpportunityID = '{TEST_OPP_ID_S1_UPDATE}'")

                if not original_agent_data: logging.warning(f"Could not find original data for SalesAgentID {TEST_AGENT_ID_S1}")
                if not original_opp_data: logging.warning(f"Could not find original data for OpportunityID {TEST_OPP_ID_S1_UPDATE}")

                # 1. Insert new Account with explicit ID
                sql_insert_acc = f"""
                INSERT INTO Accounts (AccountID, AccountName, SectorID, YearEstablished, Revenue, Employees, HeadquartersID, created_at, updated_at)
                VALUES ({TEST_ACC_ID_S1_NEW}, '{TEST_ACC_NAME_S1}', 1, 2025, 50000.00, 10, 1, NOW(), NOW())
                ON CONFLICT (AccountID) DO NOTHING; -- Avoid error if test account already exists from failed cleanup
                """
                logging.info(f"Inserting/Ensuring Account with ID: {TEST_ACC_ID_S1_NEW}")
                hook_oltp.run(sql_insert_acc)

                # 2. Update existing SalesAgent
                sql_update_agent = f"""
                UPDATE SalesAgents SET ManagerID = {TEST_AGENT_MANAGER_ID_S1_NEW}, updated_at = NOW()
                WHERE SalesAgentID = {TEST_AGENT_ID_S1};"""
                logging.info(f"Updating SalesAgentID {TEST_AGENT_ID_S1}...")
                hook_oltp.run(sql_update_agent)

                # 3. Insert new SalesPipeline record
                sql_insert_opp = f"""
                INSERT INTO SalesPipeline (OpportunityID, SalesAgentID, ProductID, AccountID, DealStageID, EngageDate, CloseDate, CloseValue, created_at, updated_at)
                VALUES ('{new_opp_id_s1}', {TEST_AGENT_ID_S1}, {TEST_PROD_ID_S2}, {TEST_ACC_ID_S1_NEW}, {TEST_OPP_STAGE_S1_ORIG}, CURRENT_DATE - INTERVAL '10 days', CURRENT_DATE, 1234.56, NOW(), NOW());"""
                logging.info(f"Inserting new SalesPipeline record (OppID: {new_opp_id_s1})...")
                hook_oltp.run(sql_insert_opp)

                # 4. Update existing SalesPipeline record
                sql_update_opp = f"""
                UPDATE SalesPipeline SET DealStageID = {TEST_OPP_STAGE_S1_NEW}, CloseValue = {TEST_OPP_VALUE_S1_NEW}, updated_at = NOW()
                WHERE OpportunityID = '{TEST_OPP_ID_S1_UPDATE}';"""
                logging.info(f"Updating SalesPipeline OpportunityID {TEST_OPP_ID_S1_UPDATE}...")
                hook_oltp.run(sql_update_opp)

                logging.info("--- Scenario 1: OLTP Modifications Complete ---")
                # Pass original values and generated IDs for cleanup
                return {
                    "new_account_id": TEST_ACC_ID_S1_NEW,
                    "new_opp_id": new_opp_id_s1,
                    "original_agent_manager_id": original_agent_data[0] if original_agent_data else None,
                    "original_opp_stage_id": original_opp_data[0] if original_opp_data else None,
                    "original_opp_close_value": original_opp_data[1] if original_opp_data else None
                }
            except Exception as e:
                logging.error(f"Error modifying OLTP data for Scenario 1: {e}")
                raise

        # Define Trigger Operator at the DAG level within the group
        trigger_incremental_s1 = TriggerDagRunOperator(
            task_id="trigger_incremental_load_s1",
            trigger_dag_id=INCREMENTAL_DAG_ID,
            conf={"triggered_by_test": "scenario_1", "run_id": f"test_s1_{{{{ dag_run.id }}}}"}, # Use current dag_run.id for uniqueness
            wait_for_completion=True, # Wait for the triggered DAG to finish
            poke_interval=30,         # How often to check status
            # timeout=1800,           # REMOVED INVALID ARGUMENT
            allowed_states=[State.SUCCESS], # Required state
            # Corrected: Use only valid DagRunState values
            failed_states=[State.FAILED]
        )

        @task
        def verify_olap_s1():
            """Runs verification queries against OLAP after incremental load for Scenario 1."""
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            logging.info("--- Scenario 1: Verifying OLAP Data ---")
            try:
                # 1. Check new Account
                sql_check_acc = f"SELECT AccountID, AccountName FROM DimAccount WHERE AccountID = {TEST_ACC_ID_S1_NEW}"
                res_acc = hook_olap.get_records(sql_check_acc)
                logging.info(f"Check New Account (ID={TEST_ACC_ID_S1_NEW}): {res_acc}")
                if not res_acc or res_acc[0][0] != TEST_ACC_ID_S1_NEW: logging.warning("Verification FAILED: New Account not found or incorrect.")

                # 2. Check updated Agent (assuming SCD1 / UPSERT)
                sql_check_agent = f"SELECT SalesAgentID, ManagerName FROM DimSalesAgent WHERE SalesAgentID = {TEST_AGENT_ID_S1}"
                res_agent = hook_olap.get_first(sql_check_agent)
                logging.info(f"Check Updated Agent (ID={TEST_AGENT_ID_S1}): {res_agent}")
                # Add specific check if ManagerName is expected to change and DimSalesManager exists
                # if not res_agent or res_agent[1] != EXPECTED_MANAGER_NAME_AFTER_UPDATE: logging.warning("Verification FAILED: Agent update incorrect.")

                # 3. Check new Fact record (basic check)
                sql_check_fact_new = f"SELECT COUNT(*) FROM FactSalesPerformance WHERE AccountKey = {TEST_ACC_ID_S1_NEW}"
                res_fact_new = hook_olap.get_first(sql_check_fact_new)
                logging.info(f"Check New Fact (AccountKey={TEST_ACC_ID_S1_NEW}): Count = {res_fact_new[0] if res_fact_new else 'Error'}")
                if not res_fact_new or res_fact_new[0] < 1: logging.warning("Verification FAILED: New fact record not found.")

                # 4. Check updated Fact record (assuming UPSERT worked and OpportunityID is in Fact table)
                sql_check_fact_upd = f"""
                SELECT COUNT(*), MAX(DealStageKey), MAX(CloseValue)
                FROM FactSalesPerformance
                WHERE OpportunityID = '{TEST_OPP_ID_S1_UPDATE}'
                """
                res_fact_upd = hook_olap.get_first(sql_check_fact_upd)
                logging.info(f"Check Updated Fact (OppID='{TEST_OPP_ID_S1_UPDATE}'): Result = {res_fact_upd}")
                if not res_fact_upd or res_fact_upd[0] != 1 or res_fact_upd[1] != TEST_OPP_STAGE_S1_NEW or res_fact_upd[2] != TEST_OPP_VALUE_S1_NEW:
                    logging.warning(f"Verification FAILED: Updated fact check failed. Expected Count=1, Stage={TEST_OPP_STAGE_S1_NEW}, Value={TEST_OPP_VALUE_S1_NEW}")

            except Exception as e:
                logging.error(f"Error during OLAP verification for Scenario 1: {e}")
            logging.info("--- Scenario 1: OLAP Verification Complete (Check Logs) ---")

        @task(trigger_rule=TriggerRule.ALL_DONE) # Run cleanup regardless of verification status
        def cleanup_oltp_s1(modify_task_output: dict): # Get output from modify task via XComs
            """Reverts changes made in OLTP for Scenario 1."""
            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            new_opp_id = modify_task_output.get("new_opp_id") # Get the generated Opp ID
            original_data = modify_task_output # The whole dict is the original data context
            logging.info("--- Scenario 1: Cleaning Up OLTP Data ---")
            try:
                # Delete new SalesPipeline record
                if new_opp_id:
                    sql_delete_opp_new = f"DELETE FROM SalesPipeline WHERE OpportunityID = '{new_opp_id}'"
                    logging.info(f"Deleting OppID {new_opp_id}...")
                    hook_oltp.run(sql_delete_opp_new)
                else: logging.warning("Skipping delete for new OppID as it wasn't captured.")

                # Revert updated SalesPipeline record
                if original_data.get("original_opp_stage_id") is not None and original_data.get("original_opp_close_value") is not None:
                    sql_revert_opp = f"""UPDATE SalesPipeline SET DealStageID = {original_data['original_opp_stage_id']}, CloseValue = {original_data['original_opp_close_value']}, updated_at = NOW() WHERE OpportunityID = '{TEST_OPP_ID_S1_UPDATE}';"""
                    logging.info(f"Reverting OppID {TEST_OPP_ID_S1_UPDATE}...")
                    hook_oltp.run(sql_revert_opp)
                else: logging.warning(f"Skipping revert for OppID {TEST_OPP_ID_S1_UPDATE} (missing original data).")

                # Revert updated SalesAgent record
                if original_data.get("original_agent_manager_id") is not None:
                    sql_revert_agent = f"""UPDATE SalesAgents SET ManagerID = {original_data['original_agent_manager_id']}, updated_at = NOW() WHERE SalesAgentID = {TEST_AGENT_ID_S1};"""
                    logging.info(f"Reverting SalesAgentID {TEST_AGENT_ID_S1}...")
                    hook_oltp.run(sql_revert_agent)
                else: logging.warning(f"Skipping revert for SalesAgentID {TEST_AGENT_ID_S1} (missing original data).")

                # Delete new Account record using the known test ID
                sql_delete_acc = f"DELETE FROM Accounts WHERE AccountID = {TEST_ACC_ID_S1_NEW}"
                logging.info(f"Deleting AccountID {TEST_ACC_ID_S1_NEW}...")
                hook_oltp.run(sql_delete_acc)

                logging.info("--- Scenario 1: OLTP Cleanup Complete ---")
            except Exception as e:
                logging.error(f"Error during OLTP cleanup for Scenario 1: {e}")

        # Define flow for Scenario 1
        modify_oltp_task = modify_oltp_s1()
        verify_olap_task = verify_olap_s1()
        cleanup_oltp_task = cleanup_oltp_s1(modify_oltp_task) # Pass XCom implicitly

        modify_oltp_task >> trigger_incremental_s1 >> verify_olap_task >> cleanup_oltp_task

    # --- Scenario 2 Group ---
    @task_group(group_id='scenario_2_scd2_product')
    def scenario_2_group():
        @task
        def modify_oltp_s2() -> dict:
            """Applies data modifications in OLTP for Scenario 2 (Product SCD2)."""
            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            original_product_data = None
            logging.info("--- Scenario 2: Modifying OLTP Data ---")
            try:
                original_product_data = hook_oltp.get_first(f"SELECT ProductName, SalesPrice FROM Products WHERE ProductID = {TEST_PROD_ID_S2}")
                if not original_product_data:
                    logging.error(f"Cannot run Scenario 2: ProductID {TEST_PROD_ID_S2} not found in OLTP.")
                    raise AirflowSkipException(f"ProductID {TEST_PROD_ID_S2} not found.")
                new_name = original_product_data[0].replace(TEST_PROD_NAME_S2_SUFFIX, '') + TEST_PROD_NAME_S2_SUFFIX # Avoid appending suffix multiple times
                sql_update_prod = f"""UPDATE Products SET SalesPrice = SalesPrice * {TEST_PROD_PRICE_MULTIPLIER_S2}, ProductName = %s, updated_at = NOW() WHERE ProductID = %s;"""
                logging.info(f"Updating ProductID {TEST_PROD_ID_S2}...")
                hook_oltp.run(sql_update_prod, parameters=(new_name, TEST_PROD_ID_S2))
                logging.info("--- Scenario 2: OLTP Modifications Complete ---")
                return {"original_product_name": original_product_data[0], "original_product_price": original_product_data[1]}
            except Exception as e: logging.error(f"Error modifying OLTP data for Scenario 2: {e}"); raise

        # Define Trigger Operator at the DAG level within the group
        trigger_incremental_s2 = TriggerDagRunOperator(
            task_id="trigger_incremental_load_s2",
            trigger_dag_id=INCREMENTAL_DAG_ID,
            conf={"triggered_by_test": "scenario_2", "run_id": f"test_s2_{{{{ dag_run.id }}}}"}, # Use current dag_run.id
            wait_for_completion=True, # Wait for the triggered DAG to finish
            poke_interval=30,
            # timeout=1800,           # REMOVED INVALID ARGUMENT
            allowed_states=[State.SUCCESS], # USES IMPORTED State
            # Corrected: Use only valid DagRunState values
            failed_states=[State.FAILED]
        )

        @task
        def verify_olap_s2():
            """Runs verification queries against DimProduct for Scenario 2."""
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            logging.info("--- Scenario 2: Verifying OLAP Data (DimProduct SCD2) ---")
            try:
                sql_check_prod = f"""SELECT ProductID, ProductName, SalesPrice, ValidFrom, ValidTo, IsCurrent FROM DimProduct WHERE ProductID = {TEST_PROD_ID_S2} ORDER BY ValidFrom DESC;"""
                res_prod = hook_olap.get_records(sql_check_prod)
                logging.info(f"Check Product History (ID={TEST_PROD_ID_S2}):\n{json.dumps(res_prod, indent=2, default=str)}")
                if len(res_prod) < 2: logging.warning("Verification FAILED: Expected at least 2 versions for Product, found less.")
                elif not res_prod[0][5]: logging.warning("Verification FAILED: Expected latest version for Product to be IsCurrent=TRUE.")
                elif len(res_prod) > 1 and res_prod[1][5]: logging.warning("Verification FAILED: Expected previous version for Product to be IsCurrent=FALSE.") # Check index exists before accessing
                elif len(res_prod) > 1 and not res_prod[1][4]: logging.warning("Verification FAILED: Expected previous version for Product to have a ValidTo date.") # Check index exists
            except Exception as e: logging.error(f"Error during OLAP verification for Scenario 2: {e}")
            logging.info("--- Scenario 2: OLAP Verification Complete (Check Logs) ---")

        @task(trigger_rule=TriggerRule.ALL_DONE)
        def cleanup_oltp_s2(original_data: dict):
            """Reverts changes made in OLTP for Scenario 2."""
            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            logging.info("--- Scenario 2: Cleaning Up OLTP Data ---")
            if not original_data or original_data.get("original_product_name") is None:
                 logging.warning(f"Skipping cleanup for ProductID {TEST_PROD_ID_S2} due to missing original data.")
                 return
            try:
                # Use parameters to avoid SQL injection issues with names
                sql_revert_prod = f"""UPDATE Products SET ProductName = %s, SalesPrice = %s, updated_at = NOW() WHERE ProductID = %s;"""
                logging.info(f"Reverting ProductID {TEST_PROD_ID_S2}...")
                hook_oltp.run(sql_revert_prod, parameters=(original_data['original_product_name'], original_data['original_product_price'], TEST_PROD_ID_S2))
                logging.info("--- Scenario 2: OLTP Cleanup Complete ---")
            except Exception as e: logging.error(f"Error during OLTP cleanup for Scenario 2: {e}")

        # Define flow for Scenario 2
        modify_oltp_task_s2 = modify_oltp_s2()
        verify_olap_task_s2 = verify_olap_s2()
        cleanup_oltp_task_s2 = cleanup_oltp_s2(modify_oltp_task_s2) # Pass XCom implicitly

        modify_oltp_task_s2 >> trigger_incremental_s2 >> verify_olap_task_s2 >> cleanup_oltp_task_s2

    # Define overall DAG flow
    start >> scenario_1_group() >> scenario_2_group() >> end

# Instantiate the DAG
b2b_etl_test_scenarios_dag()
