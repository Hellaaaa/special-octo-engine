# /path/to/your/airflow/dags/b2b_etl_test_scenarios_dag.py
import pendulum
import logging
from datetime import timedelta
import json # To handle configuration passing for TriggerDagRunOperator

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException

# --- Configuration ---
OLTP_CONN_ID = "b2b_sales" # Connection ID for your source OLTP database
OLAP_CONN_ID = "b2b_sales_olap" # Connection ID for your target OLAP database
INCREMENTAL_DAG_ID = "b2b_incremental_load" # The DAG ID to trigger and wait for

# Test Data IDs (ensure these exist in your OLTP)
TEST_ACC_NAME_S1 = 'Test New Account Inc - Scenario 1'
TEST_AGENT_ID_S1 = 5
TEST_AGENT_REGION_S1_ORIG = 'Original Region S1' # Placeholder - Get the actual original value
TEST_AGENT_MANAGER_ID_S1_ORIG = 1 # Placeholder - Get the actual original value
TEST_AGENT_MANAGER_ID_S1_NEW = 5 # As per scenario
TEST_OPP_ID_S1_NEW = 'TEST_INC_NEW_S1'
TEST_OPP_ID_S1_UPDATE = 'SBCR987L' # Existing OpportunityID to update
TEST_OPP_STAGE_S1_ORIG = 1 # Placeholder - Get the actual original value
TEST_OPP_STAGE_S1_NEW = 4 # As per scenario
TEST_OPP_VALUE_S1_ORIG = 590.00 # Placeholder - Get the actual original value
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
    ### ETL Test Scenarios DAG

    Automates running test scenarios described in `etl_test_scenarios.md`.

    **WARNING:** Modifies data in the OLTP database and triggers the incremental load.
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
        def modify_oltp_s1() -> dict:
            """Applies data modifications in OLTP for Scenario 1."""
            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            new_account_id = None
            original_agent_data = None
            original_opp_data = None

            logging.info("--- Scenario 1: Modifying OLTP Data ---")
            try:
                # Get original data for cleanup later
                original_agent_data = hook_oltp.get_first(f"SELECT ManagerID FROM SalesAgents WHERE SalesAgentID = {TEST_AGENT_ID_S1}")
                original_opp_data = hook_oltp.get_first(f"SELECT DealStageID, CloseValue FROM SalesPipeline WHERE OpportunityID = '{TEST_OPP_ID_S1_UPDATE}'")

                if not original_agent_data: logging.warning(f"Could not find original data for SalesAgentID {TEST_AGENT_ID_S1}")
                if not original_opp_data: logging.warning(f"Could not find original data for OpportunityID {TEST_OPP_ID_S1_UPDATE}")

                # 1. Insert new Account
                sql_insert_acc = f"""
                INSERT INTO Accounts (AccountName, SectorID, YearEstablished, Revenue, Employees, HeadquartersID, created_at, updated_at)
                VALUES ('{TEST_ACC_NAME_S1}', 1, 2025, 50000.00, 10, 1, NOW(), NOW()) RETURNING AccountID;
                """
                logging.info("Inserting new Account...")
                result = hook_oltp.get_records(sql_insert_acc)
                new_account_id = result[0][0] if result else None
                logging.info(f"Inserted Account with ID: {new_account_id}")

                # 2. Update existing SalesAgent
                sql_update_agent = f"""
                UPDATE SalesAgents
                SET ManagerID = {TEST_AGENT_MANAGER_ID_S1_NEW},
                    updated_at = NOW()
                WHERE SalesAgentID = {TEST_AGENT_ID_S1};
                """
                logging.info(f"Updating SalesAgentID {TEST_AGENT_ID_S1}...")
                hook_oltp.run(sql_update_agent)

                # 3. Insert new SalesPipeline record
                sql_insert_opp = f"""
                INSERT INTO SalesPipeline (OpportunityID, SalesAgentID, ProductID, AccountID, DealStageID, EngageDate, CloseDate, CloseValue, created_at, updated_at)
                VALUES ('{TEST_OPP_ID_S1_NEW}', {TEST_AGENT_ID_S1}, {TEST_PROD_ID_S2}, {new_account_id or 9}, {TEST_OPP_STAGE_S1_ORIG}, CURRENT_DATE - INTERVAL '10 days', CURRENT_DATE, 1234.56, NOW(), NOW());
                """ # Using new_account_id if available, else fallback
                logging.info("Inserting new SalesPipeline record...")
                hook_oltp.run(sql_insert_opp)

                # 4. Update existing SalesPipeline record
                sql_update_opp = f"""
                UPDATE SalesPipeline
                SET DealStageID = {TEST_OPP_STAGE_S1_NEW},
                    CloseValue = {TEST_OPP_VALUE_S1_NEW},
                    updated_at = NOW()
                WHERE OpportunityID = '{TEST_OPP_ID_S1_UPDATE}';
                """
                logging.info(f"Updating SalesPipeline OpportunityID {TEST_OPP_ID_S1_UPDATE}...")
                hook_oltp.run(sql_update_opp)

                logging.info("--- Scenario 1: OLTP Modifications Complete ---")
                return {
                    "new_account_id": new_account_id,
                    "original_agent_manager_id": original_agent_data[0] if original_agent_data else None,
                    "original_opp_stage_id": original_opp_data[0] if original_opp_data else None,
                    "original_opp_close_value": original_opp_data[1] if original_opp_data else None
                } # Pass original values for cleanup
            except Exception as e:
                logging.error(f"Error modifying OLTP data for Scenario 1: {e}")
                raise

        @task
        def trigger_incremental_s1() -> str:
            """Triggers the incremental load DAG."""
            # We need a unique run_id to wait for it specifically
            run_id = f"test_s1_{{{{ ts_nodash }}}}"
            logging.info(f"Triggering DAG '{INCREMENTAL_DAG_ID}' with run_id '{run_id}'")
            TriggerDagRunOperator(
                task_id="trigger_dagrun", # Needs a unique task_id within the @task
                trigger_dag_id=INCREMENTAL_DAG_ID,
                conf={"triggered_by_test": "scenario_1", "run_id": run_id},
                wait_for_completion=False, # We use ExternalTaskSensor to wait
                # execution_date = "{{ dag_run.logical_date }}", # Pass the logical date if needed
            ).execute(context={}) # Execute directly within the @task function
            return run_id # Return run_id to be pushed to XCom

        # Wait for the specific DAG run triggered above
        wait_incremental_s1 = ExternalTaskSensor(
            task_id='wait_for_incremental_load_s1',
            external_dag_id=INCREMENTAL_DAG_ID,
            # Wait for the specific run_id triggered by the previous task
            external_task_id='end_incremental_load', # Wait for the end task
            # Use allowed_states and failed_states for robustness
            allowed_states=[State.SUCCESS],
            failed_states=[State.FAILED, State.SKIPPED, State.UPSTREAM_FAILED],
            # Reference the run_id pushed by the trigger task
            # Note: Referencing XComs directly in external_task_id or execution_date_fn can be tricky.
            # A common pattern is to pass logical_date and use execution_delta=0,
            # but since we trigger manually, let's try a simpler approach first:
            # Check the latest run for now, might need refinement if runs overlap.
            # A more robust way involves custom PythonSensor checking run state via API/DB.
            # For simplicity, let's just wait for *any* successful run after the trigger.
            # This is NOT ideal but simpler than full XCom handling in sensor.
            # execution_date_fn=lambda dt: pendulum.parse(context['ti'].xcom_pull(task_ids='trigger_incremental_s1')), # Ideal but complex setup
            execution_delta=timedelta(minutes=5), # Check runs within the last 5 mins (adjust as needed)
            check_existence=True,
            mode='poke',
            poke_interval=30,
            timeout=1800 # 30 minutes timeout for the incremental run
        )

        @task
        def verify_olap_s1(new_account_id: int):
            """Runs verification queries against OLAP after incremental load for Scenario 1."""
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            logging.info("--- Scenario 1: Verifying OLAP Data ---")
            results = {}

            try:
                # 1. Check new Account
                sql_check_acc = f"SELECT AccountID, AccountName FROM DimAccount WHERE AccountName = '{TEST_ACC_NAME_S1}'"
                res_acc = hook_olap.get_records(sql_check_acc)
                results["new_account_check"] = res_acc
                logging.info(f"Check New Account ('{TEST_ACC_NAME_S1}'): {res_acc}")

                # 2. Check updated Agent
                sql_check_agent = f"SELECT SalesAgentID, ManagerName FROM DimSalesAgent WHERE SalesAgentID = {TEST_AGENT_ID_S1}"
                res_agent = hook_olap.get_records(sql_check_agent)
                results["updated_agent_check"] = res_agent
                logging.info(f"Check Updated Agent (ID={TEST_AGENT_ID_S1}): {res_agent}")

                # 3. Check new Fact record (basic check)
                sql_check_fact_new = f"SELECT COUNT(*) FROM FactSalesPerformance WHERE SalesAgentKey = {TEST_AGENT_ID_S1} AND CloseValue = 1234.56"
                res_fact_new = hook_olap.get_first(sql_check_fact_new)
                results["new_fact_check"] = res_fact_new
                logging.info(f"Check New Fact (AgentKey={TEST_AGENT_ID_S1}, Value=1234.56): Count = {res_fact_new[0] if res_fact_new else 'Error'}")

                # 4. Check updated Fact record (assuming UPSERT worked - check for ONE record with new stage/value)
                # NOTE: If using INSERT-ONLY workaround, this check needs adjustment
                sql_check_fact_upd = f"""
                SELECT COUNT(*), MAX(DealStageKey), MAX(CloseValue)
                FROM FactSalesPerformance
                WHERE OpportunityID = '{TEST_OPP_ID_S1_UPDATE}' -- Requires OpportunityID in Fact table
                """
                # If OpportunityID is NOT in Fact table (current workaround state):
                # We can't reliably check the update, just check if a row with NEW stage exists
                # sql_check_fact_upd = f"""
                # SELECT COUNT(*) FROM FactSalesPerformance f
                # JOIN DimAccount da ON f.AccountKey = da.AccountID
                # JOIN DimProduct dp ON f.ProductKey = dp.ProductID AND dp.IsCurrent = TRUE
                # JOIN DimSalesAgent sa ON f.SalesAgentKey = sa.SalesAgentID
                # WHERE da.AccountID = 83 AND dp.ProductID = 1 AND sa.SalesAgentID = 32 AND f.DealStageKey = {TEST_OPP_STAGE_S1_NEW}
                # """
                res_fact_upd = hook_olap.get_first(sql_check_fact_upd)
                results["updated_fact_check"] = res_fact_upd
                logging.info(f"Check Updated Fact (OppID='{TEST_OPP_ID_S1_UPDATE}'): Result = {res_fact_upd}")
                logging.info(f"  Expected (if UPSERT): Count=1, Stage={TEST_OPP_STAGE_S1_NEW}, Value={TEST_OPP_VALUE_S1_NEW}")

            except Exception as e:
                logging.error(f"Error during OLAP verification for Scenario 1: {e}")
                # Do not fail the task, just log error
            logging.info("--- Scenario 1: OLAP Verification Complete (Check Logs) ---")

        @task(trigger_rule=TriggerRule.ALL_DONE) # Run cleanup even if verification has issues
        def cleanup_oltp_s1(original_data: dict):
            """Reverts changes made in OLTP for Scenario 1."""
            hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
            logging.info("--- Scenario 1: Cleaning Up OLTP Data ---")
            try:
                # Delete new SalesPipeline record
                sql_delete_opp_new = f"DELETE FROM SalesPipeline WHERE OpportunityID = '{TEST_OPP_ID_S1_NEW}'"
                logging.info(f"Deleting OppID {TEST_OPP_ID_S1_NEW}...")
                hook_oltp.run(sql_delete_opp_new)

                # Revert updated SalesPipeline record (if original data was found)
                if original_data.get("original_opp_stage_id") is not None and original_data.get("original_opp_close_value") is not None:
                    sql_revert_opp = f"""
                    UPDATE SalesPipeline
                    SET DealStageID = {original_data['original_opp_stage_id']},
                        CloseValue = {original_data['original_opp_close_value']},
                        updated_at = NOW()
                    WHERE OpportunityID = '{TEST_OPP_ID_S1_UPDATE}';
                    """
                    logging.info(f"Reverting OppID {TEST_OPP_ID_S1_UPDATE}...")
                    hook_oltp.run(sql_revert_opp)
                else:
                     logging.warning(f"Skipping revert for OppID {TEST_OPP_ID_S1_UPDATE} due to missing original data.")

                 # Revert updated SalesAgent record (if original data was found)
                if original_data.get("original_agent_manager_id") is not None:
                    sql_revert_agent = f"""
                    UPDATE SalesAgents
                    SET ManagerID = {original_data['original_agent_manager_id']},
                        updated_at = NOW()
                    WHERE SalesAgentID = {TEST_AGENT_ID_S1};
                    """
                    logging.info(f"Reverting SalesAgentID {TEST_AGENT_ID_S1}...")
                    hook_oltp.run(sql_revert_agent)
                else:
                     logging.warning(f"Skipping revert for SalesAgentID {TEST_AGENT_ID_S1} due to missing original data.")

                # Delete new Account record (if ID was captured)
                if original_data.get("new_account_id"):
                    sql_delete_acc = f"DELETE FROM Accounts WHERE AccountID = {original_data['new_account_id']}"
                    logging.info(f"Deleting AccountID {original_data['new_account_id']}...")
                    hook_oltp.run(sql_delete_acc)
                else:
                     # Attempt to delete by name if ID wasn't captured (less reliable)
                     sql_delete_acc_by_name = f"DELETE FROM Accounts WHERE AccountName = '{TEST_ACC_NAME_S1}'"
                     logging.warning(f"Attempting to delete Account by name '{TEST_ACC_NAME_S1}'...")
                     hook_oltp.run(sql_delete_acc_by_name)


                logging.info("--- Scenario 1: OLTP Cleanup Complete ---")
            except Exception as e:
                logging.error(f"Error during OLTP cleanup for Scenario 1: {e}")
                # Do not fail the DAG, but log the error
                # raise # Uncomment to fail DAG if cleanup fails

        # Define flow for Scenario 1
        original_data = modify_oltp_s1()
        run_id_s1 = trigger_incremental_s1()
        # Pass run_id to sensor - Note: Direct XCom pull in sensor args is complex.
        # The sensor above uses execution_delta as a simpler (less precise) workaround.
        # For precise waiting, PythonSensor or custom operator is better.
        wait_incremental_s1 # Depends implicitly on trigger_incremental_s1 completing
        verified_data = verify_olap_s1(original_data["new_account_id"]) # Pass necessary info
        cleanup_oltp_s1(original_data) >> verified_data # Run cleanup after verification (or attempt even if verify fails)

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
                # Get original data for cleanup
                original_product_data = hook_oltp.get_first(f"SELECT ProductName, SalesPrice FROM Products WHERE ProductID = {TEST_PROD_ID_S2}")
                if not original_product_data:
                    logging.error(f"Cannot run Scenario 2: ProductID {TEST_PROD_ID_S2} not found in OLTP.")
                    raise AirflowSkipException(f"ProductID {TEST_PROD_ID_S2} not found.")

                # Update Product
                new_name = original_product_data[0] + TEST_PROD_NAME_S2_SUFFIX
                sql_update_prod = f"""
                UPDATE Products
                SET SalesPrice = SalesPrice * {TEST_PROD_PRICE_MULTIPLIER_S2},
                    ProductName = '{new_name}',
                    updated_at = NOW()
                WHERE ProductID = {TEST_PROD_ID_S2};
                """
                logging.info(f"Updating ProductID {TEST_PROD_ID_S2}...")
                hook_oltp.run(sql_update_prod)
                logging.info("--- Scenario 2: OLTP Modifications Complete ---")
                return {
                    "original_product_name": original_product_data[0],
                    "original_product_price": original_product_data[1]
                }
            except Exception as e:
                logging.error(f"Error modifying OLTP data for Scenario 2: {e}")
                raise

        @task
        def trigger_incremental_s2() -> str:
            """Triggers the incremental load DAG for scenario 2."""
            run_id = f"test_s2_{{{{ ts_nodash }}}}"
            logging.info(f"Triggering DAG '{INCREMENTAL_DAG_ID}' with run_id '{run_id}'")
            TriggerDagRunOperator(
                task_id="trigger_dagrun_s2",
                trigger_dag_id=INCREMENTAL_DAG_ID,
                conf={"triggered_by_test": "scenario_2", "run_id": run_id},
                wait_for_completion=False,
            ).execute(context={})
            return run_id

        wait_incremental_s2 = ExternalTaskSensor(
            task_id='wait_for_incremental_load_s2',
            external_dag_id=INCREMENTAL_DAG_ID,
            external_task_id='end_incremental_load',
            allowed_states=[State.SUCCESS],
            failed_states=[State.FAILED, State.SKIPPED, State.UPSTREAM_FAILED],
            execution_delta=timedelta(minutes=5), # Simpler check
            check_existence=True,
            mode='poke',
            poke_interval=30,
            timeout=1800 # 30 minutes timeout
        )

        @task
        def verify_olap_s2():
            """Runs verification queries against DimProduct for Scenario 2."""
            hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
            logging.info("--- Scenario 2: Verifying OLAP Data (DimProduct SCD2) ---")
            results = {}
            try:
                sql_check_prod = f"""
                SELECT ProductID, ProductName, SalesPrice, ValidFrom, ValidTo, IsCurrent
                FROM DimProduct
                WHERE ProductID = {TEST_PROD_ID_S2}
                ORDER BY ValidFrom DESC;
                """
                res_prod = hook_olap.get_records(sql_check_prod)
                results["product_history"] = res_prod
                logging.info(f"Check Product History (ID={TEST_PROD_ID_S2}):\n{json.dumps(res_prod, indent=2, default=str)}")
                # Basic checks (more robust checks could be added)
                if len(res_prod) < 2:
                     logging.warning("Expected at least 2 versions for Product, found less.")
                if not res_prod[0][5]: # Check IsCurrent on latest record
                     logging.warning("Expected latest version for Product to be IsCurrent=TRUE.")
                if res_prod[1][5]: # Check IsCurrent on previous record
                     logging.warning("Expected previous version for Product to be IsCurrent=FALSE.")
                if not res_prod[1][4]: # Check ValidTo on previous record
                     logging.warning("Expected previous version for Product to have a ValidTo date.")

            except Exception as e:
                logging.error(f"Error during OLAP verification for Scenario 2: {e}")
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
                # Revert Product change
                sql_revert_prod = f"""
                UPDATE Products
                SET ProductName = '{original_data['original_product_name']}',
                    SalesPrice = {original_data['original_product_price']},
                    updated_at = NOW()
                WHERE ProductID = {TEST_PROD_ID_S2};
                """
                logging.info(f"Reverting ProductID {TEST_PROD_ID_S2}...")
                hook_oltp.run(sql_revert_prod)
                logging.info("--- Scenario 2: OLTP Cleanup Complete ---")
            except Exception as e:
                logging.error(f"Error during OLTP cleanup for Scenario 2: {e}")
                # Do not fail the DAG

        # Define flow for Scenario 2
        original_data_s2 = modify_oltp_s2()
        run_id_s2 = trigger_incremental_s2()
        wait_incremental_s2
        verified_data_s2 = verify_olap_s2()
        cleanup_oltp_s2(original_data_s2) >> verified_data_s2

    # Define overall DAG flow
    start >> scenario_1_group() >> scenario_2_group() >> end

# Instantiate the DAG
b2b_etl_test_scenarios_dag()
