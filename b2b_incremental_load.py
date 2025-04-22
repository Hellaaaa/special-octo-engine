# /path/to/your/airflow/dags/b2b_incremental_load_dag.py
import pendulum
import logging
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowNotFoundException

# --- Configuration ---
OLTP_CONN_ID = "b2b_sales"
OLAP_CONN_ID = "b2b_sales_olap"
DEFAULT_START_ID = 0 # Початковий ID для першого запуску
WATERMARK_VAR_PREFIX = "b2b_incremental_maxid_" # Префікс для Airflow Variables

# --- Helper: Watermark Management (ID-based) ---
def get_max_id_watermark(table_key: str) -> int:
    """Отримує останній максимальний ID для таблиці з Airflow Variable."""
    var_name = f"{WATERMARK_VAR_PREFIX}{table_key}"
    try:
        watermark = Variable.get(var_name, default_var=str(DEFAULT_START_ID))
        logging.info(f"Отримано водяний знак max_id для {table_key}: {watermark}")
        return int(watermark)
    except AirflowNotFoundException:
        logging.info(f"Водяний знак {var_name} не знайдено. Використовується {DEFAULT_START_ID}")
        return DEFAULT_START_ID
    except ValueError:
         logging.warning(f"Неправильний формат водяного знаку '{watermark}' для {var_name}. Використовується {DEFAULT_START_ID}")
         return DEFAULT_START_ID
    except Exception as e:
        logging.error(f"Помилка при отриманні водяного знаку {var_name}: {e}. Використовується {DEFAULT_START_ID}")
        return DEFAULT_START_ID

def set_max_id_watermark(table_key: str, value: int):
    """Встановлює новий максимальний ID для таблиці в Airflow Variable."""
    var_name = f"{WATERMARK_VAR_PREFIX}{table_key}"
    try:
        Variable.set(var_name, str(value))
        logging.info(f"Встановлено новий водяний знак max_id для {table_key}: {value}")
    except Exception as e:
        logging.error(f"Не вдалося встановити водяний знак {var_name} зі значенням {value}: {e}")
        # raise # Розгляньте можливість підняти виняток

# --- DAG Definition ---
@dag(
    dag_id='b2b_incremental_load',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule='@daily', # Або інший інтервал
    catchup=False,
    # default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)}, # Закоментовано для тестування
    default_args={'retry_delay': timedelta(minutes=5)}, # Залишаємо retry_delay на випадок ручного retry
    tags=['b2b_sales', 'incremental_load', 'refined', 'id_watermark'],
    doc_md="""
    ### B2B Sales Incremental Load DAG (ID Watermark)

    Завантажує **НОВІ** дані з OLTP до OLAP з моменту останнього успішного запуску,
    використовуючи максимальний ID первинного ключа та Airflow Variables для водяних знаків.
    **ВАЖЛИВО: Цей метод НЕ відстежує ОНОВЛЕННЯ існуючих записів.**
    Обробляє нові записи для вимірів (як SCD Type 1) та фактів.

    **CDC/Watermarking (ID-based):**
    - Зчитує останній оброблений `MAX(ID)` з Airflow Variable (`b2b_incremental_maxid_[table_key]`).
    - Запитує OLTP таблиці для записів, де `ID` > останнього `MAX(ID)`.
    - Оновлює водяний знак `MAX(ID)` після успішної обробки.

    **Завдання:**
    1. Завантажити нові записи DimAccount (SCD Type 1 - Insert/Replace).
    2. Завантажити нові записи DimProduct (SCD Type 1 - Insert/Replace, оскільки оновлення не відстежуються).
    3. Завантажити нові записи DimSalesAgent (SCD Type 1 - Insert/Replace).
    4. Завантажити нові записи FactSalesPerformance (Insert, з пошуком ключів вимірів).
    """
)
def b2b_incremental_load_dag():

    start = EmptyOperator(task_id='start_incremental_load')
    end = EmptyOperator(task_id='end_incremental_load')

    # --- Task 1: Dimension - Account (Incremental Load - New Records Only) ---
    @task
    def load_new_dim_account(**context):
        table_key = 'accounts' # Ключ для водяного знаку
        last_max_id = get_max_id_watermark(table_key)
        logging.info(f"Перевірка нових записів Account з ID > {last_max_id}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
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
        WHERE a.AccountID > %s -- Вибираємо тільки нові записи
        ORDER BY a.AccountID; -- Важливо для визначення нового max_id
        """
        new_data = hook_oltp.get_records(sql_extract, parameters=(last_max_id,))

        if not new_data:
            logging.info("Нових записів Account не виявлено.")
            # Водяний знак не оновлюємо, оскільки max_id не змінився
            return

        logging.info(f"Знайдено {len(new_data)} нових акаунтів.")
        olap_data = [
            (row[0], row[1], row[2], row[3], row[4]) for row in new_data
        ]
        current_max_id = new_data[-1][0] # Останній ID в отриманому списку

        # Upsert (фактично Insert/Replace для нових ID) в OLAP
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        try:
            # Використовуємо replace=True, щоб перезаписати, якщо ID якимось чином вже існує (малоймовірно)
            hook_olap.insert_rows(
                table="DimAccount",
                rows=olap_data,
                target_fields=["AccountID", "AccountName", "Sector", "RevenueRange", "ParentAccountID"],
                commit_every=1000,
                replace=True,
                replace_index="AccountID"
            )
            logging.info(f"Вставлено/Оновлено {len(olap_data)} записів у DimAccount.")
            set_max_id_watermark(table_key, current_max_id) # Оновлюємо водяний знак
        except Exception as e:
            logging.error(f"Помилка під час завантаження в DimAccount: {e}")
            raise

    # --- Task 2: Dimension - Product (Incremental Load - New Records Only) ---
    @task
    def load_new_dim_product(**context):
        table_key = 'products'
        last_max_id = get_max_id_watermark(table_key)
        today_date = pendulum.now('UTC').to_date_string()
        logging.info(f"Перевірка нових записів Product з ID > {last_max_id}")

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
        WHERE p.ProductID > %s -- Тільки нові
        ORDER BY p.ProductID;
        """
        new_data = hook_oltp.get_records(sql_extract, parameters=(last_max_id,))

        if not new_data:
            logging.info("Нових записів Product не виявлено.")
            return

        logging.info(f"Знайдено {len(new_data)} нових продуктів.")
        # Оскільки ми не відстежуємо оновлення, логіка SCD2 тут не застосовується.
        # Просто вставляємо нові записи як поточні.
        olap_data = [
            (row[0], row[1], row[2], row[3], row[4], today_date, None, True) # ValidFrom=today, IsCurrent=True
            for row in new_data
        ]
        current_max_id = new_data[-1][0]

        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        try:
            # Використовуємо replace=True на випадок, якщо запис з таким ID вже існує
            # (наприклад, після збою попереднього запуску)
            hook_olap.insert_rows(
                table="DimProduct",
                rows=olap_data,
                target_fields=["ProductID", "ProductName", "SeriesName", "Price", "PriceRange", "ValidFrom", "ValidTo", "IsCurrent"],
                commit_every=1000,
                replace=True,
                replace_index="ProductID"
            )
            logging.info(f"Вставлено/Оновлено {len(olap_data)} записів у DimProduct.")
            set_max_id_watermark(table_key, current_max_id)
        except Exception as e:
            logging.error(f"Помилка під час завантаження в DimProduct: {e}")
            raise

    # --- Task 3: Dimension - SalesAgent (Incremental Load - New Records Only) ---
    @task
    def load_new_dim_sales_agent(**context):
        table_key = 'salesagents'
        last_max_id = get_max_id_watermark(table_key)
        logging.info(f"Перевірка нових записів SalesAgent з ID > {last_max_id}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        sql_extract = """
        SELECT
            sa.SalesAgentID, sa.SalesAgentName, sm.ManagerName, l.LocationName AS Region
        FROM SalesAgents sa
        LEFT JOIN SalesManagers sm ON sa.ManagerID = sm.ManagerID
        LEFT JOIN Locations l ON sa.RegionalOfficeID = l.LocationID
        WHERE sa.SalesAgentID > %s -- Тільки нові
        ORDER BY sa.SalesAgentID;
        """
        new_data = hook_oltp.get_records(sql_extract, parameters=(last_max_id,))

        if not new_data:
            logging.info("Нових записів SalesAgent не виявлено.")
            return

        logging.info(f"Знайдено {len(new_data)} нових агентів.")
        olap_data = [
            (row[0], row[1], row[2], row[3]) for row in new_data
        ]
        current_max_id = new_data[-1][0]

        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        try:
            hook_olap.insert_rows(
                table="DimSalesAgent",
                rows=olap_data,
                target_fields=["SalesAgentID", "SalesAgentName", "ManagerName", "Region"],
                commit_every=1000,
                replace=True,
                replace_index="SalesAgentID"
            )
            logging.info(f"Вставлено/Оновлено {len(olap_data)} записів у DimSalesAgent.")
            set_max_id_watermark(table_key, current_max_id)
        except Exception as e:
            logging.error(f"Помилка під час завантаження в DimSalesAgent: {e}")
            raise

    # --- Task 4: Fact Table - SalesPerformance (Incremental Load - New Records Only) ---
    @task
    def load_new_fact_sales_performance(**context):
        table_key = 'salespipeline' # Використовуємо OpportunityID як ключ для SalesPipeline
        last_max_id = get_max_id_watermark(table_key)
        logging.info(f"Перевірка нових записів SalesPipeline з OpportunityID > {last_max_id}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)

        # Вибираємо тільки нові записи з OLTP
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
            COALESCE(sp.CloseDate, sp.EngageDate) as EventDate -- Дата для пошуку ключів
        FROM SalesPipeline sp
        LEFT JOIN DealStages ds ON sp.DealStageID = ds.DealStageID
        WHERE sp.OpportunityID > %s -- Тільки нові OpportunityID
          AND sp.AccountID IS NOT NULL -- Фільтруємо NULL ключі
          AND sp.ProductID IS NOT NULL
          AND sp.SalesAgentID IS NOT NULL
          AND sp.DealStageID IS NOT NULL
          AND COALESCE(sp.CloseDate, sp.EngageDate) IS NOT NULL -- Потрібна дата для DateKey
        ORDER BY sp.OpportunityID; -- Важливо для визначення нового max_id
        """
        new_data = hook_oltp.get_records(sql_extract, parameters=(last_max_id,))

        if not new_data:
            logging.info("Нових записів SalesPipeline не виявлено.")
            return

        logging.info(f"Знайдено {len(new_data)} нових записів SalesPipeline.")
        current_max_id = new_data[-1][0] # Новий максимальний OpportunityID

        # Підготовка даних для OLAP з пошуком ключів вимірів
        olap_data_to_insert = []
        missing_keys = False
        for row in new_data:
            opportunity_id = row[0]
            sales_agent_id = row[1]
            product_id = row[2]
            account_id = row[3]
            deal_stage_id = row[4]
            event_date = row[10]
            close_value = row[7]
            duration_days = row[8]
            expected_success_rate = row[9]

            # Пошук ключів вимірів (залишається таким же)
            date_key_sql = "SELECT DateKey FROM DimDate WHERE Date = %s"
            date_key_result = hook_olap.get_first(date_key_sql, parameters=(event_date,))
            date_key = date_key_result[0] if date_key_result else None

            account_key_sql = "SELECT AccountID FROM DimAccount WHERE AccountID = %s"
            account_key_result = hook_olap.get_first(account_key_sql, parameters=(account_id,))
            account_key = account_key_result[0] if account_key_result else None

            product_key_sql = "SELECT ProductID FROM DimProduct WHERE ProductID = %s AND IsCurrent = TRUE"
            product_key_result = hook_olap.get_first(product_key_sql, parameters=(product_id,))
            product_key = product_key_result[0] if product_key_result else None

            agent_key_sql = "SELECT SalesAgentID FROM DimSalesAgent WHERE SalesAgentID = %s"
            agent_key_result = hook_olap.get_first(agent_key_sql, parameters=(sales_agent_id,))
            agent_key = agent_key_result[0] if agent_key_result else None

            stage_key_sql = "SELECT StageID FROM DimDealStage WHERE StageID = %s"
            stage_key_result = hook_olap.get_first(stage_key_sql, parameters=(deal_stage_id,))
            stage_key = stage_key_result[0] if stage_key_result else None

            if all([date_key, account_key, product_key, agent_key, stage_key]):
                # Додаємо OpportunityID, якщо він є в цільовій таблиці
                # Припускаємо, що він потрібен для унікальності або аналізу
                olap_data_to_insert.append((
                    opportunity_id, # Додаємо OpportunityID
                    date_key,
                    account_key,
                    product_key,
                    agent_key,
                    stage_key,
                    close_value,
                    duration_days,
                    expected_success_rate
                ))
            else:
                logging.warning(f"Пропущено OpportunityID {opportunity_id} через відсутні ключі вимірів "
                                f"(Date: {date_key is not None}, Acc: {account_key is not None}, Prod: {product_key is not None}, "
                                f"Agent: {agent_key is not None}, Stage: {stage_key is not None}) для дати події {event_date}")
                missing_keys = True

        if not olap_data_to_insert:
             logging.warning("Немає даних для завантаження в FactSalesPerformance після пошуку ключів.")
             # Оновлюємо водяний знак, оскільки ми обробили всі ID до current_max_id
             set_max_id_watermark(table_key, current_max_id)
             return

        logging.info(f"Підготовлено {len(olap_data_to_insert)} нових записів для INSERT в FactSalesPerformance.")

        # Виконуємо INSERT в OLAP (без UPSERT, оскільки відстежуємо тільки нові OpportunityID)
        try:
            # Переконуємося, що цільові поля включають OpportunityID, якщо він є в таблиці
            target_fields = [
                "OpportunityID", "DateKey", "AccountKey", "ProductKey", "SalesAgentKey",
                "DealStageKey", "CloseValue", "DurationDays", "ExpectedSuccessRate"
            ]
            hook_olap.insert_rows(
                table="FactSalesPerformance",
                rows=olap_data_to_insert,
                target_fields=target_fields,
                commit_every=1000
            )
            logging.info(f"Вставлено {len(olap_data_to_insert)} нових записів у FactSalesPerformance.")
            set_max_id_watermark(table_key, current_max_id) # Оновлюємо водяний знак
            if missing_keys:
                 logging.warning("Деякі записи були пропущені через відсутні ключі вимірів.")

        except Exception as e:
            logging.error(f"Помилка під час INSERT в FactSalesPerformance: {e}")
            raise

    # --- Task Dependencies ---
    task_load_new_account = load_new_dim_account()
    task_load_new_product = load_new_dim_product()
    task_load_new_agent = load_new_dim_sales_agent()
    task_load_new_facts = load_new_fact_sales_performance()

    # Завантаження нових вимірів можуть йти паралельно
    start >> [task_load_new_account, task_load_new_product, task_load_new_agent]

    # Завантаження фактів залежить від завантаження нових вимірів
    [task_load_new_account, task_load_new_product, task_load_new_agent] >> task_load_new_facts

    task_load_new_facts >> end

# Instantiate the DAG
b2b_incremental_load_dag()
