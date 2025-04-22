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
DEFAULT_START_TIME = '1970-01-01T00:00:00+00:00' # Початкова дата для першого запуску
WATERMARK_VAR_PREFIX = "b2b_incremental_watermark_" # Префікс для Airflow Variables

# --- Helper: Watermark Management ---
def get_watermark(table_name: str) -> str:
    """Отримує останній водяний знак (timestamp) для таблиці з Airflow Variable."""
    var_name = f"{WATERMARK_VAR_PREFIX}{table_name}"
    try:
        watermark = Variable.get(var_name, default_var=DEFAULT_START_TIME)
        logging.info(f"Отримано водяний знак для {table_name}: {watermark}")
        # Перевірка формату (опціонально, але корисно)
        try:
            pendulum.parse(watermark)
        except Exception:
            logging.warning(f"Неправильний формат водяного знаку '{watermark}' для {var_name}. Використовується {DEFAULT_START_TIME}")
            return DEFAULT_START_TIME
        return watermark
    except AirflowNotFoundException:
        logging.info(f"Водяний знак {var_name} не знайдено. Використовується {DEFAULT_START_TIME}")
        return DEFAULT_START_TIME
    except Exception as e:
        logging.error(f"Помилка при отриманні водяного знаку {var_name}: {e}. Використовується {DEFAULT_START_TIME}")
        return DEFAULT_START_TIME

def set_watermark(table_name: str, value: str):
    """Встановлює новий водяний знак (timestamp) для таблиці в Airflow Variable."""
    var_name = f"{WATERMARK_VAR_PREFIX}{table_name}"
    try:
        # Перевірка формату перед збереженням
        pendulum.parse(value)
        Variable.set(var_name, value)
        logging.info(f"Встановлено новий водяний знак для {table_name}: {value}")
    except Exception as e:
        logging.error(f"Не вдалося встановити водяний знак {var_name} зі значенням {value}: {e}")
        # Розгляньте можливість підняти виняток, якщо збереження водяного знаку критичне
        # raise

# --- DAG Definition ---
@dag(
    dag_id='b2b_incremental_load',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule='@daily', # Або інший інтервал
    catchup=False,
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},
    tags=['b2b_sales', 'incremental_load', 'refined'],
    doc_md="""
    ### B2B Sales Incremental Load DAG (Refined)

    Завантажує нові та оновлені дані з OLTP до OLAP з моменту останнього успішного запуску,
    використовуючи мітки часу `updated_at` та Airflow Variables для водяних знаків.
    Обробляє SCD Type 2 для DimProduct та SCD Type 1 для інших вимірів.
    Виконує UPSERT для таблиці фактів на основі OpportunityID (потребує змін у схемі).

    **CDC/Watermarking:**
    - Зчитує останній успішно оброблений `updated_at` з Airflow Variable (`b2b_incremental_watermark_[table_name]`).
    - Запитує OLTP таблиці для записів, де `updated_at` > останнього водяного знаку і <= поточного часу виконання.
    - Оновлює водяний знак після успішної обробки.

    **Завдання:**
    1. Оновити DimAccount (SCD Type 1 - Upsert).
    2. Оновити DimProduct (SCD Type 2 - Expire/Insert).
    3. Оновити DimSalesAgent (SCD Type 1 - Upsert).
    4. Завантажити/Оновити FactSalesPerformance (Upsert, з пошуком ключів вимірів).
    """
)
def b2b_incremental_load_dag():

    start = EmptyOperator(task_id='start_incremental_load')
    end = EmptyOperator(task_id='end_incremental_load')

    # --- Task 1: Dimension - Account (Incremental Update - SCD Type 1: Overwrite) ---
    @task
    def update_dim_account(**context):
        table_name = 'accounts' # Назва вихідної таблиці для водяного знаку
        start_time = get_watermark(table_name)
        end_time = pendulum.now('UTC').isoformat() # Використовуємо поточний час як кінець інтервалу
        logging.info(f"Перевірка оновлень Account між {start_time} та {end_time}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        # Припускаємо, що Accounts має updated_at.
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
        changed_data = hook_oltp.get_records(sql_extract, parameters=(start_time, end_time))

        if not changed_data:
            logging.info("Змін в Account не виявлено.")
            set_watermark(table_name, end_time) # Оновлюємо водяний знак, навіть якщо змін не було
            return

        logging.info(f"Знайдено {len(changed_data)} змінених/нових акаунтів.")
        olap_data = [
            (row[0], row[1], row[2], row[3], row[4]) for row in changed_data
        ]

        # Upsert в OLAP
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        try:
            hook_olap.insert_rows(
                table="DimAccount",
                rows=olap_data,
                target_fields=["AccountID", "AccountName", "Sector", "RevenueRange", "ParentAccountID"],
                commit_every=1000, # Розмір пакету для коміту
                replace=True,
                replace_index="AccountID" # Ключ для перевірки конфлікту
            )
            logging.info(f"Виконано Upsert для {len(olap_data)} записів у DimAccount.")
            set_watermark(table_name, end_time) # Оновлюємо водяний знак після успішної обробки
        except Exception as e:
            logging.error(f"Помилка під час Upsert в DimAccount: {e}")
            raise # Перекидаємо виняток, щоб завдання зазнало невдачі і водяний знак не оновився


    # --- Task 2: Dimension - Product (Incremental Update - SCD Type 2: Expire/Insert) ---
    @task
    def update_dim_product(**context):
        table_name = 'products'
        start_time = get_watermark(table_name)
        end_time = pendulum.now('UTC') # Використовуємо поточний час
        end_time_iso = end_time.isoformat()
        today_date = end_time.to_date_string()
        previous_day = end_time.subtract(days=1).to_date_string()
        logging.info(f"Перевірка оновлень Product між {start_time} та {end_time_iso}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        # Припускаємо, що Products має updated_at
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
        changed_data = hook_oltp.get_records(sql_extract, parameters=(start_time, end_time_iso))

        if not changed_data:
            logging.info("Змін в Product не виявлено.")
            set_watermark(table_name, end_time_iso)
            return

        logging.info(f"Знайдено {len(changed_data)} змінених/нових продуктів для обробки SCD Type 2.")
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)
        processed_count = 0

        try:
            with hook_olap.get_conn() as conn:
                with conn.cursor() as cur:
                    for row in changed_data:
                        product_id = row[0]
                        new_name = row[1]
                        new_series = row[2]
                        new_price = row[3]
                        new_price_range = row[4]

                        # Перевіряємо поточний активний запис для цього ProductID
                        cur.execute("""
                            SELECT ProductName, SeriesName, Price, PriceRange
                            FROM DimProduct
                            WHERE ProductID = %s AND IsCurrent = TRUE;
                        """, (product_id,))
                        current_active_product = cur.fetchone()

                        if current_active_product:
                            # Продукт існує, перевіряємо, чи змінилися атрибути
                            old_name, old_series, old_price, old_range = current_active_product
                            if (new_name != old_name or new_series != old_series or
                                new_price != old_price or new_price_range != old_range):

                                logging.info(f"SCD Type 2: Зміна виявлена для ProductID: {product_id}. Оновлення запису.")
                                # 1. Закриваємо старий запис
                                logging.info(f" Закриття старого запису (ProductID: {product_id}), встановлення ValidTo={previous_day}")
                                cur.execute("""
                                    UPDATE DimProduct
                                    SET ValidTo = %s, IsCurrent = FALSE
                                    WHERE ProductID = %s AND IsCurrent = TRUE;
                                """, (previous_day, product_id))

                                # 2. Вставляємо новий запис
                                logging.info(f" Вставка нового поточного запису (ProductID: {product_id}) з ValidFrom={today_date}")
                                cur.execute("""
                                     INSERT INTO DimProduct
                                         (ProductID, ProductName, SeriesName, Price, PriceRange, ValidFrom, ValidTo, IsCurrent)
                                     VALUES (%s, %s, %s, %s, %s, %s, NULL, TRUE);
                                 """, (product_id, new_name, new_series, new_price, new_price_range, today_date))
                                processed_count += 1
                            else:
                                # Атрибути не змінилися (можливо, оновився лише updated_at)
                                logging.info(f"Несуттєва зміна для існуючого ProductID: {product_id}")
                        else:
                            # Продукт не існує або немає активного запису - вставляємо як новий
                            logging.info(f"SCD Type 2: Вставка нового ProductID: {product_id}")
                            cur.execute("""
                                 INSERT INTO DimProduct
                                     (ProductID, ProductName, SeriesName, Price, PriceRange, ValidFrom, ValidTo, IsCurrent)
                                 VALUES (%s, %s, %s, %s, %s, %s, NULL, TRUE);
                             """, (product_id, new_name, new_series, new_price, new_price_range, today_date))
                            processed_count += 1

                conn.commit() # Коміт транзакції після обробки всіх продуктів у пакеті
            logging.info(f"Обробка SCD Type 2 для DimProduct завершена. Оброблено записів: {processed_count}")
            set_watermark(table_name, end_time_iso) # Оновлюємо водяний знак
        except Exception as e:
            logging.error(f"Помилка під час обробки SCD Type 2 для DimProduct: {e}")
            raise


    # --- Task 3: Dimension - SalesAgent (Incremental Update - SCD Type 1: Overwrite) ---
    @task
    def update_dim_sales_agent(**context):
        table_name = 'salesagents'
        start_time = get_watermark(table_name)
        end_time = pendulum.now('UTC').isoformat()
        logging.info(f"Перевірка оновлень SalesAgent між {start_time} та {end_time}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        # Припускаємо, що SalesAgents має updated_at
        sql_extract = """
        SELECT
            sa.SalesAgentID, sa.SalesAgentName, sm.ManagerName, l.LocationName AS Region
        FROM SalesAgents sa
        LEFT JOIN SalesManagers sm ON sa.ManagerID = sm.ManagerID
        LEFT JOIN Locations l ON sa.RegionalOfficeID = l.LocationID
        WHERE sa.updated_at > %s AND sa.updated_at <= %s;
        """
        changed_data = hook_oltp.get_records(sql_extract, parameters=(start_time, end_time))

        if not changed_data:
            logging.info("Змін в SalesAgent не виявлено.")
            set_watermark(table_name, end_time)
            return

        logging.info(f"Знайдено {len(changed_data)} змінених/нових агентів.")
        olap_data = [
            (row[0], row[1], row[2], row[3]) for row in changed_data
        ]

        # Upsert в OLAP
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
            logging.info(f"Виконано Upsert для {len(olap_data)} записів у DimSalesAgent.")
            set_watermark(table_name, end_time)
        except Exception as e:
            logging.error(f"Помилка під час Upsert в DimSalesAgent: {e}")
            raise

    # --- Task 4: Fact Table - SalesPerformance (Incremental Load/Update - UPSERT) ---
    @task
    def update_fact_sales_performance(**context):
        table_name = 'salespipeline'
        start_time = get_watermark(table_name)
        end_time = pendulum.now('UTC').isoformat()
        logging.info(f"Перевірка оновлень SalesPipeline між {start_time} та {end_time}")

        hook_oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        hook_olap = PostgresHook(postgres_conn_id=OLAP_CONN_ID)

        # Вибираємо змінені/нові записи з OLTP
        # Важливо: Потрібна колонка updated_at в SalesPipeline
        sql_extract = """
        SELECT
            sp.OpportunityID, sp.SalesAgentID, sp.ProductID, sp.AccountID, sp.DealStageID,
            sp.EngageDate, sp.CloseDate, sp.CloseValue,
            CASE WHEN sp.CloseDate IS NOT NULL AND sp.EngageDate IS NOT NULL
                 THEN sp.CloseDate - sp.EngageDate ELSE NULL END AS DurationDays,
            CASE ds.StageName -- Example logic
               WHEN 'Won' THEN 100.0 WHEN 'Lost' THEN 0.0
               WHEN 'Engaging' THEN 75.0 WHEN 'Prospecting' THEN 25.0
               ELSE 10.0 END AS ExpectedSuccessRate,
            -- Використовуємо дату події (EngageDate або CloseDate) для пошуку ключів вимірів
            COALESCE(sp.CloseDate, sp.EngageDate) as EventDate
        FROM SalesPipeline sp
        LEFT JOIN DealStages ds ON sp.DealStageID = ds.DealStageID
        WHERE sp.updated_at > %s AND sp.updated_at <= %s
          AND sp.AccountID IS NOT NULL -- Фільтруємо NULL ключі
          AND sp.ProductID IS NOT NULL
          AND sp.SalesAgentID IS NOT NULL
          AND sp.DealStageID IS NOT NULL
          AND COALESCE(sp.CloseDate, sp.EngageDate) IS NOT NULL; -- Потрібна дата для DateKey
        """
        changed_data = hook_oltp.get_records(sql_extract, parameters=(start_time, end_time))

        if not changed_data:
            logging.info("Змін в SalesPipeline не виявлено.")
            set_watermark(table_name, end_time)
            return

        logging.info(f"Знайдено {len(changed_data)} змінених/нових записів SalesPipeline.")

        # Підготовка даних для OLAP з пошуком ключів вимірів
        olap_data_to_upsert = []
        missing_keys = False
        for row in changed_data:
            opportunity_id = row[0]
            sales_agent_id = row[1]
            product_id = row[2]
            account_id = row[3]
            deal_stage_id = row[4]
            event_date = row[10] # Дата події для пошуку ключів
            close_value = row[7]
            duration_days = row[8]
            expected_success_rate = row[9]

            # 1. Пошук DateKey
            date_key_sql = "SELECT DateKey FROM DimDate WHERE Date = %s"
            date_key_result = hook_olap.get_first(date_key_sql, parameters=(event_date,))
            date_key = date_key_result[0] if date_key_result else None

            # 2. Пошук AccountKey (SCD Type 1 - завжди поточний)
            account_key_sql = "SELECT AccountID FROM DimAccount WHERE AccountID = %s"
            account_key_result = hook_olap.get_first(account_key_sql, parameters=(account_id,))
            account_key = account_key_result[0] if account_key_result else None

            # 3. Пошук ProductKey (SCD Type 2 - потрібен ключ, активний на дату події)
            # У нашому випадку (без окремого сурогатного ключа), беремо поточний активний ключ
            product_key_sql = "SELECT ProductID FROM DimProduct WHERE ProductID = %s AND IsCurrent = TRUE"
            # Або, якщо потрібен ключ, активний на event_date:
            # product_key_sql = "SELECT ProductID FROM DimProduct WHERE ProductID = %s AND %s >= ValidFrom AND (%s < ValidTo OR ValidTo IS NULL)"
            product_key_result = hook_olap.get_first(product_key_sql, parameters=(product_id,)) #, event_date, event_date))
            product_key = product_key_result[0] if product_key_result else None

            # 4. Пошук SalesAgentKey (SCD Type 1)
            agent_key_sql = "SELECT SalesAgentID FROM DimSalesAgent WHERE SalesAgentID = %s"
            agent_key_result = hook_olap.get_first(agent_key_sql, parameters=(sales_agent_id,))
            agent_key = agent_key_result[0] if agent_key_result else None

            # 5. Пошук DealStageKey
            stage_key_sql = "SELECT StageID FROM DimDealStage WHERE StageID = %s"
            stage_key_result = hook_olap.get_first(stage_key_sql, parameters=(deal_stage_id,))
            stage_key = stage_key_result[0] if stage_key_result else None

            # Перевірка, чи всі ключі знайдено
            if all([date_key, account_key, product_key, agent_key, stage_key]):
                olap_data_to_upsert.append((
                    opportunity_id, # Бізнес-ключ для UPSERT
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
                missing_keys = True # Позначаємо, що були пропуски

        if not olap_data_to_upsert:
             logging.warning("Немає даних для завантаження в FactSalesPerformance після пошуку ключів.")
             # Все одно оновлюємо водяний знак, оскільки ми обробили інтервал часу
             set_watermark(table_name, end_time)
             return

        logging.info(f"Підготовлено {len(olap_data_to_upsert)} записів для UPSERT в FactSalesPerformance.")

        # Виконуємо UPSERT в OLAP
        # Потрібно додати OpportunityID до FactSalesPerformance та створити UNIQUE constraint на ньому
        try:
            with hook_olap.get_conn() as conn:
                with conn.cursor() as cur:
                    upsert_sql = """
                    INSERT INTO FactSalesPerformance (
                        OpportunityID, DateKey, AccountKey, ProductKey, SalesAgentKey,
                        DealStageKey, CloseValue, DurationDays, ExpectedSuccessRate
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (OpportunityID) DO UPDATE SET -- Потребує UNIQUE constraint на OpportunityID
                        DateKey = EXCLUDED.DateKey,
                        AccountKey = EXCLUDED.AccountKey,
                        ProductKey = EXCLUDED.ProductKey,
                        SalesAgentKey = EXCLUDED.SalesAgentKey,
                        DealStageKey = EXCLUDED.DealStageKey,
                        CloseValue = EXCLUDED.CloseValue,
                        DurationDays = EXCLUDED.DurationDays,
                        ExpectedSuccessRate = EXCLUDED.ExpectedSuccessRate;
                    """
                    # Використовуємо execute_values для кращої продуктивності, якщо доступно, або executemany
                    # psycopg2.extras.execute_values(cur, upsert_sql, olap_data_to_upsert, page_size=1000)
                    # Або стандартний executemany:
                    cur.executemany(upsert_sql, olap_data_to_upsert)
                conn.commit()
            logging.info(f"Виконано UPSERT для {len(olap_data_to_upsert)} записів у FactSalesPerformance.")
            set_watermark(table_name, end_time) # Оновлюємо водяний знак
            if missing_keys:
                 logging.warning("Деякі записи були пропущені через відсутні ключі вимірів.")

        except Exception as e:
            logging.error(f"Помилка під час UPSERT в FactSalesPerformance: {e}")
            raise


    # --- Task Dependencies ---
    task_update_account = update_dim_account()
    task_update_product = update_dim_product()
    task_update_agent = update_dim_sales_agent()
    task_update_facts = update_fact_sales_performance()

    # Оновлення вимірів можуть йти паралельно
    start >> [task_update_account, task_update_product, task_update_agent]

    # Оновлення фактів залежить від оновлення всіх вимірів (для правильного пошуку ключів)
    [task_update_account, task_update_product, task_update_agent] >> task_update_facts

    task_update_facts >> end

# Instantiate the DAG
b2b_incremental_load_dag()

