from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import logging
from pathlib import Path
from crud.functions import parse_scripts
from crud.config import SQL_SCRIPTS_PATH

POSTGRES_CONN_ID = "postgres_conn_id"

def sanitize_function_name(filename: str) -> str:
    return filename.split("__")[-1].replace(".sql", "")

FUNCTIONS_LIST = [sanitize_function_name(f.name) for f in Path(SQL_SCRIPTS_PATH).glob("*.sql")]

def validate_function_name(func_name: str):
    if not func_name.startswith("fn_"):
        raise ValueError(f"Invalid function name: {func_name}")
    if not func_name.islower():
        raise ValueError(f"Function name must be lowercase: {func_name}")

def execute_function(func_name: str):
    validate_function_name(func_name)
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = None
    try:
        conn = hook.get_conn()
        with conn.cursor() as cursor:
            logging.info(f"Executing: SELECT {func_name}()")
            cursor.execute(f"SELECT {func_name}();")
            conn.commit()
        logging.info(f"Успешно выполнено: {func_name}")
    except Exception as e:
        logging.error(f"Ошибка выполнения {func_name}: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def execute_all_functions():
    for func_name in FUNCTIONS_LIST:
        try:
            execute_function(func_name)
        except Exception as e:
            logging.error(f"Прерывание выполнения из-за ошибки в {func_name}")
            raise

with DAG(
    dag_id="hw_5",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["postgres"],
) as dag:

    start = EmptyOperator(task_id='start')
    
    execute_functions = PythonOperator(
        task_id='execute_functions',
        python_callable=execute_all_functions
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> execute_functions >> end