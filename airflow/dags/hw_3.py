from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import logging
from crud.functions import parse_scripts
from crud.config import SQL_SCRIPTS_PATH

POSTGRES_CONN_ID = "postgres_conn_id"
FUNCTIONS_LIST = parse_scripts(SQL_SCRIPTS_PATH)  

def execute_function(func_name: str):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    try:
        logging.info(f"Starting execution of {func_name}")
        conn = hook.get_conn()
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT {func_name}();")
            conn.commit()
        logging.info(f"Successfully executed {func_name}")
        
    except Exception as e:
        logging.error(f"Error executing {func_name}: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def execute_functions_list():
    for func_name in FUNCTIONS_LIST:
        execute_function(func_name)

with DAG(
    dag_id="hw_3",
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=False,
    tags=["postgres", 'mizumizu'],
) as dag:

    start_task = EmptyOperator(task_id='start')

    execute_task = PythonOperator(
        task_id='execute_functions',
        python_callable=execute_functions_list
    )

    end_task = EmptyOperator(task_id='end')

    start_task >> execute_task >> end_task