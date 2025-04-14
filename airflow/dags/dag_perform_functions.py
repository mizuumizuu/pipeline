"""
This DAG performs functions that are present in PostgreSQL Database
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from crud.functions import parse_scripts
from crud.config import SQL_SCRIPTS_PATH
import logging

FUNCTIONS_LIST = parse_scripts(SQL_SCRIPTS_PATH)
POSTGRES_CONN_ID = "postgres_conn_id"

def perform_functions():
    import psycopg2 as pg
    
    logging.info('Connecting to PostgreSQL Database')
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.connect(postgres_hook.get_uri())
    logging.info('Connected to PostgreSQL Database!')

    for function_name in FUNCTIONS_LIST:
        logging.info(f'Executing function: {function_name}')
        cursor = conn.cursor()
        cursor.callproc(function_name)
        conn.commit()
        logging.info(f'Successfully executed function: {function_name}')
    
    logging.info('Closing connection')
    cursor.close() if cursor else None
    conn.close() if conn else None
    
    
with DAG(
    dag_id="dag_perform_functions",
    description=__doc__,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["example", "postgres"],
    max_active_runs=1,
) as dag:

    start_task = EmptyOperator(
        task_id='start',
    )

    end_task = EmptyOperator(
        task_id='end',
    )
    
    perform_functions_task = PythonOperator(
        task_id='perform_functions',
        python_callable=perform_functions,
    )


    start_task >> perform_functions_task >> end_task