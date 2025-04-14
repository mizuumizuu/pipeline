from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

POSTGRES_CONN_ID = "postgres_conn_id"


def perform_function(function_name):
    import psycopg2 as pg
    
    logging.info('Connecting to PostgreSQL Database')
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.connect(postgres_hook.get_uri())
    logging.info('Connected to PostgreSQL Database!')

    cursor = conn.cursor()
    cursor.callproc(function_name)
    conn.commit()
    logging.info(f'Successfully executed function: {function_name}')
    
    cursor.close() if cursor else None
    conn.close() if conn else None
    

with DAG(
    dag_id="stg_to_ods",
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=False,
    tags=["example", "postgres"],
) as dag:

    start_task = EmptyOperator(
        task_id='start',
    )

    fn_clear_data_task = PythonOperator(
        task_id='fn_clear_data',
        python_callable=perform_function,
        op_kwargs={'function_name': 'fn_clear_data'},
    )

    fn_build_mart_task = PythonOperator(
        task_id='fn_build_mart',
        python_callable=perform_function,
        op_kwargs={'function_name': 'fn_build_mart'},
    )

    end_task = EmptyOperator(
        task_id='end',
    )
    
    start_task >> fn_clear_data_task >> fn_build_mart_task >> end_task