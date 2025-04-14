
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from crud.functions import parse_scripts
from crud.config import SQL_SCRIPTS_PATH
import logging

FUNCTIONS_LIST = parse_scripts(SQL_SCRIPTS_PATH, raw=True)
POSTGRES_CONN_ID = "postgres_conn_id"

def build_functions():
    import psycopg2 as pg
    
    logging.info('Connecting to PostgreSQL Database')
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.connect(postgres_hook.get_uri())
    logging.info('Connected to PostgreSQL Database!')

    logging.info('Start to build functions...')
    
    for function_name in FUNCTIONS_LIST:
        with open(f'{SQL_SCRIPTS_PATH}/{function_name}', 'r') as file:
            query = file.read()
            
            logging.info(f'Building function {function_name}:\n{query}')

            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            logging.info(f'Successfully built function: {function_name}')
    

with DAG(
    dag_id="dag_build_functions",
    description=__doc__,
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=False,
    tags=["example", "postgres"],
) as dag:

    start_task = EmptyOperator(
        task_id='start',
    )

    build_functions_task = PythonOperator(
        task_id='build_functions',
        python_callable=build_functions,
    )
    
    trigger_perform_dag_task = TriggerDagRunOperator(
        task_id='trigger_perform_dag',
        trigger_dag_id='dag_perform_functions',
    )

    end_task = EmptyOperator(
        task_id='end',
    )

    start_task >> build_functions_task >> trigger_perform_dag_task >> end_task