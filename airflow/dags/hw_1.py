from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

def print_current_date(**kwargs):
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"Текущая дата: {current_date}")
    print(f"Текущая дата (через print): {current_date}")

with DAG(
    dag_id="hw_1",
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=False,
    tags=["postgres", 'mizumizu'],
) as dag:
    
    start_task = EmptyOperator(
        task_id='start',
    )

    print_date_task = PythonOperator(
        task_id='print_date_function',
        python_callable=print_current_date,
    )

    end_task = EmptyOperator(
        task_id='end',
    )

    start_task >> print_date_task >> end_task