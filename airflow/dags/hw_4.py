from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import logging
from pathlib import Path

POSTGRES_CONN_ID = "postgres_conn_id"
SQL_SCRIPTS_PATH = Path(__file__).parent / "sql"  # Путь к папке с SQL-скриптами

def create_function(func_name: str):
    """Создает функцию в PostgreSQL из SQL-файла"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = None
    try:
        script_file = SQL_SCRIPTS_PATH / f"{func_name}.sql"
        
        if not script_file.exists():
            raise FileNotFoundError(f"SQL файл {script_file} не найден")
            
        with open(script_file, "r") as f:
            sql = f.read()
            
        conn = hook.get_conn()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            conn.commit()
        logging.info(f"Функция {func_name} создана")
        
    except Exception as e:
        logging.error(f"Ошибка: {str(e)}")
        if conn:
            conn.rollback() 
        raise
    finally:
        if conn:
            conn.close()

def create_all_functions():
    for script in SQL_SCRIPTS_PATH.glob("*.sql"):
        func_name = script.stem  # Имя файла без расширения
        create_function(func_name)

with DAG(
    dag_id="hw_4",
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=False,
    tags=["postgres"],
) as dag:

    start = EmptyOperator(task_id='start')
    
    create_functions = PythonOperator(
        task_id='create_functions',
        python_callable=create_all_functions
    )
    
    trigger_hw5 = TriggerDagRunOperator(
        task_id='trigger_hw5',
        trigger_dag_id='hw_5'
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> create_functions >> trigger_hw5 >> end