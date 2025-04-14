# Написать ДАГ hw_6, в котором:
# 1. Считываются данные в pandas Dataframe как SELECT * FROM dm.data_mart LIMIT 100;
# 2. Делается произвольная фильтрация через pandas по 2 полям, меняются количество и названия колонок на произвольные
# 3. Готовая таблицы выгружается обратно в Postgres как dm.data_mart_pandas
from crud.config import DATABASE_URL
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

def pandas_df_reformatting() -> pd.DataFrame:
    engine = create_engine(DATABASE_URL) 
    engine.connect
    df = pd.read_sql(sql='SELECT * FROM dm.data_mart LIMIT 100', con=engine)
    df = df.drop('stockcode', axis=1) 
    df = df.drop('invoicedate', axis=1)
    df = df.rename(columns={'customerid': 'client_id'})
    df = df.rename(columns={'country_name': 'country'})
    filtered_df_1 = df[(df['unitprice'] > 2)] 
    filtered_df = filtered_df_1.query('quantity > 1') 
    print('\nРезультат выполнения запроса:')
    print(filtered_df.to_string(index=False, justify='left'))
    filtered_df.to_sql(
        'data_mart_pandas', 
        engine,
        schema='dm',
        if_exists='replace', 
        index=False
        ) 

with DAG(
    dag_id='hw_6',
    schedule='@once',
    start_date=days_ago(1),
    catchup=False,
    tags=['postgres', 'mizumizu'],
) as dag:

    start = EmptyOperator(task_id='start')
    
    process_data = PythonOperator(
        task_id='process_data',
        python_callable=pandas_df_reformatting
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> process_data >> end