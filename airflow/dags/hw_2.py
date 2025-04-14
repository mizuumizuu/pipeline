from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import EmptyOperator
from airflow.utils.dates import days_ago
from pathlib import Path
import yaml
import logging
from typing import Union

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

def calculate_product() -> Union[float, int, str]:
    try:
        config_path = Path(__file__).parent / "source" / "config.yaml"
        
        if not config_path.exists():
            raise FileNotFoundError("Файл config.yaml не найден")
        
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        if not all(key in config.get("spec", {}) for key in ["variables", "format"]):
            raise ValueError("Некорректная структура конфиг-файла")

        variables = config["spec"]["variables"]
        format_type = config["spec"]["format"].lower()


        numbers = []
        for item in variables:
            try:
                num = float(item)
                numbers.append(num)
            except (ValueError, TypeError):
                continue  

        if not numbers:
            raise ValueError("Нет валидных числовых значений в variables")

        result = 1.0
        for num in numbers:
            result *= num
        if format_type == "float":
            return round(result, 2)
        elif format_type == "int":
            return int(round(result))
        return f"{round(result, 2)}"
            
    except FileNotFoundError:
        return "-1"
    except yaml.YAMLError as e:
        print(f"Ошибка парсинга YAML: {str(e)}")
        return "-1"
    except ValueError as e:
        print(f"Ошибка: {str(e)}")
        return "-1"
    except Exception as e:
        print(f"Неизвестная ошибка: {str(e)}")
        return "-1"

with DAG(
    dag_id="hw_2",
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=False,
    tags=["postgres", 'mizumizu'],
    default_args=default_args
) as dag:
    
    start_task = EmptyOperator(
        task_id='start',
    )

    calculate_product_task = PythonOperator(
        task_id='calculate_product_task',
        python_callable=calculate_product,
    )

    end_task = EmptyOperator(
        task_id='end',
    )

    start_task >> calculate_product_task >> end_task