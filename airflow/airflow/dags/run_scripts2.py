from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def my_task():
    print("Hello Airflow! Task 2 is running.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'example_dag',
    default_args=default_args,
    description='My second DAG',
    start_date=datetime(2025, 12, 10),
    schedule=timedelta(minutes=3),
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id='print_hello',
        python_callable=my_task
    )

    task1
