from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from train import etl

default_args = {
    'owner': 'Ali',
    'start_date': datetime(2023, 5, 27),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'train_data_etl',
    default_args=default_args,
    description='ETL process for train data',
    schedule='*/5 * * * *',  # Run every 5 minutes
)

etl_task = PythonOperator(
    task_id='execute_etl',
    python_callable=etl,
    dag=dag
)

etl_task
