from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime
from etl import read_csv, read_db, transform_csv, transform_db, merge, load, store

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 30),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'project_dag',
    default_args=default_args,
    description='Spotify and Grammys analysis DAG',
    schedule_interval='@daily',
) as dag:

    read_csv_task = PythonOperator(
        task_id='read_csv_task',
        python_callable=read_csv,
        provide_context = True,
    )

    read_db_task = PythonOperator(
        task_id='read_db_task',
        python_callable=read_db,
        provide_context = True,
        )

    transform_csv_task = PythonOperator(
        task_id='transform_csv_task',
        python_callable=transform_csv,
        provide_context = True,
        )

    transform_db_task = PythonOperator(
        task_id='transform_db_task',
        python_callable=transform_db,
        provide_context = True,
        )
    
    merge_task = PythonOperator(
        task_id='merge_task',
        python_callable=merge,
        provide_context = True,
        )
    
    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
        provide_context = True,
        )
    
    store_task = PythonOperator(
        task_id='store_task',
        python_callable=store,
        provide_context = True,
        )

    read_csv_task >> transform_csv_task >> merge_task >> load_task >> store_task
    read_db_task >> transform_db_task >> merge_task
