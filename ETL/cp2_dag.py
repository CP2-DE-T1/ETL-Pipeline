from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from cp2_etl import extract_cp2_log, transform_cp2_log, load_cp2_log

default_args={
    'owner':'JoshSong',
    'depends_on_past':False,
    'start_date': datetime(2023,2,21),
    'email':['hssong95@me.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

# with DAG()

dag = DAG(
    'cp2_dag',
    default_args=default_args,
    description='etl for cp2'
)

run_etl = PythonOperator(
    task_id='complete_cp2_etl',
    python_callable=extract_cp2_log,
    dag=dag,
)

run_etl