import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'fikry',
    'start_date': dt.datetime(2024, 11, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=600),
}


with DAG('dag-dataset-superstore',
         default_args=default_args,
         schedule_interval='10-30/10 9 * * 6',
         catchup=False,
         ) as dag:

    python_extract = BashOperator(task_id='extract-fikry', bash_command='sudo -u airflow python /opt/airflow/scripts/extract-fikry.py')
    python_transform = BashOperator(task_id='transform-fikry', bash_command='sudo -u airflow python /opt/airflow/scripts/transform-fikry.py')
    python_load = BashOperator(task_id='load-fikry', bash_command='sudo -u airflow python /opt/airflow/scripts/load-fikry.py')
    

python_extract >> python_transform >> python_load