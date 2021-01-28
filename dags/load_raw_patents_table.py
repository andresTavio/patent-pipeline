from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pathlib
from datetime import datetime

BASE_DIR = pathlib.Path().cwd()
DAGS_DIR = BASE_DIR.joinpath('dags')
# APPLICATION_FILE_PATH = DAGS_DIR.joinpath('scripts/spark_load_raw_patents_table.py').resolve()
APPLICATION_FILE_PATH = '/opt/airflow/dags/scripts/spark_load_raw_patents_table.py'

default_args = {
    'owner': 'dev',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 28),
    'retries': 0
}

with DAG('load_raw_patents_table',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False) as dag:
    
    load_table = SparkSubmitOperator(
        task_id='load_table',
        application=APPLICATION_FILE_PATH,
        name='load_table',
        conn_id='spark2'
    )
