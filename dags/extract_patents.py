from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from hooks.patents_view import PatentsViewHook
from operators.local_to_s3 import LocalToS3Operator
from operators.patents_view import PatentsToLocalOperator
from scripts.dag_util import construct_files_dict
from datetime import datetime
import pathlib

default_args = {
    'owner': 'dev',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 28),
    'retries': 0
}

BASE_DIR = pathlib.Path().cwd()
FILES_DIR = BASE_DIR.joinpath('files')
EXECUTION_DATE = '{{ next_ds }}'
S3_BUCKET = 'raw-patents-us-east-2'
LOCAL_FILE_DIRECTORY_FULL_PATH = '{}/{}'.format(FILES_DIR.resolve(), 'patents')
FILES = {'raw_patents': {'file_name': 'raw_patents.json'}}
FILES = construct_files_dict(FILES, EXECUTION_DATE, LOCAL_FILE_DIRECTORY_FULL_PATH)
QUERY_FILE_PATH = FILES_DIR.joinpath('patents_query.json').resolve()

with DAG('extract_patents',
         default_args=default_args,
         schedule_interval='@quarterly',
         catchup=False,
         max_active_runs=1) as dag:

    create_local_file_directory = BashOperator(
        task_id='create_local_file_directory',
        bash_command='mkdir -p {}/{}'.format(LOCAL_FILE_DIRECTORY_FULL_PATH, EXECUTION_DATE)
    )

    extract_patents = PatentsToLocalOperator(
        task_id='extract_patents',
        entity='patents', 
        query_file_path=QUERY_FILE_PATH,
        response_file_path=FILES['raw_patents']['local_file_path']
    )
    
    load_patents_to_s3 = LocalToS3Operator(
        task_id='load_patents',
        s3_conn_id='',  # set in environment variable
        s3_bucket=S3_BUCKET,
        s3_key=FILES['raw_patents']['s3_key'],
        local_file_path=FILES['raw_patents']['local_file_path'],
        replace=True
    )

    create_local_file_directory >> extract_patents >> load_patents_to_s3
