from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from hooks.patents_view import PatentsViewHook
from operators.local_to_s3 import LocalToS3Operator
from operators.patents_view import PatentsToLocalOperator
from scripts.dag_util import construct_files_dict
from config.patents_config import (CURRENT_DATE,
                                   EXECUTION_DATE,
                                   LOCAL_FILE_PATH_RAW_DATA,
                                   LOCAL_FILE_PATH_PATENT_QUERY,
                                   S3_BUCKET_RAW_DATA)
from datetime import datetime
import pathlib

default_args = {
    'owner': 'dev',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 28),
    'retries': 0
}

FILES = {'raw_patents': {'file_name': 'raw_patents.json'}}
FILES = construct_files_dict(FILES, f'{CURRENT_DATE}/{EXECUTION_DATE}', LOCAL_FILE_PATH_RAW_DATA)

with DAG('extract_patents',
         default_args=default_args,
         schedule_interval='@quarterly',
         catchup=False) as dag:

    create_local_file_directory = BashOperator(
        task_id='create_local_file_directory',
        bash_command='mkdir -p {}/{}'.format(LOCAL_FILE_PATH_RAW_DATA, f'{CURRENT_DATE}/{EXECUTION_DATE}')
    )

    extract_patents = PatentsToLocalOperator(
        task_id='extract_patents',
        entity='patents', 
        query_file_path=LOCAL_FILE_PATH_PATENT_QUERY,
        response_file_path=FILES['raw_patents']['local_file_path']
    )
    
    load_patents_to_s3 = LocalToS3Operator(
        task_id='load_patents',
        s3_conn_id='',  # set in environment variable
        s3_bucket=S3_BUCKET_RAW_DATA,
        s3_key=FILES['raw_patents']['s3_key'],
        local_file_path=FILES['raw_patents']['local_file_path'],
        replace=True
    )

    create_local_file_directory >> extract_patents >> load_patents_to_s3
