from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from hooks.patents_view import PatentsViewHook
from operators.local_to_s3 import LocalToS3Operator
from operators.patents_view import PatentsToLocalOperator
from scripts.dag_util import construct_files_dict
from datetime import datetime
import pathlib
import json

default_args = {
    'owner': 'dev',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 28),
    'retries': 0
}

BASE_DIR = pathlib.Path().cwd()
FILES_DIR = BASE_DIR.joinpath('files')
EXECUTION_DATE = '{{ next_ds }}'
# PREVIOUS_EXECUTION_DATE = '{{ ds }}'
PREVIOUS_EXECUTION_DATE = '2020-08-15'
S3_BUCKET = 'raw-patents-us-east-2'
LOCAL_FILE_DIRECTORY_FULL_PATH = '{}/{}'.format(FILES_DIR.resolve(), 'patents')
FILES = {'raw_patents': {'file_name': 'raw_patents.json'}}
FILES = construct_files_dict(FILES, EXECUTION_DATE, LOCAL_FILE_DIRECTORY_FULL_PATH)
QUERY_FILE_PATH = FILES_DIR.joinpath('assignee_contains_query.json')

with DAG('extract_patents',
         default_args=default_args,
         schedule_interval='@quarterly',
         catchup=False) as dag:

    create_local_file_directory = BashOperator(
        task_id='create_local_file_directory',
        bash_command='mkdir -p {}/{}'.format(LOCAL_FILE_DIRECTORY_FULL_PATH, EXECUTION_DATE),
        dag=dag
    )

    @dag.task
    def read_query_from_file(file_path):
        with open(file_path, 'r') as f:
            json_dict = json.load(f)
    
        return json_dict

    assignee_contains_query = read_query_from_file(QUERY_FILE_PATH)

    extract_patents = PatentsToLocalOperator(
        task_id='extract_patents',
        file_path=FILES['raw_patents']['local_file_path'],
        entity='patents', 
        query={'_and':
            [
                {'_gte': {'patent_date': PREVIOUS_EXECUTION_DATE}}, 
                {'_lt': {'patent_date': EXECUTION_DATE}},
                assignee_contains_query
            ]
        },
        fields=['patent_number', 'patent_date', 'patent_title', 'assignee_organization'],
        sort=[{'patent_number': 'asc'}],
        options={'per_page': 500}
    )
    
    load_patents_to_s3 = LocalToS3Operator(
        task_id='load_patents',
        s3_conn_id='',  # set in environment variable
        s3_bucket=S3_BUCKET,
        s3_key=FILES['raw_patents']['s3_key'],
        local_file_path=FILES['raw_patents']['local_file_path'],
        replace=True,
        dag=dag
    )

    create_local_file_directory >> extract_patents >> load_patents_to_s3
