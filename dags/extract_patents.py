from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from hooks.patents_view import PatentsViewHook
from operators.local_to_s3 import LocalToS3Operator
from datetime import datetime
import pathlib
import json

default_args = {
    'owner': 'dev',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 28),
    'retries': 0
}

def construct_files_dict(files_dict, execution_date, local_file_directory_path):
    # construct each file path and s3 key from the file name
    for file_key in list(files_dict.keys()):
        files_dict[file_key]['s3_key'] = '{}/{}'.format(execution_date, files_dict[file_key]['file_name'])
        files_dict[file_key]['local_file_path'] = '{}/{}'.format(local_file_directory_path, files_dict[file_key]['s3_key'])

    return files_dict

def call_api(file_path, entity, query, fields, sort, options):
    hook = PatentsViewHook()
    response = hook.post(entity, query, fields, sort, options)
    print(response)
    
    with open(file_path, 'w') as f:
        json.dump(response, f)

    return response

BASE_DIR = pathlib.Path().cwd()
FILES_DIR = BASE_DIR.joinpath('files')
EXECUTION_DATE = '{{ ds }}'
S3_BUCKET = 'raw-patents-us-east-2'
LOCAL_FILE_DIRECTORY_FULL_PATH = '{}/{}'.format(FILES_DIR.resolve(), 'patents')
FILES = {'raw_patents': {'file_name': 'raw_patents.json'}}
FILES = construct_files_dict(FILES, EXECUTION_DATE, LOCAL_FILE_DIRECTORY_FULL_PATH)

with DAG('extract_patents',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False) as dag:

    create_local_file_directory = BashOperator(
        task_id='create_local_file_directory',
        bash_command='mkdir -p {}/{}'.format(LOCAL_FILE_DIRECTORY_FULL_PATH, EXECUTION_DATE),
        dag=dag
    )

    extract_patents = PythonOperator(
        task_id='extract_patents',
        python_callable=call_api,
        op_kwargs={
                'file_path': FILES['raw_patents']['local_file_path'],
                'entity': 'patents', 
                'query': {'inventor_last_name': 'Whitney'},
                'fields': ['patent_number', 'patent_date'],
                'sort': [{'patent_number': 'asc'}],
                'options': {'per_page': 500}
        }
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
