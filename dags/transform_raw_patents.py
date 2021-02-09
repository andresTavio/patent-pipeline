from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.operators.python_operator import PythonOperator
from operators.local_to_s3 import LocalToS3Operator
from scripts.dag_util import construct_files_dict_no_date
import pathlib
from datetime import datetime
from airflow.utils.helpers import chain
from airflow.operators.dummy import DummyOperator

BASE_DIR = pathlib.Path().cwd()
SPARK_SCRIPTS_DIR = BASE_DIR.joinpath('spark/scripts')
LOCAL_FILE_DIRECTORY_FULL_PATH = SPARK_SCRIPTS_DIR.resolve()

S3_BUCKET_SCRIPTS = 'patents-spark-scripts-us-east-2'
S3_BUCKET_DATA = 'raw-patents-us-east-2'
S3_BUCKET_TRANSFORMED_DATA = 'transformed-patents-us-east-2'

EXECUTION_DATE = '{{ next_ds }}'
SPARK_FILES = {
    'parse_patent_from_raw_patents': {
        'file_name': 'parse_patent_from_raw_patents.py',
        'spark_step_args': {
            'name': 'Parse patent from raw patents',
            'python_dependencies': 's3://patents-spark-scripts-us-east-2/parse_entity_from_raw_patents.py',
            's3_input': 's3://{}/{}/{}'.format(S3_BUCKET_DATA, EXECUTION_DATE, '*.json'),
            's3_script': 's3://{}/{}'.format(S3_BUCKET_SCRIPTS, 'parse_patent_from_raw_patents.py'),
            's3_output': 's3://{}/{}'.format(S3_BUCKET_TRANSFORMED_DATA, 'patent'),
        }
    },
    'parse_inventor_from_raw_patents': {
        'file_name': 'parse_inventor_from_raw_patents.py',
        'spark_step_args': {
            'name': 'Parse inventor from raw patents',
            'python_dependencies': 's3://patents-spark-scripts-us-east-2/parse_entity_from_raw_patents.py',
            's3_input': 's3://{}/{}/{}'.format(S3_BUCKET_DATA, EXECUTION_DATE, '*.json'),
            's3_script': 's3://{}/{}'.format(S3_BUCKET_SCRIPTS, 'parse_inventor_from_raw_patents.py'),
            's3_output': 's3://{}/{}'.format(S3_BUCKET_TRANSFORMED_DATA, 'inventor'),
        }
    },
    'parse_assignee_from_raw_patents': {
        'file_name': 'parse_assignee_from_raw_patents.py',
        'spark_step_args': {
            'name': 'Parse assignee from raw patents',
            'python_dependencies': 's3://patents-spark-scripts-us-east-2/parse_entity_from_raw_patents.py',
            's3_input': 's3://{}/{}/{}'.format(S3_BUCKET_DATA, EXECUTION_DATE, '*.json'),
            's3_script': 's3://{}/{}'.format(S3_BUCKET_SCRIPTS, 'parse_assignee_from_raw_patents.py'),
            's3_output': 's3://{}/{}'.format(S3_BUCKET_TRANSFORMED_DATA, 'assignee'),
        }
    },
    'parse_cpc_from_raw_patents': {
        'file_name': 'parse_cpc_from_raw_patents.py',
        'spark_step_args': {
            'name': 'Parse cpc from raw patents',
            'python_dependencies': 's3://patents-spark-scripts-us-east-2/parse_entity_from_raw_patents.py',
            's3_input': 's3://{}/{}/{}'.format(S3_BUCKET_DATA, EXECUTION_DATE, '*.json'),
            's3_script': 's3://{}/{}'.format(S3_BUCKET_SCRIPTS, 'parse_cpc_from_raw_patents.py'),
            's3_output': 's3://{}/{}'.format(S3_BUCKET_TRANSFORMED_DATA, 'cpc')
        }
    },
    'parse_entity_from_raw_patents': {
        'file_name': 'parse_entity_from_raw_patents.py',
        'spark_step_args': None
    }
}
SPARK_FILES = construct_files_dict_no_date(SPARK_FILES, LOCAL_FILE_DIRECTORY_FULL_PATH)


default_args = {
    'owner': 'dev',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 28),
    'retries': 0
}


def render_spark_step_func(**kwargs):
    spark_step = {
        'Name': kwargs['name'],
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--py-files',
                kwargs['python_dependencies'],
                kwargs['s3_script'],
                '--input',
                kwargs['s3_input'],
                '--output',
                kwargs['s3_output']
            ]
        }
    }

    return [spark_step]


with DAG('transform_raw_patents',
    default_args=default_args,
    schedule_interval='@quarterly',
    catchup=False) as dag:

    # create non entity specific tasks
    start_dag = DummyOperator(
        task_id='start_dag',
    )

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        aws_conn_id='aws_default',
        emr_conn_id='emr_default'
    )
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}"
    
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id=job_flow_id,
        aws_conn_id='aws_default'
    )

    # create_emr_cluster = DummyOperator(
    #     task_id='create_emr_cluster',
    # )
    # job_flow_id = 'j-2L9SK0W463HO5'

    # terminate_emr_cluster = DummyOperator(
    #     task_id='terminate_emr_cluster',
    # )

    # create DAG for each entity
    for key, file in SPARK_FILES.items():
        # add initial steps
        entity_task_list = [start_dag]

        # load spark script to s3
        load_spark_script_to_s3 = LocalToS3Operator(
            task_id='load_spark_script_{}_to_s3'.format(key),
            s3_conn_id='',  # set in environment variable
            s3_bucket=S3_BUCKET_SCRIPTS,
            s3_key=file['s3_key'],
            local_file_path=file['local_file_path'],
            replace=True
        )
        entity_task_list.append(load_spark_script_to_s3)

        # start emr cluster
        entity_task_list.append(create_emr_cluster)

        # if file is related to a spark step, then create additional tasks in DAG
        if file['spark_step_args']:
            # render spark step
            render_spark_step = PythonOperator (
                task_id='render_spark_step_{}'.format(key),
                python_callable=render_spark_step_func,
                provide_context=True,
                op_kwargs={
                    'name': file['spark_step_args']['name'],
                    'python_dependencies': file['spark_step_args']['python_dependencies'],
                    's3_input': file['spark_step_args']['s3_input'],
                    's3_script': file['spark_step_args']['s3_script'],
                    's3_output': file['spark_step_args']['s3_output']
                }
            )
            entity_task_list.append(render_spark_step)

            # add spark step to emr
            add_step = EmrAddStepsOperator(
                task_id='add_step_{}'.format(key),
                job_flow_id=job_flow_id,
                aws_conn_id='aws_default',
                steps="{{ task_instance.xcom_pull(task_ids='" + 'render_spark_step_{}'.format(key) + "', key='return_value') }}",
            )
            entity_task_list.append(add_step)

            # wait for the step to complete
            watch_step = EmrStepSensor(
                task_id='watch_step_{}'.format(key),
                job_flow_id=job_flow_id,
                step_id="{{ task_instance.xcom_pull(task_ids='" + 'add_step_{}'.format(key) + "', key='return_value')[0] }}",
                aws_conn_id='aws_default'
            )
            entity_task_list.append(watch_step)

        # add final steps
        entity_task_list.append(terminate_emr_cluster)

        # build a dependency chain for the entity using entity_task_list
        chain(*entity_task_list)


