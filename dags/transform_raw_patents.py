from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from operators.local_to_s3 import LocalToS3Operator
from scripts.dag_util import construct_files_dict_no_date
import pathlib
from datetime import datetime
from airflow.utils.helpers import chain
from airflow.operators.dummy import DummyOperator

BASE_DIR = pathlib.Path().cwd()
SPARK_SCRIPTS_DIR = BASE_DIR.joinpath('spark/scripts')
LOCAL_FILE_DIRECTORY_FULL_PATH = SPARK_SCRIPTS_DIR.resolve()

EXECUTION_DATE = '{{ next_ds }}'
SPARK_FILES = {
    'parse_patent_from_raw_patents': {
        'file_name': 'parse_patent_from_raw_patents.py',
    },
    'parse_inventor_from_raw_patents': {
        'file_name': 'parse_inventor_from_raw_patents.py',
    },
    'parse_assignee_from_raw_patents': {
        'file_name': 'parse_assignee_from_raw_patents.py',
    },
    'parse_cpc_from_raw_patents': {
        'file_name': 'parse_cpc_from_raw_patents.py'
    },
    'parse_entity_from_raw_patents': {
        'file_name': 'parse_entity_from_raw_patents.py',
    }
}
SPARK_FILES = construct_files_dict_no_date(SPARK_FILES, LOCAL_FILE_DIRECTORY_FULL_PATH)

S3_BUCKET_SCRIPTS = 'patents-spark-scripts-us-east-2'
S3_BUCKET_DATA = 'raw-patents-us-east-2'
S3_BUCKET_TRANSFORMED_DATA = 'transformed-patents-us-east-2'

SPARK_STEPS = [
    {
        "Name": "Parse patent from raw patents",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                '--py-files',
                '{{ params.python_dependencies }}',
                '{{ params.patent_s3_script }}',
                '--input',
                '{{ params.s3_input }}',
                '--output',
                '{{ params.patent_s3_output }}'
            ]
        }
    },
    # {
    #     "Name": "Parse inventor from raw patents",
    #     "ActionOnFailure": "CONTINUE",
    #     "HadoopJarStep": {
    #         "Jar": "command-runner.jar",
    #         "Args": [
    #             "spark-submit",
    #             "--deploy-mode",
    #             "cluster",
    #             '--py-files',
    #             '{{ params.python_dependencies }}',
    #             '{{ params.inventor_s3_script }}',
    #             '--input',
    #             '{{ params.s3_input }}',
    #             '--output',
    #             '{{ params.inventor_s3_output }}'
    #         ]
    #     }
    # },
    # {
    #     "Name": "Parse assignee from raw patents",
    #     "ActionOnFailure": "CONTINUE",
    #     "HadoopJarStep": {
    #         "Jar": "command-runner.jar",
    #         "Args": [
    #             "spark-submit",
    #             "--deploy-mode",
    #             "cluster",
    #             '--py-files',
    #             '{{ params.python_dependencies }}',
    #             '{{ params.assignee_s3_script }}',
    #             '--input',
    #             '{{ params.s3_input }}',
    #             '--output',
    #             '{{ params.assignee_s3_output }}'
    #         ]
    #     }
    # },
    # {
    #     "Name": "Parse cpc from raw patents",
    #     "ActionOnFailure": "CONTINUE",
    #     "HadoopJarStep": {
    #         "Jar": "command-runner.jar",
    #         "Args": [
    #             "spark-submit",
    #             "--deploy-mode",
    #             "cluster",
    #             '--py-files',
    #             '{{ params.python_dependencies }}',
    #             '{{ params.cpc_s3_script }}',
    #             '--input',
    #             '{{ params.s3_input }}',
    #             '--output',
    #             '{{ params.cpc_s3_output }}'
    #         ]
    #     }
    # }
]

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

    start_dag = DummyOperator(
        task_id='start_dag',
    )

    # job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}"
    job_flow_id = 'j-33DU4UHRC0PB4'

    # create_emr_cluster = EmrCreateJobFlowOperator(
    #     task_id='create_emr_cluster',
    #     aws_conn_id='aws_default',
    #     emr_conn_id='emr_default'
    # )
    create_emr_cluster = DummyOperator(
        task_id='create_emr_cluster',
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id=job_flow_id,
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
        params={
            'python_dependencies': 's3://patents-spark-scripts-us-east-2/parse_entity_from_raw_patents.py',
            's3_input': 's3://{}/{}'.format(S3_BUCKET_DATA, '2018-*/*.json'),
            'patent_s3_script': 's3://{}/{}'.format(S3_BUCKET_SCRIPTS, 'parse_patent_from_raw_patents.py'),
            'patent_s3_output': 's3://{}/{}'.format(S3_BUCKET_TRANSFORMED_DATA, 'patent'),
            'inventor_s3_script': 's3://{}/{}'.format(S3_BUCKET_SCRIPTS, 'parse_inventor_from_raw_patents.py'),
            'inventor_s3_output': 's3://{}/{}'.format(S3_BUCKET_TRANSFORMED_DATA, 'inventor'),
            'assignee_s3_script': 's3://{}/{}'.format(S3_BUCKET_SCRIPTS, 'parse_assignee_from_raw_patents.py'),
            'assignee_s3_output': 's3://{}/{}'.format(S3_BUCKET_TRANSFORMED_DATA, 'assignee'),
            'cpc_s3_script': 's3://{}/{}'.format(S3_BUCKET_SCRIPTS, 'parse_cpc_from_raw_patents.py'),
            'cpc_s3_output': 's3://{}/{}'.format(S3_BUCKET_TRANSFORMED_DATA, 'cpc'),
        }
    )

    # wait for the steps to complete
    last_step = len(SPARK_STEPS) - 1
    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id=job_flow_id,
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[" + str(last_step) + "] }}",
        aws_conn_id='aws_default'
    )

    # Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id=job_flow_id,
        aws_conn_id="aws_default"
    )

    # upload all spark files to s3
    for key, files in SPARK_FILES.items():
        # add initial steps
        entity_task_list = [start_dag]

        # add entity specific steps
        load_spark_script_to_s3 = LocalToS3Operator(
            task_id='load_spark_script_{}_to_s3'.format(key),
            s3_conn_id='',  # set in environment variable
            s3_bucket=S3_BUCKET_SCRIPTS,
            s3_key=files['s3_key'],
            local_file_path=files['local_file_path'],
            replace=True
        )

        entity_task_list.append(load_spark_script_to_s3)

        # add final steps
        entity_task_list.extend([create_emr_cluster, step_adder, step_checker, terminate_emr_cluster])

        # build a dependency chain for the entity using entity_task_list
        chain(*entity_task_list)


