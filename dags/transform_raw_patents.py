from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from operators.local_to_s3 import LocalToS3Operator
from scripts.dag_util import construct_files_dict_no_date
import pathlib
from datetime import datetime

BASE_DIR = pathlib.Path().cwd()
SPARK_SCRIPTS_DIR = BASE_DIR.joinpath('spark/scripts')
LOCAL_FILE_DIRECTORY_FULL_PATH = SPARK_SCRIPTS_DIR.resolve()

EXECUTION_DATE = '{{ next_ds }}'
SPARK_FILES = {'load_raw_patent_table': {'file_name': 'load_raw_patent_table.py'}}
SPARK_FILES = construct_files_dict_no_date(SPARK_FILES, LOCAL_FILE_DIRECTORY_FULL_PATH)

S3_BUCKET_SCRIPTS = 'patents-spark-scripts-us-east-2'
S3_BUCKET_DATA = 'raw-patents-us-east-2'
S3_BUCKET_TRANSFORMED_DATA = 'transformed-patents-us-east-2'

JOB_FLOW_OVERRIDES = {
    'Name': 'Normalize Patents',
    'ReleaseLabel': 'emr-6.2.0',
    'Applications': [{'Name': 'Hadoop'}, {'Name': 'Spark'}], # We want our EMR cluster to have HDFS and Spark
    'Configurations': [
        {
            'Classification': 'spark-env',
            'Configurations': [
                {
                    'Classification': 'export',
                    'Properties': {'PYSPARK_PYTHON': '/usr/bin/python3'}, # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Core - 2',
                'Market': 'SPOT',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2,
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}

s3_clean = "clean_data/"
SPARK_STEPS = [
    {
        "Name": "Normalize data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "{{ params.s3_script }}",
            ]
        }
    }
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

    load_spark_script_to_s3 = LocalToS3Operator(
        task_id='load_spark_script_to_s3',
        s3_conn_id='',  # set in environment variable
        s3_bucket=S3_BUCKET_SCRIPTS,
        s3_key=SPARK_FILES['load_raw_patent_table']['s3_key'],
        local_file_path=SPARK_FILES['load_raw_patent_table']['local_file_path'],
        replace=True
    )

    clusterid = 'j-3M7C84C2ZXDK8'
    # create_emr_cluster = EmrCreateJobFlowOperator(
    #     task_id='create_emr_cluster',
    #     job_flow_overrides=JOB_FLOW_OVERRIDES,
    #     aws_conn_id='aws_default',
    #     emr_conn_id='emr_default'
    # )
    
    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        # job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        job_flow_id=clusterid,
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
        params={ # these params are used to fill the paramterized values in SPARK_STEPS json
            'files_bucket_name': S3_BUCKET_DATA,
            'scripts_bucket_name': S3_BUCKET_SCRIPTS,
            's3_data': 's3://{}/{}'.format(S3_BUCKET_DATA, '2018-01-01/raw_patents.json'),
            's3_script': 's3://{}/{}'.format(S3_BUCKET_SCRIPTS, 'load_raw_patent_table.py'),
            's3_clean': 's3://{}/{}'.format(S3_BUCKET_TRANSFORMED_DATA, '2018-01-01/patents.json')
        }
    )

    last_step = len(SPARK_STEPS) - 1 # this value will let the sensor know the last step to watch
    # wait for the steps to complete
    step_checker = EmrStepSensor(
        task_id='watch_step',
        # job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        job_flow_id=clusterid,
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[" + str(last_step) + "] }}",
        aws_conn_id='aws_default'
    )

    # # Terminate the EMR cluster
    # terminate_emr_cluster = EmrTerminateJobFlowOperator(
    #     task_id="terminate_emr_cluster",
    #     # job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    #     job_flow_id=clusterid,
    #     aws_conn_id="aws_default"
    # )

    # load_spark_script_to_s3 >> create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
    load_spark_script_to_s3 >> step_adder >> step_checker
