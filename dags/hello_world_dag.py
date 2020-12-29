from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

with DAG('hello_world', start_date=datetime(2020, 12, 28)) as dag:
    op = DummyOperator(task_id='hello_world_task')
