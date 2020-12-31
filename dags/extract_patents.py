from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from hooks.patents_view import PatentsViewHook
from datetime import datetime

default_args = {
    'owner': 'dev',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 28),
    'retries': 0
}

def call_api(entity, query, fields, sort, options):
    hook = PatentsViewHook()
    response = hook.post(entity, query, fields, sort, options)
    print(response)
    return response


with DAG('extract_patents',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False) as dag:

    extract_patents = PythonOperator(
        task_id='extract_patents',
        python_callable=call_api,
        op_kwargs={
                'entity': 'patents', 
                'query': {'inventor_last_name': 'Whitney'},
                'fields': ['patent_number', 'patent_date'],
                'sort': [{'patent_number': 'asc'}],
                'options': {'per_page': 500}
        })
