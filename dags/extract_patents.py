from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from patents_view import PatentsViewApi

with DAG('extract_patents', start_date=datetime(2020, 12, 28), schedule_interval='@once') as dag:
    op = PythonOperator(
        task_id='extract_patents_task',
        python_callable=PatentsViewApi().post,
        op_kwargs={
                'entity': 'patents', 
                'query': {'inventor_last_name': 'Whitney'},
                'fields': ['patent_number', 'patent_date'],
                'sort': [{'patent_number': 'asc'}],
                'options': {'per_page': 500}
        })

