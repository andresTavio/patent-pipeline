from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.patents_view import PatentsViewHook
import json


class PatentsToLocalOperator(BaseOperator):
    """Queries PatentsView API and dumps to local json file.

    Attributes:
        entity: string, name of PatentsView endpoint to query
        query: dict, JSON formatted object containing the query parameters
        response_file_path: string, full local file path to write response
    """

    template_fields = ['query', 'response_file_path']

    @apply_defaults
    def __init__(self,
                 entity,
                 query,
                 response_file_path,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.entity = entity
        self.query = query
        self.response_file_path = response_file_path

    def execute(self, context):
        print(f'Querying PatentsView API for {self.entity} with parameters {self.query}')
        hook = PatentsViewHook()
        response = hook.post(self.entity, self.query)
        
        with open(self.response_file_path, 'w') as f:
            json.dump(response, f)

        print(f'Saved results to {self.response_file_path}')

