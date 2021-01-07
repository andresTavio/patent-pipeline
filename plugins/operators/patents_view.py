from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.patents_view import PatentsViewHook
import json


class PatentsToLocalOperator(BaseOperator):
    """Queries PatentsView API and dumps to local json file.

    Attributes:
        entity: string, name of PatentsView endpoint to query
        query_file_path: string, full local file path to JSON file containing the query parameters
        response_file_path: string, full local file path to write response
    """

    template_fields = ['query_file_path', 'response_file_path']

    @apply_defaults
    def __init__(self,
                 entity,
                 query_file_path,
                 response_file_path,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.entity = entity
        self.query_file_path = query_file_path
        self.response_file_path = response_file_path

    def execute(self, context):
        print(f'Querying PatentsView API for {self.entity} with parameters in {self.query_file_path}')
        
        # read in query json file
        with open(self.query_file_path, 'r') as f:
            query = json.load(f)

        # init hook and post to api
        hook = PatentsViewHook()
        response = hook.post(self.entity, query)
        
        with open(self.response_file_path, 'w') as f:
            json.dump(response, f)

        print(f'Saved results to {self.response_file_path}')

