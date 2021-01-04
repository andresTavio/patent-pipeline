from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.patents_view import PatentsViewHook
import json


class PatentsToLocalOperator(BaseOperator):
    """Queries PatentsView API and dumps to local json file.

    Attributes:
        file_path: string, full local file path to write out to
        entity: string, name of PatentsView endpoint to query
        query: string, JSON formatted object containing the query parameters
        fields: string, JSON formatted array of fields to include in the results
        sort: string, JSON formatted array of objects to sort the results
        options: string, JSON formatted object of options to modify the query or results 
    """

    template_fields = ['file_path']

    @apply_defaults
    def __init__(self,
                 file_path,
                 entity,
                 query,
                 fields=None,
                 sort=None,
                 options=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.file_path = file_path
        self.entity = entity
        self.query = query
        self.fields = fields
        self.sort = sort
        self.options = options

    def execute(self, context):
        print('Querying PatentsView API')
        hook = PatentsViewHook()
        response = hook.post(self.entity, self.query, self.fields, self.sort, self.options)
        print(response)
        
        with open(self.file_path, 'w') as f:
            json.dump(response, f)

        print(f'Saved results to {self.file_path}')

