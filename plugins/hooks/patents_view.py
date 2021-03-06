from airflow.hooks.base_hook import BaseHook
import requests
import json

BASE_URL = 'https://api.patentsview.org/{entity}/query'

class PatentsViewHook(BaseHook):
    def __init__(self, conn_id=None, source=None):
        super().__init__(source)

    def post(self, entity, query):
        # construct url
        url = BASE_URL.format(entity=entity)

        # post request
        response = requests.post(url, json=query)
        response.raise_for_status()
        response_json = response.json()

        # set initial variables
        results_key = entity
        total_count_key = f'total_{entity[:-1]}_count'

        total_count = response_json[total_count_key]
        current_count = response_json['count']
        page = 1
        print('current count: {}'.format(current_count))
        
        # If all items have not been requested, then get another response
        while current_count < total_count:
            print('Only have {} out of {} items, requesting again'.format(current_count, total_count))
            
            # construct parameters for page
            page += 1
            if not 'o' in query:
                query['o'] = {"page": page}
            else:
                query['o'].update({"page": page})

            # post request
            next_response = requests.post(url, json=query)
            next_response_json = next_response.json()
            
            # add items to initial response
            response_json[entity].extend(next_response_json[entity])

            # update counts
            current_count += next_response_json['count']
            print('current count: {}'.format(current_count))

        return response_json
    