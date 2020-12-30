import requests
import json

base_url = 'https://api.patentsview.org/{entity}/query'


class PatentsViewApi():
    def __init__(self):
        super().__init__()
         
    def post(self, entity, query, fields=None, sort=None, options=None):
        # construct url
        url = base_url.format(entity=entity)

        # construct parameters
        params = {'q': query}
        if fields:
            params['f'] = fields
        if sort:
            params['s'] = sort
        if options:
            params['o'] = options

        # post request
        response = requests.post(url, data=json.dumps(params))
        response.raise_for_status()
        response_json = response.json()

        # set initial variables
        total_count = response_json['total_patent_count']
        current_count = response_json['count']
        page = 1
        print('current count: {}'.format(current_count))
        
        # If all items have not been requested, then get another response
        while current_count < total_count:
            print('Only have {} out of {} items, requesting again'.format(current_count, total_count))
            
            # construct parameters for page
            page += 1
            if not 'o' in params:
                params['o'] = {"page": page}
            else:
                params['o'].update({"page": page})

            # post request
            next_response = requests.post(url, data=json.dumps(params))
            next_response_json = next_response.json()
            
            # add items to initial response
            response_json['patents'].extend(next_response_json['patents'])

            # update counts
            current_count += next_response_json['count']
            print('current count: {}'.format(current_count))

        return response_json



# api = PatentsViewApi()
# api.post(
#     entity='patents', 
#     query={'inventor_last_name': 'Whitney'},
#     fields=['patent_number', 'patent_date'],
#     sort=[{'patent_number': 'asc'}],
#     options={'per_page': 500}
# )
    