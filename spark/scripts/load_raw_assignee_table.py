from load_raw_dimension_table import load_table

input_file = '/app/files/raw_patents/2018-*/*.json'
db_table = 'raw_assignee'
column_field_name = 'assignees'
column_prefix_to_remove = 'assignee_'
column_duplicates = ['id']


load_table(input_file, db_table, column_field_name, column_prefix_to_remove, column_duplicates)
