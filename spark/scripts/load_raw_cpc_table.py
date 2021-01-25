from load_raw_dimension_table import load_table

input_file = '/app/files/raw_patents/2018-*/*.json'
db_table = 'raw_cpc'
column_field_name = 'cpcs'
column_prefix_to_remove = 'cpc_'
column_duplicates = ['group_id', 'section_id']


load_table(input_file, db_table, column_field_name, column_prefix_to_remove, column_duplicates)
