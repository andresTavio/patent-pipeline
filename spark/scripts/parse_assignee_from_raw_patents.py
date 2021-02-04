import argparse
from parse_entity_from_raw_patents import parse

column_field_name = 'assignees'
column_prefix_to_remove = 'assignee_'
column_duplicates = ['id']

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--output")
    args = parser.parse_args()
    
    parse(args.input, args.output, column_field_name, column_prefix_to_remove, column_duplicates)
