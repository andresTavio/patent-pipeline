import argparse
from parse_entity_from_raw_patents import parse

column_field_name = 'cpcs'
column_prefix_to_remove = 'cpc_'
column_duplicates = ['group_id', 'section_id']

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--output")
    args = parser.parse_args()
    parse(args.input, args.output, column_field_name, column_prefix_to_remove, column_duplicates)
