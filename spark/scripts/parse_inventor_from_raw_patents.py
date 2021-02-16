import argparse
from parse_entity_from_raw_patents import parse

column_field_name = 'inventors'
column_prefix_to_remove = 'inventor_'
column_duplicates = ['id']

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--output")
    parser.add_argument("--db_table")
    args = parser.parse_args()

    parse(args.input, args.output, column_field_name, column_prefix_to_remove, column_duplicates, args.db_table)
