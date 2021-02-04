import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import os
import argparse

def parse_patents_from_raw_patents(input_file, output_file):
    spark = SparkSession \
        .builder \
        .appName('Parse patents from raw patents') \
        .getOrCreate()

    # read in
    df = spark.read.json(input_file)

    # transform one row to multi rows
    df_exploded = df.select(explode(df.patents))

    # transform one column to top level columns
    df_exploded = df_exploded.select('col.*')

    # add first investor and assignee
    df_exploded = df_exploded \
        .withColumn('assignee_0_id', df_exploded.assignees.getItem(0).assignee_id) \
        .withColumn('inventor_0_id', df_exploded.inventors.getItem(0).inventor_id) \
        .withColumn('cpc_0_group_id', df_exploded.cpcs.getItem(0).cpc_group_id) \

    # drop array columns
    drop_list = [name for name, dtype in df_exploded.dtypes if 'array' in dtype]
    df_exploded = df_exploded.drop(*drop_list)

    df_exploded.write.mode("overwrite").json(output_file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--output")
    args = parser.parse_args()
    parse_patents_from_raw_patents(args.input, args.output)
