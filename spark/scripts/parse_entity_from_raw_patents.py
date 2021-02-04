import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import os

def parse(input_file, output_file, column_field_name, column_prefix_to_remove, column_duplicates):
    # build spark session
    spark = SparkSession \
            .builder \
            .appName('Parsing {} from raw patents'.format(column_field_name)) \
            .getOrCreate()

    # read in raw json files for patents 
    df = spark.read.json(input_file)

    # expode out each patent as a row
    df = df \
        .select(explode('patents')) \
        .select('col.*')

    # explode out each entity as a row
    df_entity = df \
        .select(explode(column_field_name)) \
        .select('col.*')

    # rename columns to remove prefix
    new_columns = list(map(lambda col: col.replace(column_prefix_to_remove, ''), df_entity.columns))
    df_entity = df_entity.toDF(*new_columns)

    # drop duplicate rows
    df_entity = df_entity.dropDuplicates(column_duplicates)

    # write output
    df_entity.write.mode("overwrite").json(output_file)

    # spark = SparkSession \
    #     .builder \
    #     .master('local[*]') \
    #     .appName('Python Spark SQL transform patents') \
    #     .config('spark.jars', os.environ['SPARK_POSTGRES_DRIVER_LOCATION']) \
    #     .getOrCreate()

    # write to db
    # df_entity.write.format('jdbc') \
    #     .options(
    #         url=os.environ['POSTGRES_URL'],
    #         dbtable=db_table,
    #         user=os.environ['POSTGRES_USER'],
    #         password=os.environ['POSTGRES_PASSWORD'],
    #         driver='org.postgresql.Driver') \
    #     .save()
