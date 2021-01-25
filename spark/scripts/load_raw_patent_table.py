import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import os

spark = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('Python Spark SQL transform patents') \
        .config('spark.jars', os.environ['SPARK_POSTGRES_DRIVER_LOCATION']) \
        .getOrCreate()

# read in
df = spark.read.json('/app/files/raw_patents/2018-*/*.json')

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

# write to db
df_exploded.write.format('jdbc') \
    .options(
         url=os.environ['POSTGRES_URL'],
         dbtable='raw_patent',
         user=os.environ['POSTGRES_USER'],
         password=os.environ['POSTGRES_PASSWORD'],
         driver='org.postgresql.Driver') \
    .save()

