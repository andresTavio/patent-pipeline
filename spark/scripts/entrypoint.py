import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import os

spark = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('Python Spark SQL transform patents') \
        .config('spark.jars', os.environ["SPARK_CLASSPATH"]) \
        .config('spark.executor.extraClassPath', os.environ["SPARK_CLASSPATH"]) \
        .config('spark.driver.extraClassPath', os.environ["SPARK_CLASSPATH"]) \
        .getOrCreate()

# read in
df = spark.read.json('/app/files/raw_patents/2019-10-01/*.json')

# transform one row to multi rows
df_exploded = df.select(explode(df.patents))

# transform one column to top level columns
df_exploded = df_exploded.select('col.*')

# add first investor and assignee
df_exploded = df_exploded.withColumn('assignee_0_id', df_exploded.assignees.getItem(0).assignee_id) \
    .withColumn('assignee_0_organization', df_exploded.assignees.getItem(0).assignee_organization) \
    .withColumn('assignee_0_city', df_exploded.assignees.getItem(0).assignee_city) \
    .withColumn('assignee_0_state', df_exploded.assignees.getItem(0).assignee_state) \
    .withColumn('assignee_0_type', df_exploded.assignees.getItem(0).assignee_type) \
    .withColumn('assignee_0_first_name', df_exploded.assignees.getItem(0).assignee_first_name) \
    .withColumn('assignee_0_last_name', df_exploded.assignees.getItem(0).assignee_last_name) \
    .withColumn('inventor_0_id', df_exploded.inventors.getItem(0).inventor_id) \
    .withColumn('inventor_0_first_name', df_exploded.inventors.getItem(0).inventor_first_name) \
    .withColumn('inventor_0_last_name', df_exploded.inventors.getItem(0).inventor_last_name) \
    .withColumn('inventor_0_city', df_exploded.inventors.getItem(0).inventor_city) \
    .withColumn('inventor_0_state', df_exploded.inventors.getItem(0).inventor_state)

# # write out to parquet
# df_exploded.write.mode('overwrite').parquet('app/files/parquet_patents')

# write to db
df_exploded.write.jdbc('jdbc:postgresql://localhost', 'patents.patent', properties={'user': 'wyattshapiro', 'password': ''})

