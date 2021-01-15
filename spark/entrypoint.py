import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("Python Spark SQL example") \
        .getOrCreate()

df = spark.read.json('app/files/raw_patents.json')
df_exploded = df.select(explode(df.patents))
df_exploded = df_exploded.withColumn('date', df_exploded.col.patent_date) \
    .withColumn('year', df_exploded.col.patent_year) \
    .withColumn('title', df_exploded.col.patent_title) \
    .withColumn('type', df_exploded.col.patent_type) \
    .withColumn('processing_time', df_exploded.col.patent_average_processing_time)
df_exploded.explain()
df_exploded.printSchema()
df_exploded.show()
df_exploded.write.mode("overwrite").parquet('app/files/raw_patents')
