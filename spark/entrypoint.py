import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("Python Spark SQL example") \
        .getOrCreate()

df = spark.read.json('app/files/raw_patents.json')
df = df.select(explode(df.patents))
df.explain()
df.printSchema()
df.show()
df.write.mode('overwrite').parquet('app/files/raw_patents')