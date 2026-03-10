from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=SparkSession.builder.appName("custom schema").getOrCreate()

schema=StructType([
    StructField("eid",IntegerType(),True),
    StructField("ename",StringType(),True),
    StructField("did",IntegerType(),True),
    StructField("salary",DoubleType(),True),
    StructField("__corrup_records",StringType(),True),
])

df=spark.read.format("CSV").option("header","true").option("delimiter",",")\
    .schema(schema).option("columnNameOfCorruptRecord","__corrup_records")\
    .option("mode","PERMISSIVE").load("D:/emp4.csv")

df.printSchema()
df.show()