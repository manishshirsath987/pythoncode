from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=SparkSession.builder.appName("custom schema").getOrCreate()

header=StructType([
    StructField("dept",StringType(),True),
    StructField("dname", StringType(), True),
    StructField("eid", IntegerType(), True),
    StructField("ename", StringType(), True),
    StructField("doj", TimestampType(), True),
    StructField("sal", DoubleType(), True)
])

df=spark.read.format("CSV").option("header","false").option("delimiter","#")\
    .schema(header).load("D:/emp3.csv")

df.printSchema()
df.show()