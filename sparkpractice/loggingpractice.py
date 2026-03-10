
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("test").getOrCreate()

spark.sparkContext.setLogLevel("FATAL")

rdd=spark.sparkContext.textFile("D:/internet.logs")

rdd1=rdd.map(lambda x:x.replace("//","|")).map(lambda x:x.replace("/","|")).map(lambda x:x.replace("-","|"))

rdd2 = rdd1.map(lambda x: [
    x.split("|")[1],
    x.split("|")[2],
    x.split("|")[5]
])

from pyspark.sql import Row
schema=Row("username","city","site")

def sepr(line):
	return line[0],line[1],line[2]

df=rdd2.map(sepr).map(lambda x:schema(*x)).toDF()
df.show()