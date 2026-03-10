from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("JDBC data process").getOrCreate()

url="jdbc:mysql://database-1.cjmqoyiqsjvb.ap-south-1.rds.amazonaws.com:3306/DEV"
username="myuser"
password="mypassword"
driver="com.mysql.cj.jdbc.Driver"
query="select cust_id,cust_name,city from customer where city='Pune'"

#read
df=spark.read.format("JDBC").option("url",url).option("user",username)\
    .option("password",password).option("driver",driver)\
    .option("query",query).load()

df.show()

