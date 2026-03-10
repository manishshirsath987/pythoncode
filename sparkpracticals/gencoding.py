# owner - sayu Softtech
# name - data_process.py
# use - read data from csv process and write in json
# created on - 01-02-2026
# modified - 19-02-2026
# version - 1.0.1
# run cmd : spark-submit data_process.py

# lib
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import json
from common_functions import read_csv,read_config_from_json,write_json,remove_spaces_from_df
from pyspark.sql.types import *

# variable
spark = SparkSession.builder.appName("CSV to JSON").getOrCreate()

json_file = "D:/config.json"
config_data = read_config_from_json(json_file)

src_path = config_data.get("src_path", "")
tgt_path = config_data.get("tgt_path", "")
table_list = config_data.get("table_list", [])

print(src_path)
print(tgt_path)
print(table_list)

schema=StructType([
    StructField("p id",IntegerType(),True),
    StructField("p name", StringType(), True),
    StructField("cat", StringType(), True),
    StructField("amt", IntegerType(), True),
    StructField("avl qty", IntegerType(), True)
])


# functions


# main
try:
    df_product = read_csv(spark, src_path,schema)
    df_product=remove_spaces_from_df(spark,df_product)

    df_elect = df_product.where(df_product['cat'] == 'Electronics')
    df_elect.show()
    write_json(spark, df_product, tgt_path, "overwrite")
except:
    print("Something went Wrong!!!")