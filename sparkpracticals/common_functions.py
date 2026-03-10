from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import json

spark = SparkSession.builder.appName("CSV to JSON").getOrCreate()

def read_config_from_json(json_file):
    with open(json_file, 'r') as file:
        config = json.load(file)
    return config

def read_csv(spark, path,schema, delim=",", header="true"):
    df = spark.read.format("CSV").schema(schema).option("header", header).option("inferschema", "true").option("delimiter",
                                                                                                delim).load(path)
    return df


def write_json(spark, df, path, mode="append"):
    df.write.format("JSON").mode(mode).save(path)

def remove_spaces_from_df(spark,df):
    cols=df.columns
    for x in cols:
        y = x.replace(" ", "")
        df = df.withColumnRenamed(x, y)
    return df