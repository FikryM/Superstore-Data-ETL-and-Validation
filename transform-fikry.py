from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pymongo import MongoClient

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

df = spark.read.csv('/opt/airflow/data/Sample - Superstore.csv', header=True , inferSchema=True)

def transform_data (df) : 
    df.withColumn("Order Date", to_timestamp("Order Date", "MM/dd/yyyy"))
    return df
    
transform_data(df)

df.write.csv("/opt/airflow/data/superstore-cleaned.csv", header=True)