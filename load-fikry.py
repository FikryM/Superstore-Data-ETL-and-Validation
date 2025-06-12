from pyspark.sql import SparkSession
from pymongo import MongoClient
import pandas as pd

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

df = spark.read.csv("/opt/airflow/data/superstore-cleaned.csv", header=True, inferSchema=True)

pdf = df.toPandas() 

client = MongoClient("mongodb+srv://user:user1234@fikry-clst.hmnvw41.mongodb.net/")
data_superstore = pdf.to_dict(orient='records')

client['dataset-milestone3']['superstore-data'].insert_many(data_superstore)