
### Imports
import os
import io
from google.cloud import storage
import pandas as pd
# pip install bigquery
from google.cloud import bigquery
import psycopg2


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "amita-engineering-group-project-0489a29e6826.json"

### Connecting to Postgres###

host="34.136.83.153"
dbname="postgres"
user="amitasujith"
password="baucl"
port="5432"


conn = psycopg2.connect (
    host=host,
    dbname=dbname,
    user=user,
    password=password,
    port=port
)
print("Database connection established")


### SPARK SESSION ###

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("spark.jars", "/Users/amita/Desktop/postgresql-42.7.2.jar") \
    .getOrCreate()

# PostgreSQL information
database_url = "jdbc:postgresql://34.136.83.153:5432/postgres"
properties = {
    "user": "amitasujith",
    "password": "baucl",
    "driver": "org.postgresql.Driver"
}

# Reading data from the 'deforestation_data' table
deforestation_df = spark.read \
    .jdbc(url=database_url, table="deforestation_data", properties=properties)

# Reading data from the 'weather_data' table
weather_df = spark.read \
    .jdbc(url=database_url, table="weather_data", properties=properties)

print(deforestation_df)