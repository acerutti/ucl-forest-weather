
### Imports
import os
import io
from google.cloud import storage
import pandas as pd
# pip install bigquery
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "amita-engineering-group-project-0489a29e6826.json"

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("PostgresDataImport") \
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