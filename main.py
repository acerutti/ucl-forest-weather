"""
Make sure you have google the cloud storage package installed:

    pip install google-cloud-storage

"""
### Imports
import os
import io
from google.cloud import storage
import pandas as pd
# pip install bigquery
from google.cloud import bigquery

# alessandra relative path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "ale-secrets-engineering-group-project-fcf687e1fa4b.json"

#amita relative path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "amita-engineering-group-project-0489a29e6826.json"

####################################################################
##### CREATE BUCKET
####################################################################

## Code is comment because bucket for the weather data has already been created
# Instantiates a client
storage_client = storage.Client()

## WEATHER BUCKET
# The name for the new bucket for the weather
#bucket_name = "ucl-weather"

# Creates the new bucket for 
#bucket = storage_client.create_bucket(bucket_name)

#print(f"Bucket {bucket.name} created.")

## FOREST BUCKET
# The name for the new bucket for the weather
#bucket_name = "ucl-forest"

# Creates the new bucket for 
#bucket = storage_client.create_bucket(bucket_name)

#print(f"Bucket {bucket.name} created.")


####################################################################
###### WRITE ON THE BUCKET
####################################################################

def upload_blob(bucket_name, source_file_name, destination_blob_name):
  """Uploads a file to the bucket."""
  storage_client = storage.Client()
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(destination_blob_name)

  blob.upload_from_filename(source_file_name)

  print('File {} uploaded to {}.'.format(
      source_file_name,
      destination_blob_name))
  
#upload_blob("ucl-weather", "california_housing_test.csv", "test_dataset.csv")

#upload_blob("ucl-forest", "test_image.jpeg", "test_image")

# Upload weather data
upload_blob("ucl-weather", "data/historical_weather_data_top3.csv", "historical_weather_data_top3.csv")

# Upload update weather data (entries per year, per province per the top 3 provinces with most pictures)
upload_blob("ucl-weather", "data/historical_weather_data_annual.csv", "historical_weather_data_annual.csv")

##############################################################
## Big Query
##############################################################
## FIRST: Create Dataset
# Construct a BigQuery client object
client = bigquery.Client()

# Set dataset_id to the ID of the dataset to create.
dataset_id = "{}.forest_dataset".format(client.project)

# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset(dataset_id)

# Specify the geographic location where the dataset should reside.
dataset.location = "US"

# Send the dataset to the API for creation, with an explicit timeout.
dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
print("Created dataset {}.{}".format(client.project, dataset.dataset_id))


## SECOND: Create Schema to then populate with data
# Table for deforestation data has the same structure as the postgres

# Construct a BigQuery client object.
client = bigquery.Client()

# Table id: projectid.dataset.tablename
# dataset has been created through the web interface
table_id = "engineering-group-project.forest_dataset.deforestation_data"

# Schema for deforestation data in BigQuery
schema = [
    bigquery.SchemaField("label", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("merged_label", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("example_path", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("province", "STRING", mode="NULLABLE"),
]

# Create the table with the specified schema.
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)

##############################################################
## Weather Data Checking Class
##############################################################

df_weather = pd.read_csv("data/historical_weather_data_annual.csv")
df_weather.info()

df_weather.columns = map(str.lower, df_weather.columns)

df_weather.to_csv("data_weather_modified")

