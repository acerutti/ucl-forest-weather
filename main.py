"""
Make sure you have google the cloud storage package installed:

    pip install google-cloud-storage

"""
### Imports
import os
import io
from google.cloud import storage
import pandas as pd

# alessandra relative path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "ale-secrets-engineering-group-project-fcf687e1fa4b.json"

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







