###############################################################################
#### Bucket Creation & Data Upload ####
###############################################################################

### Imports
import os
import io
from google.cloud import storage #!pip install google-cloud-storage
import pandas as pd
# pip install bigquery
from google.cloud import bigquery
from template_postgres_specification import project_id, region, instance_name, current_user, DB_USER, DB_PASS, DB_NAME

#### CONNECTING TO GCP ENVIRONMENT ####
# 1. Ensure your secrets are in json file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "your_secrets_for_gcp.json"
# 2. Ensure you add details about your Postgres in template_postgres_specification

###############################################################################
#### CREATE BUCKET  ####
###############################################################################

# The csv files for deforestation and weather data and the image files are ploaded into buckets

# Instantiates a client
storage_client = storage.Client()

## WEATHER BUCKET ##
# The name for the new bucket for the weather

bucket_name = "ucl-weather"
bucket = storage_client.create_bucket(bucket_name)
print(f"Bucket {bucket.name} created.")

## FOREST BUCKET ##
# The name for the new bucket for the forest

bucket_name = "ucl-forest"
bucket = storage_client.create_bucket(bucket_name)
print(f"Bucket {bucket.name} created.")


###############################################################################
#### WRITE ON THE BUCKET ####
###############################################################################

# Upload the files to their respective buckets

def upload_blob(bucket_name, source_file_name, destination_blob_name):
  """Uploads a file to the bucket."""
  storage_client = storage.Client()
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(destination_blob_name)

  blob.upload_from_filename(source_file_name)

  print('File {} uploaded to {}.'.format(
      source_file_name,
      destination_blob_name))


# Upload weather data to ucl-weather bucket 

# Upload update weather data (entries per year, per province per the top 3 provinces with most pictures)
upload_blob("ucl-weather", "data/historical_weather_data_annual.csv", "historical_weather_data_annual.csv")

# Upload combined long weather data (entries per year, per province per the top 3 provinces with most pictures)
upload_blob("ucl-weather", "data/combined_weather_data.csv", "combined_weather_data.csv")

# Upload image data to ucl-forest bucket

# define function to upload different folders of the images
def upload_directory_to_bucket(bucket_name, source_directory):
    """Uploads a directory to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    
    for local_dir, _, files in os.walk(source_directory):
        for file in files:
            local_file_path = os.path.join(local_dir, file)
            
            # Construct the full path for the destination blob
            # Using the folder names as the blob prefix
            relative_path = os.path.relpath(local_file_path, source_directory)
            destination_blob_name = relative_path.replace("\\", "/")  # Ensure proper path format for GCS
            
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(local_file_path)
            
            print(f'File {local_file_path} uploaded to {destination_blob_name}.')

# upload folders with the images 
            upload_directory_to_bucket('ucl-forest', 'data/image_forest_test')

