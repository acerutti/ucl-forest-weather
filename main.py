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


#### CONNECTING TO GCP ENVIRONMENT ####

# alessandra relative path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "ale-secrets-engineering-group-project-fcf687e1fa4b.json"
#amita relative path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "amita-engineering-group-project-0489a29e6826.json"

###############################################################################
#### CREATING A COMBINED WEATHER DATA CSV FILE FROM EXISTING WEATHER DATA ####
###############################################################################

# this script aims to change the weather data so that it is in a format that can be merged
# with the deforestation_causes_regions data.
# this should help later on for better data analysis

df_weather = pd.read_csv("data/historical_weather_data_annual.csv")
df_weather.info()

df_weather.columns = map(str.lower, df_weather.columns)

# getting to long format
# Melting the DataFrame to go from wide to long format
df_temp = df_weather.melt(id_vars=['province'], value_vars=[col for col in df_weather.columns if 'temp' in col], var_name='year', value_name='average_temp')
df_rain = df_weather.melt(id_vars=['province'], value_vars=[col for col in df_weather.columns if 'rain' in col], var_name='year', value_name='average_rain')

# Extract year from the 'year' column
df_temp['year'] = df_temp['year'].str.extract('(\d{4})').astype(int)
df_rain['year'] = df_rain['year'].str.extract('(\d{4})').astype(int)

# Drop the "_average_temp" and "_average_rain" from the 'year' columns to have only the year
df_temp['year'] = df_temp['year'].astype(str).str.replace('_average_temp', '')

df_rain['year'] = df_rain['year'].astype(str).str.replace('_average_rain', '')

# Merge the temperature and rainfall DataFrames on 'province' and 'year'
df_combined = pd.merge(df_temp, df_rain, on=['province', 'year'])

df_combined.head()

# Savve df combined
df_combined.to_csv("combined_weather_data.csv", index=False)

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


###############################################################################
#### CONNECTING POSTGRES TO BIG QUERY ####
###############################################################################

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


