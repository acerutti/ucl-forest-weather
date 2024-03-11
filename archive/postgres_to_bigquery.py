###############################################################################
#### CONNECTING POSTGRES TO BIG QUERY - ATTEMPT ####
###############################################################################
### Imports
import os
import io
from google.cloud import storage #!pip install google-cloud-storage
import pandas as pd
# pip install bigquery
from google.cloud import bigquery
from template_specification_gcp_postgres import project_id, region, instance_name, current_user, DB_USER, DB_PASS, DB_NAME

#### CONNECTING TO GCP ENVIRONMENT ####
# 1. Ensure your secrets are in json file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "your_secrets_for_gcp.json"
# 2. Ensure you add details about your Postgres in template_specification_gcp_postgres

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


