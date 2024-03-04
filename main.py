"""
Make sure you have google the cloud storage package installed:

    pip install google-cloud-storage

"""

### Imports

import os
import io

from google.cloud import storage

# alessandra relative path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "ale-secrets-engineering-group-project-fcf687e1fa4b.json"

# Instantiates a client
storage_client = storage.Client()

# The name for the new bucket for the weather
bucket_name = "ucl-weather"

# Creates the new bucket
bucket = storage_client.create_bucket(bucket_name)

print(f"Bucket {bucket.name} created.")

