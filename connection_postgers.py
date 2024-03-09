##############################################################
## Connection Postgres
##############################################################

import sys
from google.cloud.sql.connector import Connector
import sqlalchemy

project_id = "engineering-group-project"
region = "us-central1"
instance_name = "forestnet-data"

!gcloud sql databases create sandwiches --instance={instance_name}

INSTANCE_CONNECTION_NAME = f"{project_id}:{region}:{instance_name}" # i.e demo-project:us-central1:demo-instance
print(f"Your instance connection name is: {INSTANCE_CONNECTION_NAME}")
DB_USER = "amitasujith"
DB_PASS = "baucl"
DB_NAME = "postgres"


# initialize Connector object
connector = Connector()

# function to return the database connection object
def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    return conn

# create connection pool with 'creator' argument to our connection object function
pool = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)